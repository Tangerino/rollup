/**
 * 
 * 2014 July 8
 * 
 * The author disclaims copyright to this source code.  In place of
 * a legal notice, here is a blessing:
 *
 *    May you do good and not evil.
 *    May you find forgiveness for yourself and forgive others.
 *    May you share freely, never taking more than you give.
 *
 *      Carlos Tangerino
 *      carlos.tangerino@gmail.com
 *      http://tangerino.me
 * 
*/

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <inttypes.h>
#include <time.h>
#include "sqlite3.h"
#include <zmq.h>
#include "rollup.h"
#include "CJson.h"
#include "worker.h"
#include "sink.h"

#define PI2 (M_PI * 2)
#define ISGMT 1
long hourlyInterval = 0;
long dailyInterval = 0;
long monthlyInterval = 0;
long totalDataPoints = 0;
long totalTime = 0;

time_t elapsedControl;

/**
 * \brief Lap count
 * @param message
 */
void lap (const char *message) {
    time_t t = time(NULL);
    printf ("%s. New lap %ld seconds\n", message, (long)(t - elapsedControl));
    totalTime += (long)(t - elapsedControl);
    elapsedControl = t;
}

struct tm * time_r (const time_t *time, struct tm *resultp, int isGmt) {
    if (isGmt) {
        return gmtime_r (time, resultp);
    } else {
        return localtime_r (time, resultp);        
    }
}

/**
 * \brief Execute a SQL command and handle error
 * @param db The database connection
 * @param sql The query to be executed
 * @return 0 if all good
 */
int execSql(sqlite3 *db, const char *sql) {
    int rc = sqlite3_exec(db, sql, NULL, 0, NULL);
    if (rc && (rc != SQLITE_CONSTRAINT)) {
        printf ("Error %d (%s) Query:%s\n", rc, sqlite3_errmsg(db), sql);
    }
    return rc;
}

/**
 * \brief Format a time stamp in ISOData format
 * @param tt The time stamp
 * @param dt The output buffer
 * @return The pointer to the output buffer
 */
char *tt2iso8602 (time_t tt, char *dt) {
    struct tm *loctime;
    char *localdt[256];
    loctime = localtime(&tt);
    strftime(dt, sizeof (localdt), "%Y-%m-%dT%H:%M:%S", loctime);
    return dt;    
}

/**
 * \brief Format the current time
 * @param isoDate
 * @return 
 */
time_t iso8602ts (const char *isoDate) {
    struct tm t;
    strptime(isoDate, "%Y-%m-%dT%H:%M:%S", &t);
    time_t tt = timegm(&t);
    return tt;
}

/**
 * \brief Add one year to the time stamp
 * @param ts The time stamp
 * @return The adjusted time stamp
 */
static time_t timeAddYear (time_t ts) {
    struct tm tm;
    time_r(&ts, &tm, ISGMT);
    tm.tm_year++;
    time_t tt = mktime(&tm);
    return tt;
}

/**
 * \brief Add one month to the time stamp
 * @param ts The time stamp
 * @return The adjusted time stamp
 */
static time_t timeAddMonth (time_t ts) {
    struct tm tm;
    time_r(&ts, &tm, ISGMT);
    tm.tm_mon++;
    if (tm.tm_mon >= 12) {
        tm.tm_mon = 0;
        tm.tm_year++;
    }
    time_t tt = mktime(&tm);
    return tt;
}

/**
 * \brief Adjust the time stamp for the beginning of the year
 * @param ts The time stamp
 * @return Adjusted time stamp
 */

time_t getStartOfYear (time_t ts) {
    struct tm tm;
    time_r (&ts, &tm, ISGMT);
    tm.tm_sec = 0;
    tm.tm_min = 0;
    tm.tm_hour = 0;
    tm.tm_mday = 1;
    tm.tm_mon = 0;
    time_t t = mktime(&tm);
    return t;
}

/**
 * \brief Adjust the time stamp for the beginning of the month
 * @param ts The time stamp
 * @return Adjusted time stamp
 */
time_t getStartOfMonth (time_t ts) {
    struct tm tm;
    time_r (&ts, &tm, ISGMT);
    tm.tm_sec = 0;
    tm.tm_min = 0;
    tm.tm_hour = 0;
    tm.tm_mday = 1;
    time_t t = mktime(&tm);
    return t;
}

/**
 * \brief Adjust the time stamp for the beginning of the day
 * @param ts The time stamp
 * @return Adjusted time stamp
 */
time_t getStartOfDay (time_t ts) {
    struct tm tm;
    time_r (&ts, &tm, ISGMT);
    tm.tm_sec = 0;
    tm.tm_min = 0;
    tm.tm_hour = 0;
    time_t t = mktime(&tm);
    return t;
}

/**
 * \brief Adjust the time stamp for the beginning of the hour
 * @param ts The time stamp
 * @return Adjusted time stamp
 */
time_t getStartOfHour (time_t ts) {
    struct tm tm;
    time_r (&ts, &tm, ISGMT);
    tm.tm_min = 0;
    tm.tm_sec = 0;
    time_t t = mktime(&tm);
    return t;
}

/**
 * \brief Update the JOB table
 * @param db The database connection
 * @param tagId The tag ID
 * @param type The aggregation type
 * @param utc The time stamp
 * @return 
 */
int updateRollupControl (sqlite3 *db, int64_t tagId, int type, time_t utc) {
    int rc;
    const char *insert  = "insert into job (tagid, type, ts) values (%" PRId64 ",%d, %" PRId64 ");";
    char query[512];
    
    switch (type) {
        case ROLLUP_HOUR:
            totalDataPoints++;
            utc = getStartOfHour(utc - 1);
            break;
        case ROLLUP_DAY:
            utc = getStartOfDay(utc);
            break;
        case ROLLUP_MONTH:
            utc = getStartOfMonth(utc);
            break;
        case ROLLUP_YEAR:
            utc = getStartOfYear(utc);
            break;
        default:
            return SQLITE_OK;
            break;
    }
    sprintf (query, insert, tagId, type, (int64_t)utc);
    rc = execSql(db, query);
    if (rc == SQLITE_CONSTRAINT) {
        rc = SQLITE_OK;
    }
    return rc;
}

/**
 * \brief Update the roll up table
 * @param db The data base connection
 * @param tagId The tag ID
 * @param type The aggregation type
 * @param ts The time stamp
 * @param st The sql statement to extract the data from
 * @return 
 */
static int upsertRollup (sqlite3 *db, uint64_t tagId, int type, time_t ts, sqlite3_stmt *st) {
    int rc;
    char query[1024];
    const char *insert = 
    "insert into rollup "
    "(tagid, type, vsum, vavg, vmax, vmin, vcount, ts) "
    "values (%" PRId64 ", %d, %g, %g, %g, %g, %" PRId64 ", %" PRId64 ");";

    const char *update = 
    "update rollup "
    "set "
    "vsum=%g, "
    "vavg=%g, "
    "vmax=%g, "
    "vmin=%g, "
    "vcount=%" PRId64 " "
    "where "
    "tagid=%" PRId64 " and "
    "type=%d and ts=%" PRId64 ";";
    double vsum =    sqlite3_column_double (st, 0);
    double vavg =    sqlite3_column_double (st, 1);
    double vmax =    sqlite3_column_double (st, 2);
    double vmin =    sqlite3_column_double (st, 3);
    int64_t vcount = sqlite3_column_int64  (st, 4);
    if (vcount == 0) {
        return SQLITE_OK;
    }
    switch (type) {
        case ROLLUP_HOUR:
            ts = getStartOfHour (ts);
            break;
        case ROLLUP_DAY:
            ts = getStartOfDay (ts);
            break;
        case ROLLUP_MONTH:
            ts = getStartOfMonth (ts);
            break;
        case ROLLUP_YEAR:
            ts = getStartOfYear (ts);
            break;
    }
    sprintf (query, insert, tagId, type, vsum, vavg, vmax, vmin, vcount, (int64_t)ts);
    rc = execSql (db, query);
    if (rc != SQLITE_OK) {
        if (rc == SQLITE_CONSTRAINT) {
            sprintf (query, update, vsum, vavg, vmax, vmin, vcount, tagId, type, (int64_t)ts);
            rc = execSql (db, query);            
        } else {
            printf ("Error inserting rollup data\n");
        }
    }
    return rc;
}

/**
 * \brief Perform the data aggregation
 *        Aggregates the data in five different flavors as in:
 *        MAX; MIN; AVERAGE; SUM and COUNT
 * @param db The database connection
 * @param tagId The tag ID
 * @param startTs Start period
 * @param endTs Final period
 * @param type Aggregation type 
 * @return 0 if all good
 */
static int rollupTag (sqlite3 *db, int64_t tagId, int64_t startTs, int64_t endTs, int type) {
    int rc = SQLITE_OK;
    char query[2048];
    const char *select = "select total(vsum), avg(vavg), max(vmax), min(vmin), sum(vcount)"
                        " from rollup "
                        " where "
                        " tagid =  %" PRId64 " and "
                        " type = %d and "
                        " ts >= %" PRId64 " and "
                        " ts < %" PRId64 " ";
    sprintf (query, select, tagId, type, startTs, endTs);
    sqlite3_stmt *st = NULL;
    rc = sqlite3_prepare_v2(db, query, (int)(strlen(query)), &st, NULL);
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            upsertRollup(db, tagId, type + 1, startTs, st);
        }
        sqlite3_finalize(st);
        if (rc == SQLITE_DONE) {
            rc = SQLITE_OK;
        }
    } else {
        printf("%s - %s\n", sqlite3_errmsg(db), query);
    }
    return rc;    
}

/**
 * \brief Roll up data by year
 * @param db The database connection
 * @param tagId The tag ID
 * @param ts The year to be rolled up
 * @return 0 if all good
 */
static int rollupTagByYear (sqlite3 *db, int64_t tagId, int64_t ts) {
    int rc = SQLITE_OK;
    time_t ts2 = timeAddYear(ts);
    rc = rollupTag(db, tagId, ts, ts2, ROLLUP_MONTH);
    return rc;
}

/**
 * \brief Roll up data by month
 * @param db The database connection
 * @param tagId The tag ID
 * @param ts The month to be rolled up
 * @return 0 if all good
 */
static int rollupTagByMonth (sqlite3 *db, int64_t tagId, int64_t ts) {
    int rc = SQLITE_OK;
    monthlyInterval++;
    time_t ts2  = timeAddMonth(ts);
    rc = rollupTag(db, tagId, ts, ts2, ROLLUP_DAY);
    return rc;
}

/**
 * \brief Roll up data by day
 * @param db The database connection
 * @param tagId The tag ID
 * @param ts The day to be rolled up
 * @return 0 if all good
 */
static int rollupTagByDay (sqlite3 *db, int64_t tagId, int64_t ts) {
    int rc = SQLITE_OK;
    dailyInterval++;
    rc = rollupTag(db, tagId, ts, ts + (3600 * 24),ROLLUP_HOUR);
    return rc;
}

int buildAndSendMessage (void *sender, int64_t type, int64_t jobId, int64_t tagId, int64_t ts, sqlite3_stmt *st) {
    int rc = 0;
    cJSON *root = cJSON_CreateObject();
    if (root) {
        cJSON_AddNumberToObject (root, "jobid", jobId);
        cJSON_AddNumberToObject (root, "tagid", tagId);
        cJSON_AddNumberToObject (root, "ts",    ts);
        cJSON_AddNumberToObject (root, "type",  type);    
        cJSON *values = cJSON_CreateArray();
        if (values) {
            while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
                const unsigned char *strValue = sqlite3_column_text (st, 0);
                cJSON *v = cJSON_CreateString(strValue);
                cJSON_AddItemToArray (values, v);
            }
            sqlite3_finalize(st);
            cJSON_AddItemToObject(root, "values", values);
            char *msg = cJSON_Print(root);
            //printf ("%s\r\n", msg);
            rc = zmq_send(sender, msg, strlen(msg), 0);
            free(msg);
        }
        cJSON_Delete(root);
    }
    return rc;
}

/**
 * \brief Roll up data by hour
 * @param db The database connection
 * @param tagId The tag ID
 * @param ts The hour to be rolled up
 * @return 0 if all good
 */
static int rollupTagByHour (sqlite3 *db, int64_t jobId, int64_t tagId, int64_t ts, void *sender) {
    int rc = SQLITE_OK;
    char query[2048];
    hourlyInterval++;
    const char *select = "select value"
                        " from history "
                        " where "
                        " tagid = %" PRId64 " AND "
                        " ts > %" PRId64 " and "
                        " ts <= %" PRId64 " ";
    sprintf (query, select, tagId, (int64_t)ts, (int64_t)ts + 3600);
    sqlite3_stmt *st = NULL;
    rc = sqlite3_prepare_v2(db, query, (int)(strlen(query)), &st, NULL);
    if (rc == SQLITE_OK) {
        rc = buildAndSendMessage(sender, ROLLUP_HOUR, jobId, tagId, ts, st);
    } else {
        printf ("%s - %s\n", sqlite3_errmsg(db), query);
    }
    return rc;
}

/**
 * \brief Do one type of data roll up for the entire data set
 * @param db The database connection
 * @param type The roll up type. See enAggregationType
 * @return 0 if all good
 */
static int rollup (sqlite3 *db, int type, void *sender) {
    int rc = SQLITE_OK;
    int nextRollup;
    char query[1024];
    const char *select = "select "
                         "id,"
                         "tagid,"
                         "ts "
                         "from job "
                         "where "
                         "type = %d "
                         "order by tagid, ts";
    switch (type) {
        case ROLLUP_HOUR:
            nextRollup = ROLLUP_DAY;
            break;
        case ROLLUP_DAY:
            nextRollup = ROLLUP_MONTH;
            break;
        case ROLLUP_MONTH:
            nextRollup = ROLLUP_YEAR;
            break;
        case ROLLUP_YEAR:
            nextRollup = -1;
            break;
        default:
            return ~SQLITE_OK;
    }

    sprintf (query, select, type);
    sqlite3_stmt *st = NULL;
    rc = sqlite3_prepare_v2(db, query, (int)(strlen(query)), &st, NULL);
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            int64_t id =        sqlite3_column_int64 (st, 0);
            int64_t tagId =     sqlite3_column_int64 (st, 1);
            uint64_t ts =       sqlite3_column_int64 (st, 2);
            switch (type) {
                case ROLLUP_HOUR:
                    ts = getStartOfHour(ts);
                    rc = rollupTagByHour  (db, id, tagId, ts, sender);
                    break;
                case ROLLUP_DAY:
                    ts = getStartOfDay(ts);
                    //rc = rollupTagByDay   (db, id, tagId, ts);
                    break;
                case ROLLUP_MONTH:
                    ts = getStartOfMonth(ts);
                    //rc = rollupTagByMonth (db, id, tagId, ts);
                    break;
                case ROLLUP_YEAR:
                    ts = getStartOfYear(ts);
                    //rc = rollupTagByYear  (db, id, tagId, ts);
                    break;            
            }
        }
        sqlite3_finalize(st);
    } else {
        printf ("%s - %s\n", sqlite3_errmsg(db), query);
    }
    if (rc == SQLITE_DONE) {
        rc = SQLITE_OK;
    }
    return rc;
}

/**
 * \brief Roll up up data by hour; day; month and year
 * @param db The database connection
 * @return 0 if all good
 */
static int doRollup (sqlite3 *db) {
    void *ctx = zmq_ctx_new();
    assert (ctx);
    void *sender = zmq_socket (ctx, ZMQ_PUSH); 
    assert (sender);
    zmq_bind (sender, "tcp://*:5557");   // PORT_A    
    
    int rc = rollup(db, ROLLUP_HOUR, sender);
//    lap ("Hourly rollup done");
//    if (rc == SQLITE_OK) {
//        rc = rollup(db, ROLLUP_DAY);
//        lap ("Daily rollup done");
//        if (rc == SQLITE_OK) {
//            rc = rollup(db, ROLLUP_MONTH);
//            lap ("Monthly rollup done");
//            if (rc == SQLITE_OK) {
//                rc = rollup(db, ROLLUP_YEAR);
//                lap ("Yearly rollup done");
//            }
//        }
//    }
    zmq_close(sender);
    zmq_ctx_destroy(ctx);
    return rc;
}

static void newTag (sqlite3 *db, int64_t tagId) {
    char query[1024];
    const char *newTag = "insert into tag (id, name) values (%" PRId64 ", 'TAG%" PRId64 "');";
    sprintf (query, newTag, tagId, tagId);
    execSql (db, query);    
}

/**
 * \Brief Populates the data base with initial data to be rolled
 *        A good idea is to creates data interval with values 1 (one) so it is easy to
 *        predict the results
 * @param db Database connection
 * @param startDate The start date in ISODate 8601
 * @param endDate The final date in ISODate format
 * @param timeInterval The data frequency in one hour, like 15 for one data every quarter hour
 * @param tagId The Tag ID
 * @param value The tag value
 */
static void generateSampleData (sqlite3 *db, const char *startDate, const char *endDate, int timeInterval, int64_t tagId, double seed, double increment) {
    const char *insert = "insert into history (tagid, value, ts) "
        "values (%d, %g, %" PRId64 ");";
    char query[1024];
    time_t sd = iso8602ts (startDate);
    time_t ed = iso8602ts (  endDate);
    newTag (db, tagId);
    execSql (db, "begin;");
    for (;sd <= ed; sd += timeInterval) {
        sprintf (query, insert, tagId, seed, (int64_t)sd, (int64_t)sd, (int64_t)sd);
        seed += increment;
        execSql (db, query);
        updateRollupControl (db, tagId, ROLLUP_HOUR, sd);
    }
    execSql (db, "commit;");
    /*
     
.mode list
.separator ,
select 
"(" ||
tagid, 
cast(value as integer), 
"'" || strftime('%Y-%m-%d %H:%M:%S.0000000 +00:00',ts,'unixepoch') || "'", 
192 || "),"
from history;
          
     */
}

static void generateSampleData2 (sqlite3 *db, const char *startDate, const char *endDate, int timeInterval, int64_t tagId, double frequency) {
    const char *insert = "insert into history (tagid, value, ts) "
        "values (%d, %g, %" PRId64 ");";
    char query[1024];
    time_t sd = iso8602ts (startDate);
    time_t ed = iso8602ts (  endDate);
    newTag (db, tagId);
    execSql (db, "begin;");
    double seed = 0;
    if (frequency == 0) frequency = 1;
    for (;sd <= ed; sd += timeInterval) {
        sprintf (query, insert, tagId, sin(seed), (int64_t)sd, (int64_t)sd, (int64_t)sd);
        seed += (3.14 / frequency);
        execSql (db, query);
        updateRollupControl (db, tagId, ROLLUP_HOUR, sd);
    }
    execSql (db, "commit;");
}

static void generateSampleData3 (sqlite3 *db, const char *startDate, const char *endDate, int timeInterval, int64_t tagId, double frequency) {
    const char *insert = "insert into history (tagid, value, ts) "
        "values (%d, %g, %" PRId64 ");";

    char query[1024];
    time_t sd = iso8602ts (startDate);
    time_t ed = iso8602ts (  endDate);
    newTag (db, tagId);
    execSql (db, "begin;");
    double seed = 0;
    if (frequency == 0) frequency = 1;
    for (;sd <= ed; sd += timeInterval) {
        double f = sin(seed * 0.5) * cos (seed * (1/0.5));
        sprintf (query, insert, tagId, f, (int64_t)sd, (int64_t)sd, (int64_t)sd);
        seed += (PI2 / frequency);
        if (seed > PI2) seed = 0;
        execSql (db, query);
        updateRollupControl (db, tagId, ROLLUP_HOUR, sd);
    }
    execSql (db, "commit;");
}

static int fromWeToHis (sqlite3 *db, int64_t tagId, sqlite3_stmt *st) {
    int64_t ts    = sqlite3_column_int64  (st, 0);
    double  value = sqlite3_column_double (st, 1);
    const char *insert = "insert into history (tagid, value, ts, dt) values "
    "(%" PRId64 ", %g, %" PRId64 ",strftime('%%Y-%%m-%%dT%%H:%%M:%%S',%" PRId64 ",'unixepoch'));";
    char query [2048];
    sprintf (query, insert, tagId, value, ts, ts);
    int rc = execSql (db, query);
    if (rc == 0) {
        rc = updateRollupControl (db, tagId, ROLLUP_HOUR, ts);
    }
    return rc;
}

static void getDataFromTable (sqlite3 *db, int64_t tagId, int64_t measureId) {
    const char *select = "select ts, value from we where tagid=%" PRId64 " "
    " and measureid = %" PRId64 ";";
    char query[2048];
    sqlite3_stmt *st;
    int64_t finalTagId = tagId * 10000 + measureId;
    newTag (db, finalTagId);
    sprintf (query, select, tagId, measureId);
    int rc = sqlite3_prepare_v2(db, query, (int)(strlen(query)), &st, NULL);
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            if (fromWeToHis (db, finalTagId, st)) {
                break;
            }
        }
        sqlite3_finalize(st);
        if (rc == SQLITE_DONE) {
            rc = SQLITE_OK;
        }
    } else {
        printf("%s - %s\n", sqlite3_errmsg(db), query);
    }
}

/**
 * \brief Data rollup with domino effect
 *        Details at http://tangerino.me/rollup.html
 * @param argc
 * @param argv
 * @return 
 */
int main (int argc, char *argv[]) {
    
    sqlite3 *db;
    pthread_t sinkT;
    pthread_t workerT;
    int rc = sqlite3_open(DATABASE, &db);
    elapsedControl = time(NULL);
    if (rc == SQLITE_OK) {
        pthread_create(&sinkT, NULL, (void *)sinkThread, NULL);
        pthread_create(&workerT, NULL, (void *)workerThread, NULL);
        execSql (db, "PRAGMA journal_mode=WAL;");
        lap ("Start process");
        execSql (db, "delete from history;");
        execSql (db, "delete from rollup;");
        execSql (db, "delete from tag;");
        execSql (db, "delete from job;");   
        const char *sd = "2015-01-01T00:00:00";
        const char *ed = "2015-01-01T03:00:00";
        generateSampleData (db, sd, ed, 900, 1, 1, 1);
//        generateSampleData2(db, sd, ed, 900, 2, 32);
//        generateSampleData3(db, sd, ed, 900, 3, 96);
//        generateSampleData (db, sd, ed, 900, 130, 1, 1);
//        getDataFromTable(db,  8408, 30);
//        getDataFromTable(db, 38339, 30);
//        getDataFromTable(db, 196342, 31);
//        getDataFromTable(db, 196342, 30);
//        lap ("Simulated data done");
        doRollup(db);
        sleep (5);
        lap ("Rollup done.");
        printf ("%ld data points, %ld hourly rollup, %ld daily rollup, %ld monthly rollup in %ld seconds", 
                totalDataPoints,
                hourlyInterval,
                dailyInterval,
                monthlyInterval,
                totalTime);
        sqlite3_close(db);
    }
    return rc;
}
