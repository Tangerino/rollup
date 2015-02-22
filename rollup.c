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
#include <time.h>
#include <math.h>
#include <pthread.h>
#include <inttypes.h>
#include <unistd.h> 
#include <time.h>
#include "sqlite3.h"
#include <zmq.h>
#include "CJson.h"
#include "worker.h"
#include "sink.h"
#include "support.h"
#include "samples.h"

long hourlyInterval = 0;
long dailyInterval = 0;
long monthlyInterval = 0;
long totalDataPoints = 0;
long totalTime = 0;
void *ctx;
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
                cJSON *v = cJSON_CreateString((const char *)strValue);
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

static int rollupTag (sqlite3 *db, int64_t tagId, int64_t startTs, int64_t endTs, int type, int64_t jobId) {
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
            double vsum =    sqlite3_column_double (st, 0);
            double vavg =    sqlite3_column_double (st, 1);
            double vmax =    sqlite3_column_double (st, 2);
            double vmin =    sqlite3_column_double (st, 3);
            int64_t vcount = sqlite3_column_int64  (st, 4);
            upsertRollup(db, tagId, type + 1, startTs, vsum, vavg, vmax, vmin, vcount, jobId, 0);
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
static int rollupTagByYear (sqlite3 *db, int64_t jobId, int64_t tagId, int64_t ts) {
    int rc = SQLITE_OK;
    time_t ts2 = timeAddYear(ts);
    rc = rollupTag(db, tagId, ts, ts2, ROLLUP_MONTH, jobId);
    return rc;
}

/**
 * \brief Roll up data by month
 * @param db The database connection
 * @param tagId The tag ID
 * @param ts The month to be rolled up
 * @return 0 if all good
 */
static int rollupTagByMonth (sqlite3 *db, int64_t jobId, int64_t tagId, int64_t ts) {
    int rc = SQLITE_OK;
    time_t ts2  = timeAddMonth(ts);
    rc = rollupTag(db, tagId, ts, ts2, ROLLUP_DAY, jobId);
    return rc;
}

/**
 * \brief Roll up data by day
 * @param db The database connection
 * @param tagId The tag ID
 * @param ts The day to be rolled up
 * @return 0 if all good
 */
static int rollupTagByDay (sqlite3 *db, int64_t jobId, int64_t tagId, int64_t ts) {
    int rc = SQLITE_OK;
    rc = rollupTag(db, tagId, ts, ts + (3600 * 24), ROLLUP_HOUR, jobId);
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
    // ts is the job's time stamp.
    // we will grab its related data
    // here is the trick part
    // we move the time stamp one second back in order to
    // comply to the rule where the time stamp is set to the 
    // end of the interval
    time_t tsData = getStartOfHour(ts - 1); // this is where we get data from
    sprintf (query, select, tagId, (int64_t)tsData, (int64_t)tsData + 3600);
    sqlite3_stmt *st = NULL;
    rc = sqlite3_prepare_v2(db, query, (int)(strlen(query)), &st, NULL);
    if (rc == SQLITE_OK) {
        rc = buildAndSendMessage(sender, ROLLUP_HOUR, jobId, tagId, ts, st); // but the original TS remains
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
static int rollup (sqlite3 *db, void *sender, int type, uint64_t *lastId) {
    int rc = SQLITE_OK;
    int jobCount = 0;
    char query[1024];
    const char *select1 = "select "
                "id,"
                "tagid,"
                "ts, type  "
                "from job "
                "where "
                "type = %d and sent = 0 and id > %" PRId64 " limit 500;";
    const char *select2 = "select "
                "id,"
                "tagid,"
                "ts, type  "
                "from job "
                "where "
                "type = %d and sent = 0";
    sqlite3_stmt *st = NULL;
    if (lastId) {
        sprintf (query, select1, type, *lastId);
    } else {
        sprintf (query, select2, type);
    }
    rc = sqlite3_prepare_v2(db, query, (int)(strlen(query)), &st, NULL);
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            int64_t id    = sqlite3_column_int64 (st, 0);
            int64_t tagId = sqlite3_column_int64 (st, 1);
            int64_t ts    = sqlite3_column_int64 (st, 2);
            int64_t type  = sqlite3_column_int64 (st, 3);
            if (lastId) *lastId = id;
            //const char *update = "update job set sent=1 where id=%" PRId64 ";";
            //sprintf (query, update, id);
            //execSql(db, query);
            jobCount++;
            switch (type) {
                case ROLLUP_HOUR:
                    ts = getStartOfHour(ts);
                    rc = rollupTagByHour  (db, id, tagId, ts, sender);
                    break;
                case ROLLUP_DAY:
                    ts = getStartOfDay(ts);
                    rc = rollupTagByDay   (db, id, tagId, ts);
                    break;
                case ROLLUP_MONTH:
                    ts = getStartOfMonth(ts);
                    rc = rollupTagByMonth (db, id, tagId, ts);
                    break;
                case ROLLUP_YEAR:
                    ts = getStartOfYear(ts);
                    rc = rollupTagByYear  (db, id, tagId, ts);
                    break; 
                default:
                    printf ("Unknown type (%d) for job id %" PRId64 "", (int)type, id);
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
    if (rc == 0) 
        return jobCount;
    else 
        return -1;
}

/**
 * \brief Roll up up data by hour; day; month and year
 * @param db The database connection
 * @return 0 if all good
 */
static int doRollup (sqlite3 *db, void *ctx) {
    void *sender = zmq_socket (ctx, ZMQ_PUSH); 
    assert (sender);
    zmq_bind (sender, "tcp://*:5557");   // PORT_A  
    int jobCount = 0;
    int jobs;
    uint64_t lastId = -1;
    do {
        jobs = rollup(db, sender, ROLLUP_HOUR, &lastId);
        printf ("%d jobs sent\r\n", jobs);
        jobCount += jobs;
        if (jobs) sleep(1);
    } while (jobs > 0);
    printf ("%d hourly jobs were sent\r\n", jobCount);
    sleep(60);
    lap ("rolling up by day");
    rollup(db, sender, ROLLUP_DAY, NULL);
    lap ("rolling up by month");
    rollup(db, sender, ROLLUP_MONTH, NULL);
    lap ("rolling up by year");
    rollup(db, sender, ROLLUP_YEAR, NULL);
    lap ("rolling up by done");
    zmq_close(sender);
    zmq_ctx_destroy(ctx);
    return 0;
}

/**
 * \brief Data rollup with domino effect
 *        Details at http://tangerino.me/rollup.html
 * @param argc
 * @param argv
 * @return 
 */
int main (int argc, char *argv[]) {
    int threadParam;
    int maxThreads = 10;
    sqlite3 *db;
    pthread_t sinkT;
    pthread_t workerT;
    ctx = zmq_ctx_new();
    assert (ctx);    
    int rc = sqlite3_open(DATABASE, &db);
    elapsedControl = time(NULL);
    if (rc == SQLITE_OK) {
        pthread_create(&sinkT, NULL, (void *)sinkThread, NULL);
        int threadCount;
        for (threadCount = 0; threadCount < maxThreads; threadCount++) {
            threadParam = threadCount + 1;
            pthread_create(&workerT, NULL, (void *)workerThread, (void *)&threadParam);
            usleep((useconds_t)50 * 1000L);
        }
        execSql (db, "PRAGMA journal_mode=WAL;PRAGMA busy_timeout = 1000;");
        lap ("Threads created");
        cleanUpTables(db);
        lap ("Clean up tables");
        sampleData (db);
        lap ("Sample data generated");
        doRollup(db, ctx);
        lap ("Process done.");
        sqlite3_close(db);
    }
    return 0;
}
