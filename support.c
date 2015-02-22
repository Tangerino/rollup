#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h> 
#include <zmq.h>
#include <pthread.h>
#include "sqlite3.h"
#include "CJson.h"
#include "jsonutil.h"
#include "support.h"

char *zmqReceive(void *socket, char * messageBuffer, int bufferSize) {
    zmq_msg_t message;
    zmq_msg_init(&message);
    int size = zmq_msg_recv(&message, socket, 0);
    if (size == -1) {
        int en = errno;
        if (en != EAGAIN && en != ETERM) {
            printf("Got an empty message. Reason %s", zmq_strerror(en));
        }
        errno = en;
        return NULL;
    }
    if (size > (bufferSize - 1)) {
        zmq_msg_close(&message);
        printf("Buffer too small %d  -> %d", size, bufferSize); // should not fall here
        return NULL;

    }
    memcpy(messageBuffer, zmq_msg_data(&message), size);
    zmq_msg_close(&message);
    messageBuffer [size] = '\0';
    return messageBuffer;
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
    int rc = SQLITE_OK;
    int retry = 0;
    int maxretry = 1;

    if (db == NULL) {
        return 1;
    }
    for (retry = 0; retry < maxretry; retry++) {
        rc = sqlite3_exec(db, sql, NULL, 0, NULL);
        if (rc == SQLITE_OK) {
            break;
        }
        if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
            useconds_t toSleep = 50 * (retry + 1);
            printf ("Retry %d %s\r\n", retry + 1, sql);
            usleep(toSleep * 1000L);
        } else {
            break;
        }
    }
    if (rc && (rc != SQLITE_CONSTRAINT)) {
        printf ("Error %d (%s) Query:%s. Retry:%d\r\n", rc, sqlite3_errmsg(db), sql, retry);
        printf ("%s\r\n", sqlite3_errmsg(db));
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
int addNewAggregationJob (sqlite3 *db, int64_t tagId, int type, time_t utc) {
    int rc;
    const char *insert  = "insert into job (tagid, type, ts, created) values (%" PRId64 ",%d, %" PRId64 ",CURRENT_TIMESTAMP);";
    char query[512];
    
    switch (type) {
        case ROLLUP_HOUR:
            utc = getStartOfHour(utc); // menos um tang cade to do
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

void newTag (sqlite3 *db, int64_t tagId) {
    char query[1024];
    const char *newTag = "insert into tag (id, name) values (%" PRId64 ", 'TAG%" PRId64 "');";
    sprintf (query, newTag, tagId, tagId);
    execSql (db, query);    
}

/**
 * \brief Add one year to the time stamp
 * @param ts The time stamp
 * @return The adjusted time stamp
 */
time_t timeAddYear (time_t ts) {
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
time_t timeAddMonth (time_t ts) {
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

int upsertRollup (sqlite3 *db, uint64_t tagId, int type, time_t ts, 
        double vsum, double vavg, double vmax, double vmin, int64_t vcount, 
        int64_t jobId, int64_t worker) {
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

    int nextRollup;
    switch (type) {
        case ROLLUP_HOUR:
            nextRollup = ROLLUP_DAY;
            ts = getStartOfHour (ts);
            break;
        case ROLLUP_DAY:
            nextRollup = ROLLUP_MONTH;
            ts = getStartOfDay (ts);
            break;
        case ROLLUP_MONTH:
            nextRollup = ROLLUP_YEAR;
            ts = getStartOfMonth (ts);
            break;
        case ROLLUP_YEAR:
            nextRollup = -1;
            ts = getStartOfYear (ts);
            break;
        default:
            nextRollup = -1;
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
    sprintf (query, "update job set type = -type, done = CURRENT_TIMESTAMP, worker = %" PRId64 " where id = %" PRId64 ";", worker, jobId);
    execSql(db,query);
    if (nextRollup >= 0) {
        addNewAggregationJob (db, tagId, nextRollup, ts);
    }        
    return rc;
}