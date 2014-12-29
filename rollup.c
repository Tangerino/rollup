/*
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
#include <inttypes.h>
#include <time.h>
#include "sqlite3.h"

time_t elapsedControl;

enum {
    ROLLUP_HOUR = 0,
    ROLLUP_DAY,
    ROLLUP_MONTH,
    ROLLUP_YEAR
} enAggregationType;

void lap (const char *message) {
    time_t t = time(NULL);
    printf ("%s. New lap %ld seconds\n", message, (long)(t - elapsedControl));
    elapsedControl = t;
}

int execSql(sqlite3 *db, const char *sql) {
    int rc = sqlite3_exec(db, sql, NULL, 0, NULL);
    if (rc && (rc != SQLITE_CONSTRAINT)) {
        printf ("Error %d (%s) Query:%s\n", rc, sqlite3_errmsg(db), sql);
    }
    return rc;
}

char *tt2iso8602 (time_t tt, char *dt) {
    struct tm *loctime;
    char *localdt[256];
    loctime = localtime(&tt);
    strftime(dt, sizeof (localdt), "%Y-%m-%dT%H:%M:%S", loctime);
    return dt;    
}

time_t iso8602ts (const char *isoDate) {
    struct tm t;
    strptime(isoDate, "%Y-%m-%dT%H:%M:%S", &t);
    time_t tt = timegm(&t);
    return tt;
}

static time_t timeAddMonth (time_t ts) {
    struct tm tm;
    localtime_r(&ts, &tm);
    tm.tm_mon++;
    if (tm.tm_mon >= 12) {
        tm.tm_mon = 0;
        tm.tm_year++;
    }
    time_t tt = mktime(&tm);
    return tt;
}

static time_t timeAddYear (time_t ts) {
    struct tm tm;
    localtime_r(&ts, &tm);
    tm.tm_year++;
    time_t tt = mktime(&tm);
    return tt;
}

time_t getStartOfYear (time_t ts) {
    struct tm tm;
    localtime_r (&ts, &tm);
    tm.tm_sec = 0;
    tm.tm_min = 0;
    tm.tm_hour = 0;
    tm.tm_mday = 1;
    tm.tm_mon = 0;
    time_t t = mktime(&tm);
    return t;
}

time_t getStartOfMonth (time_t ts) {
    struct tm tm;
    localtime_r (&ts, &tm);
    tm.tm_sec = 0;
    tm.tm_min = 0;
    tm.tm_hour = 0;
    tm.tm_mday = 1;
    time_t t = mktime(&tm);
    return t;
}

time_t getStartOfDay (time_t ts) {
    struct tm tm;
    localtime_r (&ts, &tm);
    tm.tm_sec = 0;
    tm.tm_min = 0;
    tm.tm_hour = 0;
    time_t t = mktime(&tm);
    return t;
}

time_t getStartOfHour (time_t ts) {
    struct tm tm;
    localtime_r (&ts, &tm);
    tm.tm_min = 0;
    tm.tm_sec = 0;
    time_t t = mktime(&tm);
    return t;
}

int updateRollupControl (sqlite3 *db, int64_t tagId, int type, time_t utc) {
    int rc;
    const char *insert  = "insert into job (tagid, type, ts) values (%" PRId64 ",%d, %" PRId64 ");";;
    char query[512];
    
    switch (type) {
        case ROLLUP_HOUR:
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

static int rollupTag (sqlite3 *db, int64_t tagId, int64_t startTs, int64_t endTs, int type) {
    int rc = SQLITE_OK;
    char query[2048];
    const char *select = "select sum(vsum), avg(vavg), max(vmax), min(vmin), sum(vcount)"
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

static int rollupTagByYear (sqlite3 *db, int64_t tagId, int64_t ts) {
    int rc = SQLITE_OK;
    time_t ts2 = timeAddYear(ts);
    rc = rollupTag(db, tagId, ts, ts2, ROLLUP_MONTH);
    return rc;
}

static int rollupTagByMonth (sqlite3 *db, int64_t tagId, int64_t ts) {
    int rc = SQLITE_OK;
    time_t ts2  = timeAddMonth(ts);
    rc = rollupTag(db, tagId, ts, ts2, ROLLUP_DAY);
    return rc;
}

static int rollupTagByDay (sqlite3 *db, int64_t tagId, int64_t ts) {
    int rc = SQLITE_OK;
    rc = rollupTag(db, tagId, ts, ts + (3600 * 24),ROLLUP_HOUR);
    return rc;
}

static int rollupTagByHour (sqlite3 *db, int64_t tagId, int64_t ts) {
    int rc = SQLITE_OK;
    char query[2048];
    const char *select = "select sum(value), avg(value), max(value), min(value), count(value)"
                        " from history "
                        " where "
                        " tagid = %" PRId64 " AND "
                        " ts > %" PRId64 " and "
                        " ts <= %" PRId64 " ";
    sprintf (query, select, tagId, (int64_t)ts, (int64_t)ts + 3600);
    sqlite3_stmt *st = NULL;
    rc = sqlite3_prepare_v2(db, query, (int)(strlen(query)), &st, NULL);
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            upsertRollup(db, tagId, ROLLUP_HOUR, ts, st);
        }
        sqlite3_finalize(st);
        if (rc == SQLITE_DONE) {
            rc = SQLITE_OK;
        }
    } else {
        printf ("%s - %s\n", sqlite3_errmsg(db), query);
    }
    return rc;
}

static int rollup (sqlite3 *db, int type) {
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
        case ROLLUP_HOUR:   // we move to local time when coming from history
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
                    rc = rollupTagByHour  (db, tagId, ts);
                    break;
                case ROLLUP_DAY:
                    ts = getStartOfDay(ts);
                    rc = rollupTagByDay   (db, tagId, ts);
                    break;
                case ROLLUP_MONTH:
                    ts = getStartOfMonth(ts);
                    rc = rollupTagByMonth (db, tagId, ts);
                    break;
                case ROLLUP_YEAR:
                    ts = getStartOfYear(ts);
                    rc = rollupTagByYear  (db, tagId, ts);
                    break;            
            }
            if (rc == SQLITE_OK) {
                const char *delete = "delete from job where id = %" PRId64 ";";
                sprintf (query, delete, id);
                execSql (db, query);
                if (nextRollup != -1) {
                    updateRollupControl (db, tagId, nextRollup, ts);
                }
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

static int doRollup (sqlite3 *db) {
    int rc = rollup(db, ROLLUP_HOUR);
    lap ("Hourly rollup done");
    if (rc == SQLITE_OK) {
        rc = rollup(db, ROLLUP_DAY);
        lap ("Daily rollup done");
        if (rc == SQLITE_OK) {
            rc = rollup(db, ROLLUP_MONTH);
            lap ("Monthly rollup done");
            if (rc == SQLITE_OK) {
                rc = rollup(db, ROLLUP_YEAR);
                lap ("Yearly rollup done");
            }
        }
    }
    return rc;
}

static void generateSampleData (sqlite3 *db, const char *startDate, const char *endDate, int timeInterval, int tagId, double value) {
    const char *insert = "insert into history (tagid, value, ts) "
        "values (%d, %g, %" PRId64 ");";
    char query[1024];
    time_t sd = iso8602ts (startDate);
    time_t ed = iso8602ts (  endDate);
    const char *newTag = "insert into tag (id, name) values (%d, 'TAG%d');";
    sprintf (query, newTag, tagId, tagId);
    execSql (db, query);
    execSql (db, "begin;");
    for (;sd <= ed; sd += timeInterval) {
        sprintf (query, insert, tagId, value, (int64_t)sd, (int64_t)sd, (int64_t)sd);
        execSql (db, query);
        updateRollupControl (db, 1, ROLLUP_HOUR, sd);
    }
    execSql (db, "commit;");
}

int main (int argc, char *argv[]) {
    sqlite3 *db;
    int rc = sqlite3_open("./testdb.db3", &db);
    elapsedControl = time(NULL);
    if (rc == SQLITE_OK) {
        execSql (db, "PRAGMA journal_mode=WAL;");
        lap ("Start process");
        execSql (db, "delete from history;");
        execSql (db, "delete from rollup;");
        execSql (db, "delete from tag;");
        execSql (db, "delete from job;");        
        generateSampleData(db,"2009-12-31T20:00:00", "2011-01-01T03:15:00", 900, 1, 1);
        //generateSampleData(db,"2010-01-01T00:00:00", "2014-01-01T02:00:00", 900, 2, -1);
        lap ("Simulated data done");
        doRollup(db);
        lap ("Rollup done");
        sqlite3_close(db);
    }
    return rc;
}   
