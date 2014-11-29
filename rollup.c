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
    ROLLUP_HOUR,
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
    gmtime_r(&ts, &tm);
    tm.tm_mon++;
    if (tm.tm_mon > 12) {
        tm.tm_mon = 1;
        tm.tm_year++;
    }
    time_t tt = timegm(&tm);
    return tt;
}

static time_t timeAddYear (time_t ts) {
    struct tm tm;
    gmtime_r(&ts, &tm);
    tm.tm_year++;
    time_t tt = timegm(&tm);
    return tt;
}

int updateRollupControl (sqlite3 *db, int64_t tagId, int type, time_t utc) {
    int rc;
    const char *select;
    const char *insert;
    char query[512];
    
    switch (type) {
        case ROLLUP_HOUR:
            utc--;
            insert = "insert into job (tagid, type, dt, ts) values (%" PRId64 ",%d, strftime('%%Y-%%m-%%dT%%H:00:00', %" PRId64 ",'unixepoch'), %" PRId64 ");";
            break;
        case ROLLUP_DAY:
            insert = "insert into job (tagid, type, dt, ts) values (%" PRId64 ",%d, strftime('%%Y-%%m-%%dT00:00:00', %" PRId64 ",'unixepoch'), %" PRId64 ");";
            break;
        case ROLLUP_MONTH:
            insert = "insert into job (tagid, type, dt, ts) values (%" PRId64 ",%d, strftime('%%Y-%%m-01T00:00:00', %" PRId64 ",'unixepoch'), %" PRId64 ");";
            break;
        case ROLLUP_YEAR:
            insert = "insert into job (tagid, type, dt, ts) values (%" PRId64 ",%d, strftime('%%Y-01-01T00:00:00', %" PRId64 ",'unixepoch'), %" PRId64 ");";
            break;
        default:
            return SQLITE_OK;
            break;
    }
    sprintf (query, insert, tagId, type, (int64_t)utc, (int64_t)utc);
    rc = execSql(db, query);
    if (rc == SQLITE_CONSTRAINT) {
        rc = SQLITE_OK;
    }
    return rc;
}

static int upsertRollup (sqlite3 *db, uint64_t tagId, int type, const char *dt, time_t ts, sqlite3_stmt *st) {
    int rc;
    char query[1024];
    const char *update = "update rollup set vsum=%g, vavg=%g, vmax=%g, vmin=%g, vcount=%" PRId64 " where "
                        "tagid=%" PRId64 " and "
                        "type=%d and dt='%s' and ts=%" PRId64 ";";
    const char *insert = "insert into rollup (tagid, type, dt, vsum, vavg, vmax, vmin, vcount, ts) "
                         "values (%" PRId64 ", %d, '%s', %g, %g, %g, %g, %" PRId64 ", %" PRId64 ");";
    double vsum =    sqlite3_column_double (st, 0);
    double vavg =    sqlite3_column_double (st, 1);
    double vmax =    sqlite3_column_double (st, 2);
    double vmin =    sqlite3_column_double (st, 3);
    int64_t vcount = sqlite3_column_int64  (st, 4);
    if (vcount == 0) {
        return SQLITE_OK;
    }
    sprintf (query, insert, tagId, type, dt, vsum, vavg, vmax, vmin, vcount, (int64_t)ts);
    rc = execSql (db, query);
    if (rc != SQLITE_OK) {
        if (rc == SQLITE_CONSTRAINT) {
            sprintf (query, update, vsum, vavg, vmax, vmin, vcount, tagId, type, dt, (int64_t)ts);
            rc = execSql (db, query);            
        } else {
            printf ("Error inserting rollup data\n");
        }
    }
    return rc;
}

static int rollupTagByYear (sqlite3 *db, int64_t id, int64_t tagId, const char *dt) {
    int rc = SQLITE_OK;
    char query[2048];
    time_t ts = iso8602ts(dt);
    time_t ts2 = timeAddYear(ts);
    time_t tdif = ts2 - ts;
    /* o ts - 1 abaixo eh para fazer com que os intervalos caiam no dia correto,
     do primeiro 00:15 ate o penultimo, nao vai fazer diferenca, mas para o ultimo
     que cai no outro dia, zero hora do dia seguinte,
     isto faz com que ele volte um segundo e ajuste o dia para tras*/
    const char *select = "select sum(vsum), avg(vavg), max(vmax), min(vmin), count(*)"
                        " from rollup "
                        " where "
                        " tagid =  %" PRId64 " and "
                        " type = %d and "
                        " ts >= %" PRId64 " and "
                        " ts <  %" PRId64 " ";
    sprintf (query, select, tagId, (int)ROLLUP_MONTH, (int64_t)ts, (int64_t)ts2);
    sqlite3_stmt *st = NULL;
    rc = sqlite3_prepare_v2(db, query, (int)(strlen(query)), &st, NULL);
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            upsertRollup(db, tagId, ROLLUP_YEAR, dt, ts, st);
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

static int rollupTagByMonth (sqlite3 *db, int64_t id, int64_t tagId, const char *dt) {
    int rc = SQLITE_OK;
    char query[2048];
    time_t ts   = iso8602ts(dt);
    time_t ts2  = timeAddMonth(ts);
    time_t tdif = ts2 - ts;
    /* o ts - 1 abaixo eh para fazer com que os intervalos caiam no dia correto,
     do primeiro 00:15 ate o penultimo, nao vai fazer diferenca, mas para o ultimo
     que cai no outro dia, zero hora do dia seguinte,
     isto faz com que ele volte um segundo e ajuste o dia para tras
    */
    const char *select = "select sum(vsum), avg(vavg), max(vmax), min(vmin), count(*)"
                        " from rollup "
                        " where "
                        " tagid =  %" PRId64 " and "
                        " type = %d and "
                        " ts >= %" PRId64 " and "
                        " ts < %" PRId64 " ";
    sprintf (query, select, tagId, (int)ROLLUP_DAY, (int64_t)ts, (int64_t)ts2);
    sqlite3_stmt *st = NULL;
    rc = sqlite3_prepare_v2(db, query, (int)(strlen(query)), &st, NULL);//1356912000
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            upsertRollup(db, tagId, ROLLUP_MONTH, dt, ts, st);
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

static int rollupTagByDay (sqlite3 *db, int64_t id, int64_t tagId, const char *dt) {
    int rc = SQLITE_OK;
    char query[2048];
    time_t ts = iso8602ts(dt);
    /* o ts - 1 abaixo eh para fazer com que os intervalos caiam no dia correto,
     do primeiro 00:15 ate o penultimo, nao vai fazer diferenca, mas para o ultimo
     que cai no outro dia, zero hora do dia seguinte,
     isto faz com que ele volte um segundo e ajuste o dia para tras*/
    const char *select = "select sum(vsum), avg(vavg), max(vmax), min(vmin), count(*)"
                        " from rollup "
                        " where "
                        " tagid =  %" PRId64 " and "
                        " type = %d and "
                        " ts >= %" PRId64 " and "
                        " ts < %" PRId64 " ";
    sprintf (query, select, tagId, (int)ROLLUP_HOUR, (int64_t)ts, (int64_t)ts + (3600 * 24));
    sqlite3_stmt *st = NULL;
    rc = sqlite3_prepare_v2(db, query, (int)(strlen(query)), &st, NULL);
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            upsertRollup(db, tagId, ROLLUP_DAY, dt, ts, st);
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

static int rollupTagByHour (sqlite3 *db, int64_t id, int64_t tagId, const char *dt) {
    int rc = SQLITE_OK;
    char query[2048];
    time_t ts = iso8602ts(dt);
    struct tm timeinfo;
    localtime_r (&ts, &timeinfo);
    time_t localts = ts + timeinfo.tm_gmtoff;
    char localdt[64];
    tt2iso8602(localts, localdt);
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
            upsertRollup(db, tagId, ROLLUP_HOUR, localdt, localts, st);
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
    const char *dtMask;
    int nextRollup;
    char query[1024];
    const char *select = "select id, tagid, dt from job "
                         " where "
                         " type = %d AND "
                         " dt < %s', %" PRId64 ", 'unixepoch') "
                         " ORDER BY tagid, ts;";
    switch (type) {
        case ROLLUP_HOUR:
            dtMask = "strftime('%Y-%m-%dT%H:00:00";
            nextRollup = ROLLUP_DAY;
            break;
        case ROLLUP_DAY:
            dtMask = "strftime('%Y-%m-%dT00:00:00";
            nextRollup = ROLLUP_MONTH;
            break;
        case ROLLUP_MONTH:
            dtMask = "strftime('%Y-%m-01T00:00:00";
            nextRollup = ROLLUP_YEAR;
            break;        
        case ROLLUP_YEAR:
            dtMask = "strftime('%Y-01-01T00:00:00";
            nextRollup = -1;
            break;
        default:
            return ~SQLITE_OK;
    }

    time_t utc = time(NULL);
    sprintf (query, select, type, dtMask, (int64_t)utc);
    sqlite3_stmt *st = NULL;
    rc = sqlite3_prepare_v2(db, query, (int)(strlen(query)), &st, NULL);
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            int64_t id =        sqlite3_column_int64 (st, 0);
            int64_t tagId =     sqlite3_column_int64 (st, 1);
            char *dt = (char *) sqlite3_column_text  (st, 2);
            switch (type) {
                case ROLLUP_HOUR:
                    rc = rollupTagByHour  (db, id, tagId, dt);
                    break;
                case ROLLUP_DAY:
                    rc = rollupTagByDay   (db, id, tagId, dt);
                    break;
                case ROLLUP_MONTH:
                    rc = rollupTagByMonth (db, id, tagId, dt);
                    break;
                case ROLLUP_YEAR:
                    rc = rollupTagByYear  (db, id, tagId, dt);
                    break;            }
            if (rc == SQLITE_OK) {
                const char *delete = "delete from job where id = %" PRId64 ";";
                sprintf (query, delete, id);
                execSql (db, query);
                time_t tt = iso8602ts (dt);
                if (nextRollup != -1) {
                    updateRollupControl (db, tagId, nextRollup, tt);
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

static void generateSampleData (sqlite3 *db, const char *startDate, const char *endDate, int timeInterval) {
    const char *insert = "insert into history (tagid, value, ts, dt) "
        "values (1, %g, %" PRId64 ", strftime('%%Y-%%m-%%dT%%H:%%M:%%S',%" PRId64 ",'unixepoch'));";
    char query[1024];
    time_t sd = iso8602ts (startDate);
    time_t ed = iso8602ts (  endDate);
    execSql (db, "delete from history;");
    execSql (db, "delete from rollup;");
    execSql (db, "delete from tag;");
    execSql (db, "delete from job;");
    execSql (db, "insert into tag (id, name) values (1, 'tagname');");
    execSql (db, "begin;");
    for (;sd <= ed; sd += timeInterval) {
        double v = 1;
        sprintf (query, insert, v, (int64_t)sd, (int64_t)sd);
        execSql (db, query);
        updateRollupControl (db, 1, ROLLUP_HOUR, sd);
    }
    execSql (db, "commit;");
}

void main (int argc, char *argv[]) {
    sqlite3 *db;
    int rc = sqlite3_open("./testdb.db3", &db);
    elapsedControl = time(NULL);
    if (rc == SQLITE_OK) {
        execSql (db, "PRAGMA journal_mode=WAL;");
        lap ("Start process");
        generateSampleData(db,"2000-01-01T00:00:00", "2014-01-02T02:30:00", 900);
        lap ("Simulated data done");
        doRollup(db);
        lap ("Rollup done");
        sqlite3_close(db);
    }
}   

