#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <math.h>
#include <string.h>
#include <inttypes.h>
#include "sqlite3.h"
#include "support.h"
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
        //seed += increment;
        execSql (db, query);
        addNewAggregationJob (db, tagId, ROLLUP_HOUR, sd);
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
        addNewAggregationJob (db, tagId, ROLLUP_HOUR, sd);
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
        addNewAggregationJob (db, tagId, ROLLUP_HOUR, sd);
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
        rc = addNewAggregationJob (db, tagId, ROLLUP_HOUR, ts);
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

void cleanUpTables (sqlite3 *db) {
    execSql (db, "delete from history;");
    execSql (db, "delete from rollup;");
    execSql (db, "delete from tag;");
    execSql (db, "delete from job;");     
}

void sampleData (sqlite3 *db) {
    const char *sd = "2010-01-01T00:00:00";
    const char *ed = "2011-01-03T01:30:00";
    generateSampleData (db, sd, ed, 900, 1, 1, 1);
    //        generateSampleData2(db, sd, ed, 900, 2, 32);
    //        generateSampleData3(db, sd, ed, 900, 3, 96);
    //        generateSampleData (db, sd, ed, 900, 130, 1, 1);
    //        getDataFromTable(db,  8408, 30);
    //        getDataFromTable(db, 38339, 30);
    //        getDataFromTable(db, 196342, 31);
    //        getDataFromTable(db, 196342, 30);
    //        lap ("Simulated data done");    
}
