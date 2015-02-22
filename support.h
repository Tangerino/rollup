/* 
 * File:   support.h
 * Author: carlos
 *
 * Created on January 24, 2015, 3:19 PM
 */

#ifndef SUPPORT_H
#define	SUPPORT_H

#ifdef	__cplusplus
extern "C" {
#endif
#define PI2 (M_PI * 2)
    
#define ISGMT 0
#define DEFMSGSIZE 1024
#define DATABASE "./testdb.db3"
enum {
    ROLLUP_HOUR = 1,
    ROLLUP_DAY,
    ROLLUP_MONTH,
    ROLLUP_YEAR
} enAggregationType;

char *zmqReceive(void *socket, char * messageBuffer, int bufferSize);
time_t getStartOfYear  (time_t ts);
time_t getStartOfMonth (time_t ts);
time_t getStartOfDay   (time_t ts);
time_t getStartOfHour  (time_t ts);
int execSql(sqlite3 *db, const char *sql);
char *tt2iso8602 (time_t tt, char *dt);
time_t iso8602ts (const char *isoDate);
int addNewAggregationJob (sqlite3 *db, int64_t tagId, int type, time_t utc);
void newTag (sqlite3 *db, int64_t tagId);
time_t timeAddMonth (time_t ts);
time_t timeAddYear (time_t ts);
int upsertRollup (sqlite3 *db, uint64_t tagId, int type, time_t ts, 
        double vsum, double vavg, double vmax, double vmin, int64_t vcount, 
        int64_t jobId, int64_t worker);

#ifdef	__cplusplus
}
#endif

#endif	/* SUPPORT_H */

