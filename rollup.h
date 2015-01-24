/* 
 * File:   rollup.h
 * Author: Carlos Tangerino
 *
 * Created on January 24, 2015, 10:20 AM
 */

#ifndef ROLLUP_H
#define	ROLLUP_H

#ifdef	__cplusplus
extern "C" {
#endif

#define DEFMSGSIZE 1024
#define DATABASE "./testdb.db3"
enum {
    ROLLUP_HOUR = 0,
    ROLLUP_DAY,
    ROLLUP_MONTH,
    ROLLUP_YEAR
} enAggregationType;


#ifdef	__cplusplus
}
#endif

#endif	/* ROLLUP_H */

