/* 
 * File:   samples.h
 * Author: carlos
 *
 * Created on January 25, 2015, 7:30 AM
 */

#ifndef SAMPLES_H
#define	SAMPLES_H

#ifdef	__cplusplus
extern "C" {
#endif

void sampleData (sqlite3 *db);
void cleanUpTables (sqlite3 *db);

#ifdef	__cplusplus
}
#endif

#endif	/* SAMPLES_H */

