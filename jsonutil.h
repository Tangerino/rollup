/* 
 * File:   jsonutil.h
 * Author: carlos
 *
 * Created on October 13, 2013, 10:11 AM
 */

#ifndef JSONUTIL_H
#define	JSONUTIL_H

#ifdef	__cplusplus
extern "C" {
#endif
#include "cJSON.h"
int jsonToInt64 (const char *json, const char *element, int64_t *value);
char *jsonToString (const char *json, const char *element);
int jsonToDouble(const char *json, const char *element, double *value);
int jsonGetInt64 (cJSON *j, const char *element, int64_t *value);
int jsonGetDouble (cJSON *j, const char *element, double *value);
int jsonGetString (cJSON *j, const char *element, char **value);
int jsonGetBoolean (cJSON *j, const char *element, int *value) ;

#ifdef	__cplusplus
}
#endif

#endif	/* JSONUTIL_H */

