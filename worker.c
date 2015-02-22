/**
 * 
 * 2015 January 24
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
#include <zmq.h>
#include <pthread.h>
#include "sqlite3.h"
#include "CJson.h"
#include "jsonutil.h"
#include "support.h"

#define DEFMSGSIZE 1024
extern void *ctx;

/**
 * \brief Roll up data by hour
 * @param db The database connection
 * @param tagId The tag ID
 * @param ts The hour to be rolled up
 * @return 0 if all good
 */
static cJSON *rollupTag (cJSON *values) {
    double vsum   = 0;
    double vmax   = 0;
    double vmin   = 0;
    double vavg   = 0;
    long   vcount = 0;
    long   count  = cJSON_GetArraySize (values);
    if (count > 0) {
        vmax = 1e-99;
        vmin = 1e99;
        for (vcount = 0; vcount < count; vcount++) {
            cJSON *v = cJSON_GetArrayItem (values, vcount);
            double vv;
            if (v->type == cJSON_Number) {
                vv = v->valuedouble;
            } else if (v->type == cJSON_String) {
                vv = atof (v->valuestring);
            } else {
                printf ("holy cow, what now? type = %d\r\n", v->type);
                break;
            }
            if (vv > vmax)
                vmax = vv;
            if (vv < vmin)
                vmin = vv;
            vsum += vv;
        }
        if (vcount > 0) {
            vavg = vsum / vcount;
        }
    }
    cJSON *aggregatedValues = cJSON_CreateObject();
    if (aggregatedValues) {
        cJSON_AddNumberToObject (aggregatedValues, "sum",   vsum);
        cJSON_AddNumberToObject (aggregatedValues, "max",   vmax);
        cJSON_AddNumberToObject (aggregatedValues, "min",   vmin);
        cJSON_AddNumberToObject (aggregatedValues, "count", vcount);
        cJSON_AddNumberToObject (aggregatedValues, "avg",   vavg);
    }
    return aggregatedValues;
}

static int aggregate (int id, uint64_t jobid, uint64_t type, uint64_t tagId, uint64_t ts, cJSON *values, void *sender) {
    cJSON *aggregatedValues = NULL;
    switch (type) {
        case ROLLUP_HOUR:
            aggregatedValues = rollupTag  (values);            
            break;
        case ROLLUP_DAY:
            break;
        case ROLLUP_MONTH:
            break;
        case ROLLUP_YEAR:
            break;
        default:
            printf ("[%d] Invalid rollup type (%d)", id, (int)type);
            return 1;
    }    
    cJSON *reply = cJSON_CreateObject();
    if (reply == NULL) {
        printf ("[%d] Not good\r\n", id);
        return 1;
    }
    cJSON_AddNumberToObject(reply, "workerid", id);
    cJSON_AddNumberToObject(reply, "jobid", jobid);
    cJSON_AddNumberToObject(reply, "tagid", tagId);    
    cJSON_AddNumberToObject(reply, "type", type);
    cJSON_AddNumberToObject(reply, "ts", ts);
    cJSON_AddItemToObject(reply, "values", aggregatedValues);
    char *out = cJSON_Print(reply);
    if (out) {
        zmq_send(sender, out, strlen(out), 0);
        free(out);
    }
    cJSON_Delete(reply);
    return 0;
}

static int handleMessage (int id, const char *message, void *sender) {
    int rc = 0;
    //printf ("Worker [%d] got (%s)\r\n", id, message);
    cJSON *j = cJSON_Parse(message);
    if (!j) {
        printf("[%d] Error before: [%s]\n", id, cJSON_GetErrorPtr());
        return 1;
    }
    // The message format is:
    // jobid - The job ID
    // type - The aggregation type
    // tagid - The tag ID
    // ts - The time stamp
    int64_t jobid;
    int64_t type;
    int64_t tagid;
    int64_t ts;
    rc  = jsonGetInt64(j, "jobid", &jobid);
    rc += jsonGetInt64(j, "type",  &type );
    rc += jsonGetInt64(j, "tagid", &tagid);
    rc += jsonGetInt64(j, "ts",    &ts   );
    if (rc) {
        printf ("[%d] Malformed message %s\r\n", id, message);
        rc = 2;
    } else {
        cJSON *values = cJSON_GetObjectItem (j, "values");
        rc = aggregate(id, jobid, type, tagid, ts, values, sender);
    }
    cJSON_Delete (j);
    return rc;
}

static void worker(int id, void *receiver, void *sender) {
    char msgBuffer[DEFMSGSIZE];
    printf ("Worker %d running\r\n", id);
    for (;;) {
        char *msg = zmqReceive(receiver, msgBuffer, sizeof(msgBuffer));
        if (msg == NULL) {
            printf ("Worker %d finished\r\n", id);
            break;
        }
        int rc = handleMessage (id, msgBuffer, sender);
        if (rc) {
            printf ("Error %d handling message. Worker %d is quitting\r\n", rc, id);
            break;
        }
    }
}

/**
 * Analog inputs thread
 * @param arg
 * @return
 */
void workerThread(void *arg) {
    int workerId = *(int *) arg;
    void *receiver = zmq_socket (ctx, ZMQ_PULL); 
    assert (receiver);
    zmq_connect (receiver, "tcp://localhost:5557"); // PORT_A
    void *sender = zmq_socket (ctx, ZMQ_PUSH); 
    assert (sender);
    zmq_connect (sender, "tcp://localhost:5558");   // PORT_B
    worker(workerId, receiver, sender);
    printf ("Worker thread [%d] ended\r\n", workerId);
    zmq_close(receiver);
    zmq_close(sender);
    pthread_exit(NULL);
}


