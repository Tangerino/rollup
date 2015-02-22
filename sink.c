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

extern void *ctx;

static int handleMessage (sqlite3 *db, const char *msg) {
    int rc = 0;
    //printf ("SINK GOT %s\r\n", msg);
    cJSON *j = cJSON_Parse (msg);
    if (!j) {
        printf("Error before: [%s][%s]\n", cJSON_GetErrorPtr(), msg);
        return 1;
    }
    int64_t workerId;
    int64_t jobid;
    int64_t type;
    int64_t tagid;
    int64_t ts;
    rc  = jsonGetInt64(j, "workerid", &workerId);
    rc += jsonGetInt64(j, "jobid", &jobid);
    rc += jsonGetInt64(j, "type",  &type );
    rc += jsonGetInt64(j, "tagid", &tagid);
    rc += jsonGetInt64(j, "ts",    &ts   ); 

    if (rc == 0) { // so far so good
        cJSON *values = cJSON_GetObjectItem (j, "values");
        double vmax, vmin, vsum, vavg, vcount;
        if (!values) { //weird, but possible
            vmax = vmin = vsum = vavg = vcount = 0;
        } else {
            rc  = jsonGetDouble(values, "max", &vmax);
            rc += jsonGetDouble(values, "sum", &vsum);
            rc += jsonGetDouble(values, "min", &vmin);
            rc += jsonGetDouble(values, "count", &vcount);
            rc += jsonGetDouble(values, "avg", &vavg);
            if (rc) {
                printf ("Some value are missing [%s]\r\n", msg);
            }
        }
        upsertRollup(db, tagid, type, ts, vsum, vavg, vmax, vmin, vcount, jobid, workerId);
    } else {
        printf ("Some variables are missing [%s]\r\n", msg);    
    }
    return 0;
}
    
static int sink (sqlite3 *db, void *socket) {
    int rc = 0;
    char msgBuffer[DEFMSGSIZE];
    printf ("Sink thread is running\r\n");
    for (;;) {
        char *msg = zmqReceive(socket, msgBuffer, sizeof(msgBuffer));
        if (msg == NULL) {
            break;
        }
        int rc = handleMessage (db, msgBuffer);
        if (rc) {
            printf ("Error %d handling message. Sink thread is quitting\r\n", rc);
            break;
        }
    }
    printf ("Sink thread ended\r\n");
    return rc;
}

void sinkThread(void *arg) {
    void *receiver = zmq_socket (ctx, ZMQ_PULL); 
    assert (receiver);
    zmq_bind (receiver, "tcp://*:5558"); // PORT_B
    sqlite3 *db;
    int rc = sqlite3_open(DATABASE, &db);
    if (rc == SQLITE_OK) {
        execSql (db, "PRAGMA journal_mode=WAL;PRAGMA busy_timeout = 1000;");
        sink(db, receiver);
    }
    zmq_close(receiver);
    pthread_exit(NULL);
}
