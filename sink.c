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
#include "rollup.h"
#include "support.h"

static int handleMessage (const char *msg) {
    printf ("SINK GOT %s\r\n", msg);
    return 0;
}

static int sink (sqlite3 *db, void *socket) {
    int rc = 0;
    char msgBuffer[DEFMSGSIZE];
    printf ("Sink thread is running\r\n");
    for (;;) {
        char *msg = zmqReceive(socket, msgBuffer, sizeof(msgBuffer));
        if (msg == NULL) {
            printf ("Sink thread finished\r\n");
            break;
        }
        int rc = handleMessage (msgBuffer);
        if (rc) {
            printf ("Error %d handling message. Sink thread is quitting\r\n", rc);
            break;
        }
    }
    return rc;
}

void sinkThread(void *arg) {
    void *ctx = zmq_ctx_new();
    assert (ctx);
    void *receiver = zmq_socket (ctx, ZMQ_PULL); 
    assert (receiver);
    zmq_bind (receiver, "tcp://*:5558"); // PORT_B
    sqlite3 *db;
    int rc = sqlite3_open(DATABASE, &db);
    if (rc == SQLITE_OK) {
        sink(db, receiver);
    }
    zmq_close(receiver);
    zmq_ctx_destroy(ctx);
    pthread_exit(NULL);
}



