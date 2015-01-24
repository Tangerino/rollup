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

char *zmqReceive(void *socket, char * messageBuffer, int bufferSize) {
    zmq_msg_t message;
    zmq_msg_init(&message);
    int size = zmq_msg_recv(&message, socket, 0);
    if (size == -1) {
        int en = errno;
        if (en != EAGAIN && en != ETERM) {
            printf("Got an empty message. Reason %s", zmq_strerror(en));
        }
        errno = en;
        return NULL;
    }
    if (size > (bufferSize - 1)) {
        zmq_msg_close(&message);
        printf("Buffer too small %d  -> %d", size, bufferSize); // should not fall here
        return NULL;

    }
    memcpy(messageBuffer, zmq_msg_data(&message), size);
    zmq_msg_close(&message);
    messageBuffer [size] = '\0';
    return messageBuffer;
}

