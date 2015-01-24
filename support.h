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

char *zmqReceive(void *socket, char * messageBuffer, int bufferSize);


#ifdef	__cplusplus
}
#endif

#endif	/* SUPPORT_H */

