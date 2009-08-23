#ifndef MCL_MD5_H
#define MCL_MD5_H
#include <stdio.h>

typedef unsigned char *POINTER;
typedef unsigned short int UINT2;
typedef unsigned long int UINT4;

typedef struct {
  UINT4 state[4];				 /* state (ABCD) */
  UINT4 count[2];				/* number of bits, modulo 2^64 (lsb first) */
  unsigned char buffer[64];		/* input buffer */
} MD5_CTX;

#ifdef __cplusplus
extern "C" {
#endif

int MD5String(char *string,unsigned char digest[16]);
int MD5File(char *filename,unsigned char digest[16]);
int MD5Buffer(char *buffer,unsigned int len,unsigned char digest[16]);

#ifdef __cplusplus
}
#endif

#endif
