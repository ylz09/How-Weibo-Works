#ifndef _RESORT_H
#define	_RESORT_H
#include "ac_miniblog.h" 
#define HARD_SEARCH

#define RESORT_MAX_NUM 500

#define R_AT(result, i) ((Result*)result + i)
#define R_DEL(result, i)	(((Result*)result + i)->delflag = 1)

#define R_UID(ptr)	(((Result*)ptr)->uid)
#define R_QI(ptr)	0
#define R_MID(ptr)	0
#define R_SOURCE(ptr)	(((Result*)ptr)->attr.digit_source)
#define R_WHITE_SOURCE(ptr)	(((Result*)ptr)->iswhite == 1)

#define R_LOG(fmt,...)

typedef struct{
	Attr	attr;
	uint32_t uid;
	uint32_t digit_time;
	uint32_t score;
	uint8_t	delflag:1;
	uint8_t	iswhite:1;
}Result;

#ifdef __cplusplus
extern "C" {
#endif
int resort(void *result, int num, void *env);

#ifdef __cplusplus
}
#endif

#endif
