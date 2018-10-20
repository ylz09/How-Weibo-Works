/***************************************************************************
 * 
 * Copyright (c) 2013 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file ac_social_caller.h
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/01/04
 * @version 1.0
 * @brief 
 *  
 **/

#ifndef __AC_SOCIAL_CALLER__
#define __AC_SOCIAL_CALLER__

#include "ac_caller_social_base.h"

class AcSocialCaller:public AcCallerSocialBase {

public:
	int pre_one(void *data, int idx, char *output, int maxlen);
	const char * getName();

};

#endif //__AC_SOCIAL_CALLER__
