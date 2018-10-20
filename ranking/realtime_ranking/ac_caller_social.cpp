/***************************************************************************
 * 
 * Copyright (c) 2011 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file ac_da_caller.cpp
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/01/04
 * @version 1.0
 * @brief 
 *  
 **/

#include "cache_new.h"
#include "ac_caller_social.h"

const char * AcSocialCaller::getName() {
	return "social";
}

int AcSocialCaller::pre_one(void *data, int idx, char *output, int maxlen) {
	QsInfo *info = (QsInfo*)data;
	if (is_need_resort(info) && info->result.type_incache != PAGE_CACHE) {
		return AcCallerSocialBase::pre_one(data, idx, output, maxlen);
	}

	AC_WRITE_LOG(info, "\t(Social:ignore)");
	gettimeofday(&(info->socialinfo.start_time), NULL);

	return -1;
}

