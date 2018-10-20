/***************************************************************************
 *
 * Copyright (c) 2013 Sina.com, Inc. All Rights Reserved
 * 1.0
 *
 **************************************************************************/

/**
 * @file ac_filter.h
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/02/03
 * @version 1.0
 * @brief
 *
 **/

#ifndef __AC_FILTER_H__
#define __AC_FILTER_H__

#include <vector>
#include "slog.h"
#include "config.h"



void ac_filter_init(void);

int ac_filter_output(const char* src, uint64_t uid, int status);

int ac_filter_keyword(const char* src);


#endif

