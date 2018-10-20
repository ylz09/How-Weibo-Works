/***************************************************************************
 *
 * Copyright (c) 2013 Sina.com, Inc. All Rights Reserved
 * 1.0
 *
 **************************************************************************/

/**
 * @file ac_dispatch.h
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/02/03
 * @version 1.0
 * @brief
 *
 **/

#ifndef __AC_DISPATCH_H__
#define __AC_DISPATCH_H__

#include <vector>
#include "slog.h"
#include "config.h"



int ac_dispatch_mem(QsInfo* info);

int ac_dispatch_sample(QsInfo *info, std::vector<MGroup *>&  groups, uint32_t *idx);

int ac_dispatch(QsInfo *info, std::vector<MGroup *>&  groups, uint32_t *idx);



#endif

