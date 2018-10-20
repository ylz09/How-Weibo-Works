/***************************************************************************
 *
 * Copyright (c) 2013 Sina.com, Inc. All Rights Reserved
 * 1.0
 *
 **************************************************************************/

/**
 * @file ac_sort.h
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/02/03
 * @version 1.0
 * @brief
 *
 **/
#ifndef __AC_SORT_H__
#define __AC_SORT_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "pthread.h"
#include "slog.h"
#include "resort.h"
#include "bitdup.h"
#include "ac_utils.h"
#include "ac_source_id_dict.h"
#include "ac_hot_ext_data_parser.h" 
//获取da_eva打上的重复标记，表示库中存在更早的内容相同的微博
#define GET_DUP_FLAG_OF_INDEX(qi) ((qi) & (1 << 14))
//白名单结果
#define GET_NOT_WHITE_FLAG(qi) ((qi) & (1 << 7))
//转发标记
#define GET_FORWARD_FLAG(digit_attr) ((digit_attr) & (0x1 << 2))

void ac_sort_init(void);

int ac_sort_social(QsInfo *info);

int ac_sort_filter(QsInfo *info);

int ac_abs_dup(QsInfo *info);

uint64_t ac_sort_uid(SrmDocInfo *doc);

void gettimelevel(QsInfo *info, int* timel, int* qtype);

int RankType(QsInfo *info);
int srm_resort_init(QsInfo *info);

int getSocialRelation(QsInfo * info, SrmDocInfo * doc);
bool is_good_uid(QsInfo *info, SrmDocInfo *doc);
bool is_valid_forward(SrmDocInfo *doc, const AcExtDataParser& attr, int valid_forward_fwnum);

#endif
