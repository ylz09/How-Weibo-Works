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
#ifndef __AC_QUERY_H__
#define __AC_QUERY_H__



#include "ac_include.h"
#include "ac_query_lib.h"
#include "ac_miniblog.h"
#include "ac_so.h"
#include "slog.h"
#include "da_cmd.h"
#include "ac_utils.h"
#include "ac_tool.h"
#include "string_utils.h"

#define ALL_BLUE_V 8
#define ALL_V 	9
#define NOT_V 10
/*
static const uint64_t VTYPE_NULL         = 0x0;  //空类型
static const uint64_t VTYPE_STAR         = 0x1;  //名人
static const uint64_t VTYPE_GOV          = 0x2;  //政府
static const uint64_t VTYPE_COM          = 0x4;  //企业
static const uint64_t VTYPE_MEDIA        = 0x8;  //媒体
static const uint64_t VTYPE_SCHL         = 0x10; //校园
static const uint64_t VTYPE_WEB          = 0x20; //网站
static const uint64_t VTYPE_APP          = 0x40; //应用
static const uint64_t VTYPE_ORG          = 0x80; //团体(机构)
static const uint64_t VTYPE_NONV         = 0x100;        //非V
static const uint64_t VTYPE_BLUEV        = 0x200;        //蓝V
static const uint64_t VTYPE_YELV         = 0x400;        //黄V
*/

int query_input(QsInfo *info);

char* query_work_pre(QsInfo *info, char* query);

char* query_filter_pre(QsInfo *info, int idx, int *dict);

char* query_filter_after(QsInfo *info);

char* query_lexicon_pre(QsInfo *info);

QsDAQuerys* query_bc(QsInfo *info, int idx);



#endif
