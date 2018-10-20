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
#ifndef __AC_HOT_SORT_H__
#define __AC_HOT_SORT_H__

#include <stdio.h>
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
#include "ac/src/plugins_weibo/ac_expr.h"
#include "ac_global_conf.h"
#include "dacore/resource.h"
#include "search_dict.h"
//获取da_eva打上的重复标记，表示库中存在更早的内容相同的微博
#define GET_DUP_FLAG_OF_INDEX(qi) ((qi) & (1 << 14))
//白名单结果
#define GET_NOT_WHITE_FLAG(qi) ((qi) & (1 << 7))
//转发标记
#define GET_FORWARD_FLAG(digit_attr) ((digit_attr) & (0x1 << 2))

struct Ac_hot_statistic {
	vec_expr_t exp;
	bool expr_init_ok;

	// for checkpicsource
    int uid_source_time_pass;
    int pass_by_validfans;
	int check_source_zone_normal_type;
	int check_source_zone_dict_type;
	int check_source_zone_normal_digit_type;
	int check_source_zone_dict_digit_type;
    int check_source_zone_qi_source;
    int check_source_zone_qi_or;
    int check_source_zone_qi_zone;
    int check_pic_source_dict;
    DASearchDictSet* pic_source;
	int curtime;
    int is_check_open;
    int ac_use_source_dict;
    int hot_cheat_check_qi_bits;
    bool check_pic_source_dict_query;

	Ac_hot_statistic() 
	{
		expr_init_ok = false;
		uid_source_time_pass = int_from_conf(UID_SOURCE_TIME_PASS, (3600*24*60));
		pass_by_validfans = int_from_conf(HOT_PIC_SOURCE_PASS_BY_VALIDFANS, 5000);
		check_source_zone_normal_type = int_from_conf(CHECK_SOURCE_ZONE_NORMAL_TYPE, 0);
		check_source_zone_dict_type = int_from_conf(CHECK_SOURCE_ZONE_DICT_TYPE, 0);
		check_source_zone_normal_digit_type = int_from_conf(CHECK_SOURCE_ZONE_NORMAL_DIGIT_TYPE, -1);
		check_source_zone_dict_digit_type = int_from_conf(CHECK_SOURCE_ZONE_DICT_DIGIT_TYPE, -1);
		check_source_zone_qi_source = int_from_conf(CHECK_SOURCE_ZONE_QI_SOURCE, 4);
		check_source_zone_qi_or = int_from_conf(CHECK_SOURCE_ZONE_QI_OR, 8);
		check_source_zone_qi_zone = int_from_conf(CHECK_SOURCE_ZONE_QI_ZONE, 131072);
		check_pic_source_dict = int_from_conf(CHECK_PIC_SOURCE_DICT, 1);
		pic_source = Resource::Reference<DASearchDictSet>("pic_source_normal");
		curtime = time(NULL);
		is_check_open = int_from_conf(REALTIME_HOTQUERY_CHEAT_CHECK, 1);
		ac_use_source_dict = int_from_conf(AC_USE_SOURCE_DICT, 1);
		hot_cheat_check_qi_bits = int_from_conf(HOT_CHEAT_CHECK_QI_BITS, 12);
		check_pic_source_dict_query = false;
	}
	~Ac_hot_statistic()
	{
		if (pic_source)
			pic_source->UnReference();
	}
};

void ac_sort_init(void);

int ac_sort_social(QsInfo *info);

int ac_sort_filter(QsInfo *info);

int ac_abs_dup(QsInfo *info);

uint64_t ac_sort_uid(SrmDocInfo *doc);

void gettimelevel(QsInfo *info, int* timel, int* qtype);


bool is_good_doc(QsInfo* info, SrmDocInfo* doc, int classifier, uint64_t qi, uint64_t fwnum, uint64_t cmtnum, SourceIDDict* dict);

int IsHotDoc(SrmDocInfo *doc, int time_level) ;
int RankType(QsInfo *info);
int srm_resort_init(QsInfo *info);
int IsNeedFilter(QsInfo *info, int curdoc, std::set<uint64_t> *dup_set, std::set<uint64_t> *cont_set,
		int dup_flag, int resort_flag, int *isdupset,
        std::set<uint64_t> *uid_set, int* isdup_cont);
#endif
