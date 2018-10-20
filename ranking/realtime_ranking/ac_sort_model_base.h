/***************************************************************************
 * 
 * Copyright (c) 2013 Sina.com, Inc. All Rights Reserved
 * $Id$ 
 * 
 **************************************************************************/
 
/**
 * @file ac_sort_model_base.h
 * @author wuhua1(minisearch@staff.sina.com.cn)
 * @date 2013/08/22 18:16:00
 * @version $Revision$ 
 * @brief 
 *  
 **/


#ifndef  __AC_SORT_MODEL_BASE_H_
#define  __AC_SORT_MODEL_BASE_H_

static const int SOCIAL_UP_FOLLOWER = 2;
static const int SOCIAL_RESERVE_NUM = 3;

static const int LEFT_COUNT = 1000;
static const int SORT_COUNT = 1500;
static const long MAX_UID_SIZE = 0x100000000;
static const long MAX_SOURCE_SIZE = 0x01000000;

static const int ONEDAYTIME = 86400;
static const int MAXMOVICETIME = 3600*24*3;
static const int MAX_MOVICE_ADJUST = 3;
static const int MAX_GOOD_NUM = 100;
static const int MIN_GOOD_NUM = 10;
static const int HISTORYBASE = 1;

static const int NEED_MAX_GOOD_NUM = 1200;
static const int MAX_ABS_DUP_NUM = 100;
static const int HOTFRONTNUM_MOBILE = 30;
static const int HOTFRONTNUM_PC = 5;
static const int EXPANDNUM = 3;

static const unsigned VALUE_EXTRA = 1600000000;

enum {
	RANK_SOCIAL = 0, RANK_TIME = 1, RANK_HOT = 2, RANK_FWNUM = 3, RANK_CMTNUM = 4, RANK_OTHER = 5
};

enum {
	DOC_TIME = 0, DOC_HOT = 1, DOC_SOCIAL = 2, DOC_EXPAND = 3, DOC_MEDIA_USER = 4, DOC_FAMOUS_USER = 5, DOC_ADJUST = 6
};

inline bool time_cmp(const SrmDocInfo * a, const SrmDocInfo *b) {
    return a->rank > b->rank;
}
class TimeSortCmp 
{
public:
	bool operator()(const SrmDocInfo * a, const SrmDocInfo*b) const {
        return time_cmp(a, b);
	}   
};

bool fwnum_cmp(const SrmDocInfo * a, const SrmDocInfo *b);

class FwnumSortCmp
{
public:
	bool operator()(const SrmDocInfo * a, const SrmDocInfo*b) const;
};

bool cmtnum_cmp(const SrmDocInfo * a, const SrmDocInfo *b);
class CmtnumSortCmp
{
public:
	bool operator()(const SrmDocInfo * a, const SrmDocInfo*b) const;
};

#endif  //__AC_SORT_MODEL_BASE_H_
