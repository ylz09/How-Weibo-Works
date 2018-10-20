/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * $Id$ 
 * 
 **************************************************************************/
 
/**
 * @file ac_sandbox.h
 * @author wuhua1(minisearch@staff.sina.com.cn)
 * @date 2014/10/27 03:34:12
 * @version $Revision$ 
 * @brief 
 *  
 **/


#ifndef  __AC_SANDBOX_H_
#define  __AC_SANDBOX_H_

#include <set>
#include "ac_qs_info.h"
#include "ac_intervene_dict.h"
#include "search_dict.h"

int ac_sandbox(QsInfo* info, std::set<uint64_t>* already_del_mid, bool need_refresh = false);

int ac_black_mids_mask(QsInfo* info, std::set<uint64_t>* already_del_mid, bool need_refresh = false);

int ac_sandbox_doc(QsInfo* info, SrmDocInfo *doc, int64_t uid, time_t delay_time,
            const auto_reference<DASearchDict>& suspect_uids);

#endif  //__AC_SANDBOX_H_


