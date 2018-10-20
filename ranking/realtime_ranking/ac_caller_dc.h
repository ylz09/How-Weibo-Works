/***************************************************************************
 * 
 * Copyright (c) 2013 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file ac_dc_caller.h
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/01/04
 * @version 1.0
 * @brief 
 *  
 **/

#ifndef __AC_DC_CALLER__
#define __AC_DC_CALLER__

#include "ac_caller_dc_base.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "tools.h"
#include "sframe.h"
#include "ac_type.h"
#include "config.h"
#include "filter.h"
#include "slog.h"
#include "cache.h"
#include "triedict.h"
#include "dyndict_manager.h"
#include "call_data.h"
#include "ac_tool.h"
#include "ac_g_conf.h"


class AcDcCaller:public AcCallerDcBase {

public:
	const char * getName();
	int pre_one(void *data, int idx, char *output, int maxlen);
    int done_one(void *data, int idx, char *result, int len);
private:
	int done_all_rank(QsInfo * info, int * step, uint32_t * addr);
	int get_dc_result(QsInfo *info, DIDocHead *head_doc, SrmDocInfo *doc_info);
	int statistics_dc_result(QsInfo *info);
	int dc_dedup(QsInfo *info);
    bool get_char_field_value(QsInfo *info, DIDocHead *head_doc, int field_id,
                int type, std::string* content);
};

#endif //__AC_DC_CALLER__
