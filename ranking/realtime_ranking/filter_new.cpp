/***************************************************************************
 * 
 * Copyright (c) 2011 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file filter.c
 * @author wangjian6(wangjian6@staff.sina.com.cn)
 * @date 2011/10/26 12:16:50
 * @version 1.0
 * @brief
 *  
 **/

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include "tools.h"
#include "slog.h"
#include "ac_type.h"
#include "config.h"
#include "ac_g_conf.h"
#include "filter.h"

int filter_keyword(void *h, char *keyword, int dict) {
	Filter *filter = (Filter*) h;
	int dict_id;

	if (dict == 0 || keyword == NULL || *keyword == 0) {
		return 0;
	}
	dict_id = key_find(filter->root_current, keyword);
	if (dict_id == 0) {
		return 0;
	} else if (dict_id == 8) {
		return 1;
	} else if (dict_id & dict) {
		return 2;
	} else
		return dict_id;
}
