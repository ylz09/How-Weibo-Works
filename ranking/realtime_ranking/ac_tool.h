/***************************************************************************
 * 
 * Copyright (c) 2013 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file ac_tool.h
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/01/04
 * @version 1.0
 * @brief 
 *  
 **/

#include "ac_include.h"


#ifndef AC_TOOL_H_
#define AC_TOOL_H_



void* ac_malloc(QsInfo *info, size_t size);

int ac_md5(QsInfo *info, char *query, int query_len, uint8_t bytes[16]);

char* ac_get_query(QsInfo *info, const char *query, int query_len);

int ac_add_event(QsInfo *info, ACEventEnum etype, func_event call_event, void *args, int status);

int ac_del_event(QsInfo *info, int event_id);

int ac_start_event(QsInfo *info, ACEventEnum etype, int *step);
int ac_bc_search(QsInfo *info, int *step);

void* ac_parse_json(char *request, int len);

int sid_match(const char* sid, SID_ARRAY* p_sid);

#endif
     
