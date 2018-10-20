/***************************************************************************
 * 
 * Copyright (c) 2011 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file call_monitor.c
 * @author wangjian6(wangjian6@staff.sina.com.cn)
 * @date 2011/08/03 10:08:17
 * @version 1.0
 * @brief 
 **/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "tools.h"
#include "sframe.h"
#include "ac_type.h"
#include "acmpool.h"
#include "call_data.h"

extern ACServer *g_server;
extern void *g_pool;

int call_mon(int argc, char **argv, char *reply, int len){
	char *cmd;

	cmd = argv[0];
	if(strcmp(cmd, "mem") == 0){
		int num, total, left;
		acmpool_stats(g_pool, &num, &total, &left);
		snprintf(reply, len, "num:%d\ttotal:%dM\tleft:%dM", num, total, left);
	}else if(strcmp(cmd, "memdump") == 0){
		if(argc < 2){
			sprintf(reply, "usage:memdup filename");
		}else{
			acmpool_dump(g_pool, argv[1]);
			sprintf(reply, "ok");
		}
	}else{
		snprintf(reply, len, "unrecognized cmd:%s\n", cmd);
	}
	return 0;
}
