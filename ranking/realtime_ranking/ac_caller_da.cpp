/***************************************************************************
 * 
 * Copyright (c) 2011 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file ac_da_caller.cpp
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/01/04
 * @version 1.0
 * @brief 
 *  
 **/

#include "ac_caller_da.h"

#include "ac_caller_man.h"


extern ACServer *g_server;

const char * AcDaCaller::getName() {
	return "da";
}


int myda_done(QsInfo *info, void *args, int *step){
	double di_factor;
	int num, end, ret;



	if(info->bcinfo.cur_req == 0){
		if(info->params.flag & QSPARAMS_FLAG_BCSPEC){   //ֻ��ָ����ĳ��BC
			info->bcinfo.cur_req = info->params.bc_idx;
		}else{
			//DLLCALL(so, ac_dispatch, ret, info, mac_info->group_bc, mac_info->num, &info->bcinfo.cur_req);
			ret = ac_dispatch(info, bcCaller.machines, &info->bcinfo.cur_req);
		}
	}

	di_factor = ac_conf_di_factor(info);
	num = (int)(info->params.num * di_factor);
	end = info->params.start + num;
	info->bcinfo.req_num = MAX_DOCID_NUM < end ? MAX_DOCID_NUM : end;
	return 0;
}



