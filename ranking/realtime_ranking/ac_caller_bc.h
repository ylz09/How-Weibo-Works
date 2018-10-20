/***************************************************************************
 * 
 * Copyright (c) 2011 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file ac_da_caller.h
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/01/04
 * @version 1.0
 * @brief 
 *  
 **/

#ifndef __AC_BC_CALLER__
#define __AC_BC_CALLER__

#include "ac_caller_bc_base.h"


class AcBcCaller:public AcCallerBcBase {
public:
	int bc_after(QsInfo *info, int *step);
private:
    int done_all_postrank(QsInfo * info, int * step, uint32_t *addr);
	int bc_pre_sample(QsInfo * info, int idx, char * output, int maxlen);
	int bc_pre_normal(QsInfo * info, int idx, char * output, int maxlen);
    void bc_save_result(QsInfo *info, int idx, BCResponse *response, int *filter_count);
    void bc_save_result_socialaction(QsInfo *info, int idx, BCResponse *response, int *filter_count);
	bool isSampleIdx(int idx, QsInfo * info);
	int done_all_rank(QsInfo * info);
	const char * getName();


};

#endif
