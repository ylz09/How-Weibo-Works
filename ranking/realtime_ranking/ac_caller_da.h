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

#ifndef __AC_DA_CALLER__
#define __AC_DA_CALLER__

#include "ac_caller_da_base.h"


class AcDaCaller:public AcCallerDaBase {

public:
	const char * getName();
};


#endif //__AC_DA_CALLER__
