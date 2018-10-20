/***************************************************************************
 * 
 * Copyright (c) 2011 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file ac_caller_man.h
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/01/04
 * @version 1.0
 * @brief 
 *  
 **/

#ifndef __AC_CALLER_MAN__
#define __AC_CALLER_MAN__

#include "ac_caller_bc.h"
#include "ac_caller_da.h"
#include "ac_caller_social.h"
#include "ac_caller_dc.h"



extern AcBcCaller bcCaller;
extern AcDaCaller daCaller;
extern AcSocialCaller socialCaller;
extern AcDcCaller dcCaller;

int myda_done(QsInfo *info, void *args, int *step);

#endif// __AC_CALLER_MAN__
