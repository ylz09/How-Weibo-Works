/***************************************************************************
 * 
 * Copyright (c) 2011 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file ac_minibloguser.h
 * @author wangjian6(wangjian6@staff.sina.com.cn)
 * @date 2011/09/27 10:08:17
 * @version 1.0
 * @brief 
 * 搜人的宏定义
 *  
 **/

#ifndef _AC_MINIBLOGUSER_H
#define _AC_MINIBLOGUSER_H

typedef struct{
	uint64_t digit_attr:8;		//0:8
	uint64_t status:5;		//8:5
	uint64_t level:3;		//13:3
	uint64_t digit_source:24;	//16:24
	uint64_t qi:24;			//40:24

	uint64_t user_type:8;		//64:8
	uint64_t verified:1;		//72:1
	uint64_t verified_type:3;	//73:3
	uint64_t dup:52;		//76:52
}Attr;

#endif
