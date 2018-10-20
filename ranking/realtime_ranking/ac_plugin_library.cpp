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

#include "ac_plugin_include.h"
#include "ac_caller_man.h"

enum{
	AC_PLUGIN_STEP_DA,
	AC_PLUGIN_STEP_BC,
	AC_PLUGIN_STEP_SOCIAL,
	AC_PLUGIN_STEP_DC,
	AC_PLUGIN_STEP_ALL
};

int AcPluginsLibrary::getPluginsNum() {
	return AC_PLUGIN_STEP_ALL;
}

AcBaseCaller *  AcPluginsLibrary::getPlugins(int i) {
	if (i == AC_PLUGIN_STEP_DA) {
		return &daCaller;
	}

	if (i == AC_PLUGIN_STEP_BC) {
		return &bcCaller;
	}

	if (i == AC_PLUGIN_STEP_SOCIAL) {
		return &socialCaller;
	}

	if (i == AC_PLUGIN_STEP_DC) {
		return &dcCaller;
	}
	return NULL;
}


