/***************************************************************************
 * 
 * Copyright (c) 2015 Sina.com, Inc. All Rights Reserved
 * $Id$ 
 * 
 **************************************************************************/
 
/**
 * @file ac_content_control.h
 * @date 2015/08/19 13:55:00
 * @version $Revision$ 
 * @brief 根据query添加verifiedtype字段
 *  
 **/

#ifndef _AC_CONTENT_CONTROL_H_
#define _AC_CONTENT_CONTROL_H_

#include <string>
#include <tr1/unordered_map>
#include <assert.h>
#include <stdint.h>
#include <dacore/base_resource.h>
#include "odict.h"
#include <vector>

//@name: AcContentControlData
//@brief: 根据query返回ac需要的一些参数
class AcContentControlData: public BaseResource
{
public:
	AcContentControlData()
	{}

	~AcContentControlData()
	{}

    virtual bool Load(const char* path, const char* dict);

	int GetControlValue(const std::string& str, std::vector<int>& vtype) const;

	BaseResource* Clone()
	{
		return new (std::nothrow) AcContentControlData();
	}

private:
	typedef std::tr1::unordered_map<std::string, std::vector<int> > keyword_info_dict_type;

	keyword_info_dict_type mKeywordVerifiedtypeDict;	// 关键词-认证用户类型词典
};

#endif
