/***************************************************************************
 * 
 * Copyright (c) 2015 Sina.com, Inc. All Rights Reserved
 * $Id$ 
 * 
 **************************************************************************/
 
/**
 * @file ac_content_control.cpp
 * @date 2015/08/19 13:55:00
 * @version $Revision$ 
 * @brief 根据query添加verifiedtype字段
 *  
 **/

#include <cstdio>
#include <cstring>
#include <ctime>
#include <string>
#include <vector>
#include <tr1/unordered_map>
#include "jsoncpp/jsoncpp.h"
//#include "unicode.h"
#include "ac_content_control.h"
#include <utility.h>
#include "slog.h"
#define MAX_STR_LEN 512

bool AcContentControlData::Load(const char* path, const char* dict)
{
    char filename[1024];
    snprintf(filename, sizeof(filename), "%s/%s", path, dict);

	std::vector<std::string> keyword_verifiedtype_arr;
	int ret = util::get_lines(filename,keyword_verifiedtype_arr,false);
	if(ret != 0)
	{
	    SLOG_DEBUG("failed, ret: %d",ret);
		return false;
	}	

	Json::Reader reader;
	Json::Value root;
	std::vector<int> value;

	std::vector<std::string>::const_iterator iter = keyword_verifiedtype_arr.begin();
	for(; iter != keyword_verifiedtype_arr.end(); ++iter)
	{
		std::string json_str = *iter;
		if(reader.parse(json_str, root))
		{
		    std::string query = "";
		    std::string op = "";
		    int user_type = 0;
		    int auth_type = 0;
		    int stime = 0;
		    int etime = 0;

		    if(root.isMember("query"))
			query = root["query"].asString();
		    if(root.isMember("operation"))
			op = root["operation"].asString();
		    if(root.isMember("user_type"))
			user_type = root["user_type"].asInt();
		    if(root.isMember("auth_type"))
			auth_type = root["auth_type"].asInt();
		    if(root.isMember("stime"))
			stime = root["stime"].asInt();
		    if(root.isMember("etime"))
			etime = root["etime"].asInt();

		    value.clear();
		    value.push_back(auth_type + (user_type<<8));
	            value.push_back(stime);
		    value.push_back(etime);
		    mKeywordVerifiedtypeDict[query] = value;
		    /*
			if(op == "add" or op == "edit")
			{
			    value.clear();
			    value.push_back(auth_type + (user_type<<8));
			    value.push_back(stime);
			    value.push_back(etime);
			    mKeywordVerifiedtypeDict[query] = value;
			} else {
			    mKeywordVerifiedtypeDict.erase(query);
			}
		    */
		} 
	}
	return true;
}

int AcContentControlData::GetControlValue(const std::string& str, std::vector<int>& vtype) const
{
    /*
	char norm_str[512] = "";
	strncpy(norm_str, str.c_str(), str.length());
	norm_str[str.length()] = '\0';

	int norm_str_len = strlen(norm_str);
	utf8_normalize((unsigned char*)norm_str, norm_str_len);
*/
	keyword_info_dict_type::const_iterator iter = mKeywordVerifiedtypeDict.find(str);
	if(iter != mKeywordVerifiedtypeDict.end())
	{
		vtype = iter->second;
	} else return -1;

	return 0;
}
