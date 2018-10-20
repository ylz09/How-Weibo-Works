/***************************************************************************
 * 
 * Copyright (c) 2011 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file csort_vip_base.h
 * @author guanli(guanli@staff.sina.com.cn)
 * @date 2014/03/20
 * @version 1.0
 *  
 **/

#ifndef __AC_SRC_PLUGINS_WEIBO_CSORT_VIP_BASE_H__
#define __AC_SRC_PLUGINS_WEIBO_CSORT_VIP_BASE_H__

#include <map>
#include <set>
#include <string>
#include "lib64/dacore/include/dacore/base_resource.h"

enum {
        VIP_USERS_NONE = 0,
        VIP_USERS_BULEV = 1,
        VIP_USERS_MOVIE = 2,    // INTENTION_MOVIE
        VIP_USERS_ATTRACTION = 3,   // INTENTION_ATTRACTION
        VIP_USERS_MUSIC = 4,    // INTENTION_MUSIC
        VIP_USERS_BOOK = 5, // INTENTION_BOOK
        VIP_USERS_SPORT = 6,    // INTENTION_SPORT
        VIP_USERS_VARIETY = 7,  // INTENTION_VARIETY
        VIP_USERS_TV = 8,   // INTENTION_TV
        VIP_USERS_INFLUENCE = 9,   // 
};
enum {
    VIP_NONE = 0,
    VIP_USER = 1,
    VIP_MEDIA = 2,
};

enum {
	LOCAL_DOC_MEDIA_USER = 4, LOCAL_DOC_FAMOUS_USER = 5,
};

uint32_t GetVipType(uint64_t intention, int timel);

class CsortVipBaseDict : public BaseResource {
public:
	//@override
	bool Load(const char *path, const char *dict);
	//@Override
	BaseResource * Clone();
	bool find(uint64_t uid) const;
	uint32_t count() const;
private:
	std::set<uint64_t> m_user_dict;
};

class CsortVipBase {
  public:
	CsortVipBase();
	~CsortVipBase() {}
	void init();
	void init(std::string path);
	int CheckVipType(uint64_t uid, uint32_t vip_type) const;
	int CheckMediaUser(uint64_t uid, uint32_t vip_type) const;
	int CheckMediaUser(uint64_t uid, uint64_t intention, int timel, uint32_t vip_type) const;
	int CheckFamousUser(uint64_t uid, uint32_t vip_type) const;
	int CheckFamousUser(uint64_t uid, uint64_t intention, int timel, uint32_t vip_type) const;
	bool CheckAttrValid(uint64_t stat, uint32_t type);
  private:
	uint32_t Find(uint64_t uid, uint32_t type) const;

	std::map<uint32_t, std::string> m_type_path;

};
#endif //__AC_SRC_PLUGINS_WEIBO_CSORT_VIP_BASE_H__
