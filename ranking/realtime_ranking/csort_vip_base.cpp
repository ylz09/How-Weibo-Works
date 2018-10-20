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
 **/

#include <stdlib.h>
#include <fstream>
#include "ac/src/plugins_weibo/csort_vip_base.h"
#include "include/intention_def.h"
#include "boost/algorithm/string.hpp"
#include "lib64/common/include/slog.h"
#include "lib64/dacore/include/dacore/resource.h"

BaseResource * CsortVipBaseDict::Clone() {
	return new (std::nothrow)CsortVipBaseDict();
}

bool CsortVipBaseDict::Load(const char *path, const char *dict) {
	int path_len = strlen(path);
	int dict_len = strlen(dict);
	int file_len = path_len + dict_len;
	char file[file_len + 1];
	int pos = 0;
	pos = snprintf(file + pos, file_len, "%s/", path);
	pos = snprintf(file + pos, file_len, "%s", dict);

	std::ifstream infile(file);

	std::string line;
	SLOG_WARNING("\npath[%s], dict[%s], file[%s]\n", path, dict, file);

	while (std::getline(infile, line)) {
		boost::trim(line);
		if (line.length() <= 0) {
			continue;
		}
		uint64_t uid = 0;
		uid = strtoull(line.c_str(), NULL, 10);
		m_user_dict.insert(uid);
	}
	SLOG_WARNING("\ndict:%s, size[%d]\n", dict, m_user_dict.size());
	return true;
}

bool CsortVipBaseDict::find(uint64_t uid) const {
	std::set<uint64_t>::const_iterator it = m_user_dict.find(uid);
	if (it != m_user_dict.end()) {
		return true;
	}
	return false;
}

uint32_t CsortVipBaseDict::count() const {
	return m_user_dict.size();
}

CsortVipBase::CsortVipBase() {
	m_type_path[VIP_USERS_BULEV] = "vip_users_bluev_uniq.txt";
	m_type_path[VIP_USERS_MOVIE] = "vip_users_movie.txt";
	m_type_path[VIP_USERS_ATTRACTION] = "vip_users_attraction.txt";
	m_type_path[VIP_USERS_MUSIC] = "vip_users_music.txt";
	m_type_path[VIP_USERS_BOOK] = "vip_users_book.txt";
	m_type_path[VIP_USERS_SPORT] = "vip_users_sport.txt";
	m_type_path[VIP_USERS_VARIETY] = "vip_users_variety.txt";
	m_type_path[VIP_USERS_TV] = "vip_users_tv.txt";
	m_type_path[VIP_USERS_INFLUENCE] = "vip_users_influence.txt";
	SLOG_WARNING("\nCsortVipBase::CsortVipBase");
}

void CsortVipBase::init() {
	Resource::Init("./data", "", true);
}

void CsortVipBase::init(std::string path) {
	Resource::Init(path.c_str(), "", true);
}

uint32_t GetVipType(uint64_t intention, int timel) {
	uint32_t vip_type = 0;
    switch (intention) {
        case INTENTION_MOVIE:
            vip_type |= (1 << VIP_USERS_MOVIE);
            break;
        case INTENTION_ATTRACTION:
            vip_type |= (1 << VIP_USERS_ATTRACTION);
            break;
        case INTENTION_MUSIC:
            vip_type |= (1 << VIP_USERS_MUSIC);
            break;
        case INTENTION_BOOK:
            vip_type |= (1 << VIP_USERS_BOOK);
            break;
        case INTENTION_SPORT:
            vip_type |= (1 << VIP_USERS_SPORT);
            break;
        case INTENTION_VARIETY:
            vip_type |= (1 << VIP_USERS_VARIETY);
            break;
        case INTENTION_TV:
            vip_type |= (1 << VIP_USERS_TV);
            break;
        default:
            break;
    }
	return vip_type;
    /*
	uint32_t vip_type = 0;
	if (timel >= 3) {
		vip_type |= (1 << VIP_USERS_MEDIA);
	}
	if ((intention & INTENTION_MOVIE) ||
		(intention & INTENTION_VARIETY) ||
		(intention & INTENTION_TV)) {
		vip_type |= (1 << VIP_USERS_MOVIE);
	}
	if (intention & INTENTION_IT) {
		vip_type |= (1 << VIP_USERS_INTERNET);
	}
	if (intention & INTENTION_SPORT) {
		vip_type |= (1 << VIP_USERS_FOOTBALL);
	}
	return vip_type;
    */
}

uint32_t CsortVipBase::Find(uint64_t uid, uint32_t type) const{
	std::map<uint32_t, std::string>::const_iterator it = m_type_path.find(type);
	if (it == m_type_path.end()) {
		SLOG_WARNING("\ntype:%u, error\n", type);
		return 0;
	}
	const char* path = it->second.c_str();
	CsortVipBaseDict *dict = Resource::Reference<CsortVipBaseDict>(path);//m_type_path[type].c_str());
	if (dict != NULL) {
		if (dict->find(uid)) {
			dict->UnReference();
			return type;
		}
		dict->UnReference();
	}
	return 0;
}

int CsortVipBase::CheckVipType(uint64_t uid, uint32_t vip_type) const {
    int type = CheckFamousUser(uid, vip_type);
    if (type) {
        return VIP_USER;
    }
    if (CheckMediaUser(uid, vip_type)) {
        return VIP_MEDIA;
    }
    return VIP_NONE;
}

int CsortVipBase::CheckFamousUser(uint64_t uid, uint32_t vip_type) const {
	uint32_t check_vip_type = 0;
	if ((vip_type & (1 << VIP_USERS_MOVIE)) && Find(uid, VIP_USERS_MOVIE)) {
        check_vip_type |= (1 << VIP_USERS_MOVIE);
	}
	if ((vip_type & (1 << VIP_USERS_ATTRACTION)) && Find(uid, VIP_USERS_ATTRACTION)) {
        check_vip_type |= (1 << VIP_USERS_ATTRACTION);
	}
	if ((vip_type & (1 << VIP_USERS_MUSIC)) && Find(uid, VIP_USERS_MUSIC)) {
        check_vip_type |= (1 << VIP_USERS_MUSIC);
	}
	if ((vip_type & (1 << VIP_USERS_BOOK)) && Find(uid, VIP_USERS_BOOK)) {
        check_vip_type |= (1 << VIP_USERS_BOOK);
	}
	if ((vip_type & (1 << VIP_USERS_SPORT)) && Find(uid, VIP_USERS_SPORT)) {
        check_vip_type |= (1 << VIP_USERS_SPORT);
	}
	if ((vip_type & (1 << VIP_USERS_VARIETY)) && Find(uid, VIP_USERS_VARIETY)) {
        check_vip_type |= (1 << VIP_USERS_VARIETY);
	}
	if ((vip_type & (1 << VIP_USERS_TV)) && Find(uid, VIP_USERS_TV)) {
        check_vip_type |= (1 << VIP_USERS_TV);
	}
	if (Find(uid, VIP_USERS_INFLUENCE)) {
        check_vip_type |= (1 << VIP_USERS_INFLUENCE);
	}
	return check_vip_type;
}

int CsortVipBase::CheckFamousUser(uint64_t uid, uint64_t intention, int timel, uint32_t vip_type) const {
    return CheckFamousUser(uid, vip_type);
}

int CsortVipBase::CheckMediaUser(uint64_t uid, uint32_t vip_type) const {
    return CheckMediaUser(uid, 0, 0, vip_type);
}

int CsortVipBase::CheckMediaUser(uint64_t uid, uint64_t intention, int timel, uint32_t vip_type) const {
	if (Find(uid, VIP_USERS_BULEV)) {
		return VIP_USERS_BULEV;
	}
	return 0;
}

bool CsortVipBase::CheckAttrValid(uint64_t stat, uint32_t type) {
	if (type == LOCAL_DOC_MEDIA_USER) {
/*
		if (stat & (0x1ll << 62)) {
			return false;
		}
*/
/*
		if (((stat & 0x3C00) == 0x3C00) && !(stat & (0x1ll << 32))) {
			return false;
		}
*/
	}
	return true;
}
