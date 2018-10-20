/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * $Id$ 
 * 
 **************************************************************************/
 
/**
 * @file ac_intervene_dict.cpp
 * @author wuhua1(minisearch@staff.sina.com.cn)
 * @date 2014/10/28 01:51:20
 * @version $Revision$ 
 * @brief 
 *  
 **/


#include "ac_intervene_dict.h"
#include <fstream>
#include "string_utils.hpp"
#include "slog.h"
#include "boost/lexical_cast.hpp"
#include "boost/algorithm/string.hpp"

#define MAX_INNER_UID 1024

BaseResource * User2ClassDict::Clone() {
	return new (std::nothrow) User2ClassDict();
}

bool User2ClassDict::Load(const char * path, const char * dict) {
	char filename[2048];
	snprintf(filename, sizeof(filename), "%s/%s", path, dict);

	std::ifstream fin(filename);
	if (!fin) {
		SLOG_FATAL("open resource[%s] failed", filename);
		return false;
	}

    SLOG_NOTICE("User2ClassDict::Load[%s]", filename);

	char str[MAX_INNER_UID];

	while (fin.getline(str, MAX_INNER_UID)) {
		uint64_t uid;
		int type;
		string_utils::string_list tokens = string_utils::split(str, "\t");
		if (tokens.size() != 2) {
			SLOG_WARNING("unvalid string %s", str);
			continue;
		}

		try {
			uid = boost::lexical_cast<uint64_t>(tokens.front());
			tokens.pop_front();
			type = boost::lexical_cast<int>(tokens.front());
			tokens.pop_front();
		} catch (boost::bad_lexical_cast const&) {
			SLOG_WARNING("can not convert %s", str);
			continue;
		}

		uid2class.insert(std::map<uint64_t, int>::value_type(uid, type));
	}
	return true;
}

int User2ClassDict::find(uint64_t uid) {
	std::map<uint64_t, int>::iterator it;
	it = uid2class.find(uid);
	if (it != uid2class.end()) {
		return it->second;
	}
	return -1;
}

int User2ClassDict::count() {
	return uid2class.size();
}

BaseResource * Query2ClassDict::Clone() {
	return new (std::nothrow) Query2ClassDict();
}

bool Query2ClassDict::Load(const char *path, const char *dict) {
	char filename[2048];
	snprintf(filename, sizeof(filename), "%s/%s", path, dict);

	std::ifstream fin(filename);
	if (!fin) {
		SLOG_FATAL("open resource[%s] failed", filename);
		return false;
	}

    SLOG_NOTICE("Query2ClassDict::Load[%s]", filename);

	char str[MAX_LINE_LEN];

	while (fin.getline(str, MAX_LINE_LEN)) {
		std::string query;
		std::vector<uint64_t> docids;

		string_utils::string_list tokens = string_utils::split(str, "\t");
		if (tokens.size() < 2) {
			SLOG_WARNING("without query or only query %s", str);
			continue;
		}

		try {
			query = boost::lexical_cast<std::string>(tokens.front());
			SLOG_NOTICE("TEST_BAN:%s", query.c_str());
			tokens.pop_front();
			while (tokens.size()) {
				docids.push_back(boost::lexical_cast<uint64_t>(tokens.front()));
				SLOG_NOTICE("mid:%ld", boost::lexical_cast<uint64_t>(tokens.front()));
				tokens.pop_front();
			}

		} catch (boost::bad_lexical_cast const&) {
			SLOG_WARNING("can not convert %s", str);
			continue;
		}
		query2class.insert(std::map<std::string, std::vector<uint64_t> >::value_type(query, docids));

	}
	return true;
}

uint64_t Query2ClassDict::find(char *query, int address) {
	if (query == NULL)
		return 0;
	std::string q(query);
	std::map<std::string, std::vector<uint64_t> >::iterator it;
	it = query2class.find(q);
	if (it == query2class.end())
		return 0;
	std::vector<uint64_t> mids = it->second;
	if (address >= 0 && unsigned(address) < mids.size())
		return mids[address];
	return 0;
}

int Query2ClassDict::count(int address) {
	std::map<std::string, std::vector<uint64_t> >::const_iterator it;
	int result = 0;
	for (it = query2class.begin(); it != query2class.end(); it++) {
		if (address >= 0 && (it->second).size() > unsigned(address))
			result += 1;
	}
	return result;
}


bool ACStringSet::Load(const char *path, const char *dict)
{
	char filename[2048];
	snprintf(filename, sizeof(filename), "%s/%s", path, dict);

	std::ifstream fin(filename);
	if (!fin) {
		SLOG_FATAL("open resource[%s] failed", filename);
		return false;
	}

    SLOG_NOTICE("User2ClassDict::Load[%s]", filename);

	char str[MAX_INNER_UID];

	while (fin.getline(str, MAX_INNER_UID)) {
		m_string_set.insert(str);
	}
	return true;
}

BaseResource * ACStringSet::Clone()
{
	return new (std::nothrow) ACStringSet();
}

bool ACStringSet::test(const std::string & key) const
{
	return m_string_set.find(key) != m_string_set.end();
}

bool ACStringSet::test(const char * key) const
{
	if (key == NULL) {
		return false;
	}

	return m_string_set.find(key) != m_string_set.end();
}

BaseResource * ACLongSet::Clone() {
	return new (std::nothrow)ACLongSet();
}

bool ACLongSet::Load(const char *path, const char *dict) {
	char filename[2048];
	snprintf(filename, sizeof(filename), "%s/%s", path, dict);

	std::ifstream infile(filename);
	if (!infile) {
		SLOG_WARNING("ACLongSet::Load open %s error", filename);
		return false;
	}

	std::string line;
	while (std::getline(infile, line)) {
		boost::trim(line);
		if (line.length() <= 0) {
			continue;
		}
		uint64_t uid = 0;
		uid = strtoull(line.c_str(), NULL, 10);
		m_uid_dict.insert(uid);
	}
	SLOG_NOTICE("ACLongSet::Load %s, size[%d]", filename, m_uid_dict.size());
	return true;
}

bool ACLongSet::find(uint64_t uid) const {
	return (m_uid_dict.find(uid) != m_uid_dict.end());
}

uint32_t ACLongSet::count() const {
	return m_uid_dict.size();
}

BaseResource * AcInterventionDel::Clone() {
	return new (std::nothrow) AcInterventionDel();
}
bool AcInterventionDel::Load(const char *path, const char *dict) {
	int path_len = strlen(path);
	int dict_len = strlen(dict);
	int file_len = path_len + dict_len;
	char file[file_len + 1];
	int pos = 0;
	pos = snprintf(file + pos, file_len, "%s/", path);
	pos = snprintf(file + pos, file_len, "%s", dict);

	std::ifstream fin(file);

	char str[MAX_LINE_LEN];

	while (fin.getline(str, MAX_LINE_LEN)) {
		std::string query;
		uint64_t docid = 0;
		uint32_t category = 0;
		string_utils::string_list tokens = string_utils::split(str, "\t");
		if (tokens.size() != 3) {
			SLOG_WARNING("unvalid string %s", str);
			continue;
		}
		try {
			query = boost::lexical_cast<std::string>(tokens.front());
			tokens.pop_front();
			category = boost::lexical_cast<uint32_t>(tokens.front());
			tokens.pop_front();
			docid = boost::lexical_cast<uint64_t>(tokens.front());
			//docid = strtoull(tokens.front().c_str(), NULL, 10);
			tokens.pop_front();
			SLOG_DEBUG("TEST_BAN:%s %llu", query.c_str(), docid);
		} catch (boost::bad_lexical_cast const&) {
			SLOG_WARNING("can not convert %s", str);
			continue;
		}
		std::map<std::string, std::set<uint64_t> >::iterator it = query_dict.find(query);
		if (it == query_dict.end()) {
			std::set<uint64_t> data;
			data.insert(docid);
			query_dict[query] = data;
		} else {
			it->second.insert(docid);
		}
	}
	return true;
}

uint64_t AcInterventionDel::find(char *query, uint64_t docid) const {
	if (query == NULL)
		return 0;
	std::string q(query);
	std::map<std::string, std::set<uint64_t> >::const_iterator it = query_dict.find(q);
	if (it != query_dict.end()) {
		std::set<uint64_t>::const_iterator it_docid = it->second.find(docid);
		if (it_docid != it->second.end()) {
			return 1;
		} else {
			return 0;
		}
	}
	return 0;
}

int AcInterventionDel::count() const {
	return query_dict.size();
}

const std::map<std::string, std::set<uint64_t> >& AcInterventionDel::Data() const {
	return query_dict;
}

BaseResource * AcInterventionAdd::Clone() {
	return new (std::nothrow) AcInterventionAdd();
}
bool AcInterventionAdd::Load(const char *path, const char *dict) {
	none_docid_.clear();
	int path_len = strlen(path);
	int dict_len = strlen(dict);
	int file_len = path_len + dict_len;
	char file[file_len + 1];
	int pos = 0;
	pos = snprintf(file + pos, file_len, "%s/", path);
	pos = snprintf(file + pos, file_len, "%s", dict);

	std::ifstream fin(file);

	char str[MAX_LINE_LEN];

	while (fin.getline(str, MAX_LINE_LEN)) {
		std::string query;
		uint64_t docid = 0, docid_2 = 0, docid_3 = 0;
		uint32_t category = 0;
		string_utils::string_list tokens = string_utils::split(str, "\t");
		if (tokens.size() != 5) {
			SLOG_WARNING("unvalid string %s", str);
			continue;
		}
		try {
			query = boost::lexical_cast<std::string>(tokens.front());
			tokens.pop_front();
			category = boost::lexical_cast<uint32_t>(tokens.front());
			tokens.pop_front();
			docid = boost::lexical_cast<uint64_t>(tokens.front());
			tokens.pop_front();
			docid_2 = boost::lexical_cast<uint64_t>(tokens.front());
			tokens.pop_front();
			docid_3 = boost::lexical_cast<uint64_t>(tokens.front());
			tokens.pop_front();
			SLOG_DEBUG("TEST_BAN:%s %d %llu %llu %llu", query.c_str(), category, docid, docid_2, docid_3);
		} catch (boost::bad_lexical_cast const&) {
			SLOG_WARNING("can not convert %s", str);
			continue;
		}
		std::vector<uint64_t> docid_list;
		docid_list.push_back(docid);
		docid_list.push_back(docid_2);
		docid_list.push_back(docid_3);
		std::map<uint32_t, std::map<std::string, std::vector<uint64_t> > >::iterator it = query_dict.find(category);
		if (it == query_dict.end()) {
			std::map<std::string, std::vector<uint64_t> > data;
			data[query] = docid_list;
			query_dict[category] = data;
		} else {
			it->second.insert(std::map<std::string, std::vector<uint64_t> >::value_type(query, docid_list));
		}
	}
	return true;
}

const std::vector<uint64_t>& AcInterventionAdd::find(char *query, uint32_t category) const {
	if (query == NULL)
		return none_docid_;
	std::string q(query);
	std::map<uint32_t, std::map<std::string, std::vector<uint64_t> > >::const_iterator it = query_dict.find(category);
	if (it != query_dict.end()) {
		std::map<std::string, std::vector<uint64_t> >::const_iterator it_query = it->second.find(q);
		if (it_query != it->second.end()) {
			return it_query->second;
		} else {
			return none_docid_;
		}
	}
	return none_docid_;
}

int AcInterventionAdd::count(int category) const {
	return query_dict.size();
}


BaseResource * AcSidDelay::Clone() {
	return new (std::nothrow) AcSidDelay();
}
bool AcSidDelay::Load(const char *path, const char *dict) {
	int path_len = strlen(path);
	int dict_len = strlen(dict);
	int file_len = path_len + dict_len;
	char file[file_len + 1];
	int pos = 0;
	pos = snprintf(file + pos, file_len, "%s/", path);
	pos = snprintf(file + pos, file_len, "%s", dict);

    SLOG_NOTICE("AcSidDelay:%s", file);
	std::ifstream fin(file);

	char str[MAX_LINE_LEN];

	while (fin.getline(str, MAX_LINE_LEN)) {
		std::string sid, white, unwhite;
		string_utils::string_list tokens = string_utils::split(str, "\t");
		if (tokens.size() != 3) {
			SLOG_WARNING("unvalid string %s", str);
			continue;
		}
		try {
			sid = boost::lexical_cast<std::string>(tokens.front());
			tokens.pop_front();
			white = boost::lexical_cast<std::string>(tokens.front());
			tokens.pop_front();
			unwhite = boost::lexical_cast<std::string>(tokens.front());
		} catch (boost::bad_lexical_cast const&) {
			SLOG_WARNING("can not convert %s", str);
			continue;
		}
        std::string value = white + "-" + unwhite;
        SLOG_NOTICE("AcSidDelay:%s %s %s, %s",
                    sid.c_str(), white.c_str(), unwhite.c_str(), value.c_str());
        sid_delay_dict.insert(make_pair(sid, value));
	}
	return true;
}

bool AcSidDelay::find(const std::string &sid, std::string *data) const {
    std::map<std::string, std::string>::const_iterator it = sid_delay_dict.find(sid);
    if (it != sid_delay_dict.end()) {
        data->assign(it->second);
        return true;
    }
    return false;
}
