/***************************************************************************
 * 
 * Copyright (c) 2013 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/

/**
 * @file ac_user2class.cpp
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/05/30
 * @version 1.0ic:
 * @brief 
 *  
 **/

#include "ac_intervention.h"
#include <fstream>
#include <stdlib.h>
#include "string_utils.hpp"
#include "slog.h"
#include "boost/lexical_cast.hpp"

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

