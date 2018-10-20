/***************************************************************************
 * 
 **************************************************************************/
 
/**
 * @file ac_intervene_dict.h
 * @brief 
 *  
 **/


#ifndef  __AC_INTERVENE_DICT_H_
#define  __AC_INTERVENE_DICT_H_

#include <map>
#include <set>
#include <vector>
#include <string>
#include <ext/hash_set>

#include <dacore/base_resource.h>

class User2ClassDict : public BaseResource {
public:
	//@override
	bool Load(const char *path, const char *dict);

    //@Override
    BaseResource * Clone();

	int find(uint64_t uid);

	int count();
private:
	std::map<uint64_t, int> uid2class;
};

class Query2ClassDict : public BaseResource {
public:
	//@override
	bool Load(const char *path, const char *dict);
    //@override
    BaseResource * Clone();

	uint64_t find(char *query,int address);

	int count(int address);
private:
	std::map<std::string, std::vector<uint64_t> >  query2class;
};

class ACStringSet: public BaseResource {
public:
	//@override
	bool Load(const char *path, const char *dict);
	 //@override
	BaseResource * Clone();

	bool test(const std::string & key) const;

	bool test(const char * key) const;

private:

	std::set<std::string> m_string_set;
};

class ACLongSet: public BaseResource {
public:
	//@override
	bool Load(const char *path, const char *dict);
	//@Override
	BaseResource * Clone();
	bool find(uint64_t uid) const;
	uint32_t count() const;
private:
	__gnu_cxx::hash_set<uint64_t> m_uid_dict;
};

class AcInterventionDel : public BaseResource {
public:
	//@override
	bool Load(const char *path, const char *dict);
	//@Override
	BaseResource * Clone();
	uint64_t find(char *query, uint64_t docid) const;
	int count() const;
	const std::map<std::string, std::set<uint64_t> >& Data() const;
private:
	std::map<std::string, std::set<uint64_t> > query_dict;
};


class AcInterventionAdd : public BaseResource {
public:
	//@override
	bool Load(const char *path, const char *dict);
	//@Override
	BaseResource * Clone();
	const std::vector<uint64_t>& find(char *query, uint32_t category) const;
	int count(int category) const;
private:
	std::map<uint32_t, std::map<std::string, std::vector<uint64_t> > > query_dict;
	std::vector<uint64_t> none_docid_;
};

class AcSidDelay : public BaseResource {
public:
	//@override
	bool Load(const char *path, const char *dict);
	//@Override
	BaseResource * Clone();
	bool find(const std::string &sid, std::string *data) const;
private:
	std::map<std::string, std::string> sid_delay_dict;
};

#endif  //__AC_INTERVENE_DICT_H_


