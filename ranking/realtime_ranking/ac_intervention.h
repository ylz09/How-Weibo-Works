#ifndef  __AC_SRC_PLUGINS_WEIBO_AC_INTERVENTION_H_
#define  __AC_SRC_PLUGINS_WEIBO_AC_INTERVENTION_H_

#include "dacore/base_resource.h"
#include <map>
#include <set>
#include <vector>
#include <string>

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

#endif  //__AC_SRC_PLUGINS_WEIBO_AC_INTERVENTION_H_
