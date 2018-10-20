/**
 * @file ac_source_id_dict.h
 * @brief 
 * @author qiupengfei
 * @version 1.0
 * @date 2014-04-15
 */

#ifndef AC_SOURCE_ID_DICT_H
#define AC_SOURCE_ID_DICT_H


#include "dacore/base_resource.h"
#include <set>

class SourceIDDict : public BaseResource {
public:
	//@override
	bool Load(const char *path, const char *dict);


    //@Override
    BaseResource * Clone();

	int find(uint64_t sid) const;

	int count();
private:
	std::set<uint64_t> _source_dict;
};

#endif
