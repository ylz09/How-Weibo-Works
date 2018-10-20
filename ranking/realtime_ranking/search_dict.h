/***************************************************************************
 * 
 * Copyright (c) 2012 Sina.com, Inc. All Rights Reserved
 * $Id$ 
 * 
 **************************************************************************/
 
/**
 * @file search_dict.h
 * @author wuhua1(minisearch@staff.sina.com.cn)
 * @date 2012/07/16 14:14:41
 * @version $Revision$ 
 * @brief 
 *  
 **/




#ifndef  __SEARCH_DICT_H_
#define  __SEARCH_DICT_H_

#include <new>
#include <stdint.h>
#include <dacore/base_resource.h>
#include <odict.h>
#include <oset.h>

class DASearchDict: public BaseResource
{
	
public:

	DASearchDict();

	~DASearchDict();

	virtual bool Load(const char * path, const char * dict);
	
	int Search(const char * key) const;

	int Search(uint64_t crc) const;

	int Search(const char * first, int firstlen, const char * second, int secondlen) const;

	bool Search(sodict_snode_t & snode) const;

	virtual DASearchDict * Clone()
	{
		return new(std::nothrow) DASearchDict();
	}

	sodict_search_t * RawDict()
	{
		return pdict;
	}

private:

	sodict_search_t * pdict;

};

class DASearchDictSet: public BaseResource
{
	
public:

	DASearchDictSet();

	~DASearchDictSet();

	virtual bool Load(const char * path, const char * dict);
	
	int Search(const char * key, int size);

	int Search(uint64_t crc) const;

	int Search(const char * first, int firstlen, const char * second, int secondlen);

	bool Search(soset_snode_t & snode);

	virtual DASearchDictSet * Clone()
	{
		return new(std::nothrow) DASearchDictSet();
	}

	soset_search_t * RawDict()
	{
		return pdict;
	}

private:

	soset_search_t * pdict;

};

#endif  //__SEARCH_DICT_H_


