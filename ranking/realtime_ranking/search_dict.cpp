/***************************************************************************
 * 
 * Copyright (c) 2012 Sina.com, Inc. All Rights Reserved
 * $Id$ 
 * 
 **************************************************************************/
 
/**
 * @file search_dict.cpp
 * @author wuhua1(minisearch@staff.sina.com.cn)
 * @date 2012/07/16 14:15:34
 * @version $Revision$ 
 * @brief 
 *  
 **/

#include <string.h>
#include <CRC.h>
#include "search_dict.h"

DASearchDict::DASearchDict():
BaseResource(),
pdict(NULL)
{
}

DASearchDict::~DASearchDict()
{
	if (pdict != NULL && (pdict != (sodict_search_t*) ODB_LOAD_NOT_EXISTS))
	{
		odb_destroy_search(pdict);
		pdict = NULL;
	}
}

bool DASearchDict::Load(const char * path, const char * dict)
{
	if (path == NULL || dict == NULL)
	{
		return false;
	}

	pdict = odb_load_search(const_cast<char *> (path), const_cast<char *> (dict));

	return ((pdict != NULL) && (pdict != (sodict_search_t*) ODB_LOAD_NOT_EXISTS));
}

int DASearchDict::Search(uint64_t crc) const
{
	sodict_snode_t snode;
	snode.sign1 = (unsigned int) (crc & 0x0ffffffff);
	snode.sign2 = (unsigned int) (crc >> 32);

	if (odb_seek_search(pdict, &snode) == ODB_SEEK_OK)
	{
		return snode.cuint1;
	}

	return -1;
}

int DASearchDict::Search(const char * first, int firstlen, const char * second, int secondlen) const
{
	if (first == NULL || second == NULL)
	{
		return -1;
	}

	char buf[10240];
	int totallen = firstlen + secondlen +1;

	if ((totallen >= (int) sizeof(buf)) || totallen == 0)
	{
		return -1;
	}

	memcpy(buf, first, firstlen);
	memcpy(buf+firstlen, "\t", 1);
	memcpy(buf+firstlen+1, second, secondlen);

	sodict_snode_t snode;
	CRC64(buf, totallen, &snode.sign1, &snode.sign2);

	if (odb_seek_search(pdict, &snode) == ODB_SEEK_OK)
	{
		return snode.cuint1;
	}

	return -1;
}

bool DASearchDict::Search(sodict_snode_t & snode) const
{
	if (odb_seek_search(pdict, &snode) == ODB_SEEK_OK)
	{
		return true;
	}

	return false;
}


DASearchDictSet::DASearchDictSet():
BaseResource(),
pdict(NULL)
{
}

DASearchDictSet::~DASearchDictSet()
{
	if (pdict != NULL && (pdict != (soset_search_t*) OSET_LOAD_NOT_EXISTS))
	{
		oset_destroy_search(pdict);
		pdict = NULL;
	}
}

bool DASearchDictSet::Load(const char * path, const char * dict)
{
	if (path == NULL || dict == NULL)
	{
		return false;
	}

	pdict = oset_load_search(const_cast<char *> (path), const_cast<char *> (dict));

	return ((pdict != NULL) && (pdict != (soset_search_t*) OSET_LOAD_NOT_EXISTS));
}

int DASearchDictSet::Search(uint64_t crc) const
{
	soset_snode_t snode;
	snode.sign1 = (unsigned int) (crc & 0x0ffffffff);
	snode.sign2 = (unsigned int) (crc >> 32);

	if (oset_seek_search(pdict, &snode) == OSET_SEEK_OK)
	{
		return 0;
	}

	return -1;
}
//search only string
int DASearchDictSet::Search(const char * str, int strlen)
{
	if (str == NULL)
	{
		return -1;
	}

	char buf[10240];

	if ((strlen >= (int) sizeof(buf)) || strlen == 0)
	{
		return -1;
	}

	memcpy(buf, str , strlen);

	soset_snode_t snode;
	CRC64(buf, strlen, &snode.sign1, &snode.sign2);
	//SLOG_DEBUG("str:%s,sign1:%lld,sign2:%lld",buf, snode.sign1,snode.sign2);

	if (oset_seek_search(pdict, &snode) == OSET_SEEK_OK)
	{
		return 0;
	}

	return -1;
}
int DASearchDictSet::Search(const char * first, int firstlen, const char * second, int secondlen)
{
	if (first == NULL || second == NULL)
	{
		return -1;
	}

	char buf[10240];
	int totallen = firstlen + secondlen +1;

	if ((totallen >= (int) sizeof(buf)) || totallen == 0)
	{
		return -1;
	}

	memcpy(buf, first, firstlen);
	memcpy(buf+firstlen, "\t", 1);
	memcpy(buf+firstlen+1, second, secondlen);

	soset_snode_t snode;
	CRC64(buf, totallen, &snode.sign1, &snode.sign2);

	if (oset_seek_search(pdict, &snode) == OSET_SEEK_OK)
	{
		return 0;
	}

	return -1;
}

bool DASearchDictSet::Search(soset_snode_t & snode)
{
	if (oset_seek_search(pdict, &snode) == OSET_SEEK_OK)
	{
		return true;
	}

	return false;
}

