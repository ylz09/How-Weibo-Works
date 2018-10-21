#ifndef  __REALTIME_RW_INDEX_H_
#define  __REALTIME_RW_INDEX_H_

#include <pthread.h>
#include <stdio.h>
#include <estring.h>
#include <base_resource.h>

#include "gs_iterface.h"

struct memory_cache;

class RealTimeRWIndex: public BaseResource
{

public:

	RealTimeRWIndex();

	virtual ~RealTimeRWIndex();

	virtual bool Load(const char * path, const char * conf);

	virtual int Search(int64_t key, char * value, size_t vsize, int * len = NULL);

protected:

	virtual int ProcessOnSocialRealtime(const char * record);

	int FaskDel(int64_t key, gs::attitude id, time_t t);

	int Del(int64_t key, uint64_t mid, time_t t, unsigned act);

	int Del(int64_t key);

	int Append(int64_t key, uint64_t mid, time_t t, unsigned act);

	int Restore();

	int Rollback();

	BaseResource * Clone()
	{
		return NULL;	/**< 不支持动态reload */
	}

	static void * CreateIndex(void * param);

	static int MarkDeleteItemEx(char * data, size_t size, void * param);

protected:

	memory_cache * pimp;
	pthread_rwlock_t m_rwlock;
	
	EString m_readbuf;
	EString m_updatebuf;

	int m_expiration;
	int m_supportDump;
	int m_autoDumpWindow;
	time_t m_timestamp;

	bool m_use_shm;
	int m_shm_key;
	int m_restore;
	int m_rollback_oneday;

	char m_rollbackFile[1024];
	char m_indexDumpFile[1024];
	char m_restoreFile[1024];

	char m_queueHost[1024];
	int m_queue_thread_num;
	int m_queue_size_mb;
	int m_queueTimeout;
};

#endif  //__REALTIME_RW_INDEX_H_

