#include <errno.h>
#include <string.h>
#include <slog.h>
#include <profile.h>
#include <memq.h>
#include <dalog.h>
#include <daconfig.h>
#include <dirent.h>

#include "memory_cache.h"
#include "realtime_rw_index.h"

extern int g_is_running;

static const int MAX_SAVE_NATTITUDE = 2*gs::TOPK;
RealTimeRWIndex::RealTimeRWIndex():
	pimp(NULL),
	m_readbuf(2*MAX_SAVE_NATTITUDE*sizeof(gs::attitude)),
	m_updatebuf(2*MAX_SAVE_NATTITUDE*sizeof(gs::attitude))
{
	assert(pthread_rwlock_init(&m_rwlock, NULL) == 0); 
}

RealTimeRWIndex::~RealTimeRWIndex()
{
	if (pimp != NULL)
	{	
		if (m_use_shm && m_shm_key > 0)
		{
			;
		}
		else
		{
			del_memory_cache(pimp);
		}
		pimp = NULL;
	}

	pthread_rwlock_destroy(&m_rwlock);
}

bool RealTimeRWIndex::Load(const char * path, const char * conf)
{
	if (path == NULL || conf == NULL)
	{
		return false;
	}

	char filename[1024];
	snprintf(filename, sizeof(filename), "%s/%s", path, conf);
	FILE * fp = fopen(filename, "r");
	if (fp == NULL)
	{
		slog_write(LL_FATAL, "failed to open configure[%s]", filename);
		return false;
	}

	fclose(fp);

	int hashsize = GetProfileInt(filename, "index", "hashsize", 10000001);
	int capacity = GetProfileInt(filename, "index", "capacity", 20000000);
	int unitsize = GetProfileInt(filename, "index", "unitsize", 60);
	m_expiration = GetProfileInt(filename, "index", "expiration", 3600*48);   /**< 最长保留48小时 */
	m_supportDump = GetProfileInt(filename, "index", "supportDump", 0);		
	m_autoDumpWindow = GetProfileInt(filename, "index", "autoDumpWindow", 0);
	m_timestamp = GetProfileInt(filename, "index", "timestamp", 0);
	
	m_use_shm = GetProfileInt(filename, "index", "shm", 0);
	m_shm_key = GetProfileInt(filename, "index", "skey", 0);
	
	m_restore = GetProfileInt(filename, "index", "restore", 0);
	m_rollback_oneday = GetProfileInt(filename, "index", "rollback_oneday", 0);

	char tempbuf[512] = {'\0'};
	GetProfileString(filename, "index", "dumpfile", "../dump/rtindex", tempbuf, sizeof(tempbuf));
	snprintf(m_indexDumpFile, sizeof(m_indexDumpFile), "%s%s", gDAStaticConf->workPath, tempbuf);

	tempbuf[0] = '\0';
	GetProfileString(filename, "index", "rollback", "", tempbuf, sizeof(tempbuf));
	slog_write(LL_DEBUG,"work path: %s, rollback path: %s", gDAStaticConf->workPath, tempbuf);
	if (m_restore and tempbuf[0] != '\0')
	{
		snprintf(m_rollbackFile, sizeof(m_rollbackFile), "%s%s", gDAStaticConf->workPath, tempbuf);
		slog_write(LL_DEBUG,"rollback path: %s",m_rollbackFile);
	}
	else
	{
		m_rollbackFile[0] = '\0';
	}

	GetProfileString(filename, "queue", "host", "graphsearch+commend@queue.match.sina.com.cn:11233",
			m_queueHost, sizeof(m_queueHost));

	m_queue_thread_num = GetProfileInt(filename, "queue", "queue_thread_num", 20);
	m_queue_size_mb = GetProfileInt(filename, "queue", "queue_size_mb", 1000);
	m_queueTimeout = GetProfileInt(filename, "queue", "timeout", 10);

	if (m_supportDump)
	{
		pimp = load_memory_cache(m_indexDumpFile);
		if (pimp == NULL)
		{
			slog_write(LL_WARNING, "load rtindex dump file[%s] error", m_indexDumpFile);
		}
	}

	if (pimp == NULL)
	{
		if (m_use_shm && m_shm_key > 0)
		{
			pimp = create_shared_mcache(m_shm_key, hashsize, unitsize, m_expiration, capacity);
		}
		else
		{
			pimp = create_memory_cache(hashsize, unitsize, m_expiration, capacity);
		}

		if (pimp == NULL)
		{
			slog_write(LL_FATAL, "create_memory_cache error");
			return false;
		}
	}

	pthread_t pid;
	if (pthread_create(&pid, NULL, RealTimeRWIndex::CreateIndex, this) != 0)
	{
		slog_write(LL_FATAL, "CreateIndex error");
		return false;
	}

	return true;
}

int RealTimeRWIndex::Search(int64_t key, char * value, size_t vsize, int * len)
{
	pthread_rwlock_rdlock(&m_rwlock);
	int ret = seek_memory_cache_item(pimp, key, value, vsize, len);
	pthread_rwlock_unlock(&m_rwlock);

	return ret;
}

int RealTimeRWIndex::ProcessOnSocialRealtime(const char * record)
{
	if (record == NULL)
	{
		return -1;
	}

	if (record[0] != '@')
	{
		return -1;
	}

	char action[256], category[256];
	uint64_t mid = 0, uid = 0;
	time_t t = 0;
	int level = 0;
	int invalid_fans = 0;
	int ret = -1;
	int act = 0;

	if (sscanf(record+1, "%s\t%s\t%lu\t%lu\t%lu\t%d\t%d",
				action, category, &mid, &t, &uid, &level, &invalid_fans) == 7 && t > m_timestamp)
	{
		if (strcasecmp(category, "forward") == 0)
		{
			act = 1;
		}
		else if (strcasecmp(category, "attitude") == 0)
		{
			act = 2;
		}
		else
		{
			return -1;
		}

		if (mid == 0 || uid == 0)
		{
			return -3;
		}

		if (strcasecmp(action, "ADD") == 0)
		{
			if ((invalid_fans != -1 && invalid_fans <= 4) || (level != -1 && level != 1 && level != 2))
			{
				return 0;
			}
		
			ret = Append(uid, mid, t, act);
		}
		else if (strcasecmp(action, "DEL") == 0 || strcasecmp(action, "delete") == 0)
		{
			gs::attitude at;
			at.mid = mid;
			at.action_type = act;
			ret = FaskDel(uid, at, t);
		}
		else
		{
			ret = -100;
		}
	}
	else
	{
		ret = -10;
	}

	return ret;
}

int RealTimeRWIndex::FaskDel(int64_t key, gs::attitude id, time_t t)
{
	return operator_memory_cache_item(pimp, key, &id, RealTimeRWIndex::MarkDeleteItemEx); 
}

int RealTimeRWIndex::Del(int64_t key, uint64_t mid, time_t t, unsigned act)
{
	int len = 0;
	int newlen = 0;
	pthread_rwlock_rdlock(&m_rwlock);
	int ret = seek_memory_cache_item(pimp, key, m_readbuf.getBuffer(), m_readbuf.size(), &len);
	pthread_rwlock_unlock(&m_rwlock);

	if (ret == CACHE_NOTIN)
	{
		slog_write(LL_NOTICE, "key[%lu] is not in realtime index,", key);
		return -1;
	}

	if (len % sizeof(gs::attitude) != 0)
	{
		slog_write(LL_WARNING, "key[%lu] index has error when %s, needed deleted", key, __func__);
		len = 0;
	}
	
	const gs::attitude * atti = (gs::attitude *) m_readbuf.getBuffer();
	gs::attitude * new_atti = (gs::attitude *) m_updatebuf.getBuffer();
	
	time_t curtime = time(NULL);
	for (int i = sizeof(gs::attitude); i <= len; i += sizeof(gs::attitude))
	{
		if (int (curtime - atti->t) > this->m_expiration || (atti->mid == mid && atti->action_type == act))
		{	
			++atti;
			continue;
		}
		
		*new_atti++ = *atti++;
		newlen += sizeof(gs::attitude);
	}
	
	if (newlen > 0)
	{
		pthread_rwlock_wrlock(&m_rwlock);
		ret = modify_memory_cache_item(pimp, key, m_updatebuf.getBuffer(), newlen);
		pthread_rwlock_unlock(&m_rwlock);
		
		return ret;
	}
	
	pthread_rwlock_wrlock(&m_rwlock);
	ret = rm_memory_cache_item(pimp, key);
	pthread_rwlock_unlock(&m_rwlock);

	return ret;
}

int RealTimeRWIndex::Del(int64_t key)
{
	pthread_rwlock_wrlock(&m_rwlock);
	int ret = rm_memory_cache_item(pimp, key);
	pthread_rwlock_unlock(&m_rwlock);

	return ret;
}

int RealTimeRWIndex::Append(int64_t key, uint64_t mid, time_t t, unsigned act)
{
	time_t curtime = time(NULL);
	if (int (curtime - t) > this->m_expiration)
	{
		slog_write(LL_NOTICE, "add a older attitude[%lu %lu %lu %u], need ignored", mid, t, key, act);
		return -2;
	}

	int len = 0;
	int newlen = 0;

	pthread_rwlock_rdlock(&m_rwlock);
	int ret = seek_memory_cache_item(pimp, key, m_readbuf.getBuffer(), m_readbuf.size(), &len);
	pthread_rwlock_unlock(&m_rwlock);

	if (!m_updatebuf.reserve(len + sizeof(gs::attitude)))
	{
		slog_write(LL_FATAL, "reserve for updateabuf[%ld] error", len + sizeof(gs::attitude));
		return false;
	}

	const gs::attitude * atti = (gs::attitude *) m_readbuf.getBuffer();
	gs::attitude * new_atti = (gs::attitude *) m_updatebuf.getBuffer();
	new_atti->mid = mid;
	new_atti->t = t;
	new_atti->action_type = act;
	++new_atti;
	newlen += sizeof(gs::attitude);
	
	if (len % sizeof(gs::attitude) != 0)
	{
		slog_write(LL_WARNING, "key[%lu] index[size:%d] has error, needed deleted", key, len);
		len = 0;
	}
	
	int count = 1;
	for (int i = sizeof(gs::attitude); i <= len; i += sizeof(gs::attitude))
	{
		// 忽略过期失效或者重复的uid
		if (int (curtime - atti->t) > this->m_expiration || (atti->mid == mid && atti->action_type == act))
		{
			++atti;
			continue;
		}

		*new_atti++ = *atti++;
		newlen += sizeof(gs::attitude);

		if (++count >= MAX_SAVE_NATTITUDE)
		{
			slog_write(LL_NOTICE, "key[%lu] add %d attitude over thred..", key, count);
			break;
		}
	}

	pthread_rwlock_wrlock(&m_rwlock);
	ret = update_memory_cache_item(pimp, key, m_updatebuf.getBuffer(), newlen);
	pthread_rwlock_unlock(&m_rwlock);

	if (ret != 0)
	{
		slog_write(LL_WARNING, "add[%lu %lu %lu] error", mid, t, key);
	}

	return ret;
}

int RealTimeRWIndex::Restore()
{
    if (m_timestamp == 0)
    {
	return 0;
    }

    DIR *dp;
    struct dirent *dirp;
    int count = 0;
    time_t cur = time(NULL);

    //slog_write(LL_WARNING,"m_rollbackFile:%s",m_rollbackFile);
    char* path = "/data1/minisearch/socialsearch/log";
    if((dp = opendir(path)) == NULL)
    {
	slog_write(LL_WARNING, "failed to open rollback[%s] %s", path, strerror(errno));
	return -1;
    }

    while((dirp = readdir(dp)) != NULL)
    {
	m_restoreFile[0] = '\0';
	if(strstr(dirp->d_name, "rollback") != NULL and strstr(dirp->d_name,".log") != NULL)
	{
	    snprintf(m_restoreFile,sizeof(m_restoreFile),"%s/%s",m_rollbackFile,dirp->d_name);
	    FILE * fp = fopen(m_restoreFile, "r");
	    if (fp == NULL)
	    {
		slog_write(LL_WARNING, "failed to open rollback[%s] %s", m_restoreFile, strerror(errno));
		break;
	    }
	    slog_write(LL_NOTICE, "begin restore from %s",m_restoreFile);
	    char buf[1024];  
	    while (fgets(buf, sizeof(buf), fp))
	    {
			int len = strlen(buf);
			while (len > 0 && (buf[len-1] == '\n'|| buf[len-1] == '\r'))
			{
				buf[--len] = '\0';
			}

			const char * record = strchr(buf, '\t');
			if (record == NULL)
			{
				continue;
			}
			
			ProcessOnSocialRealtime(++record);
		
			if (++count % 500000 == 0)
			{
				slog_write(LL_NOTICE, "restore %d number of record...", count);
			}
	    }
	    fclose(fp);
	}
    }
    
    slog_write(LL_NOTICE, "finish restore %d record cost %lu s", count, time(NULL) - cur);
    return 0;
}

int RealTimeRWIndex::Rollback()
{
	if (m_timestamp == 0)
	{
		return 0;
	}

	int count = 0;
	struct tm t;
	time_t cur = time(NULL);
	localtime_r(&cur, &t);

	for (int fileid = 0; ; )
	{
		if (m_rollbackFile[0] == '\0')
		{
			snprintf(m_rollbackFile, sizeof(m_rollbackFile), "%s../log/rollback_%d%02d%02d_%d.log",
					gDAStaticConf->workPath, t.tm_year+1900, t.tm_mon+1, t.tm_mday, fileid++);
		}
		
		FILE * fp = fopen(m_rollbackFile, "r");
		if (fp == NULL)
		{
			slog_write(LL_WARNING, "failed to open rollback[%s] %s", m_rollbackFile, strerror(errno));
			break;
		}
		
		slog_write(LL_NOTICE, "begin rollback from %s", m_rollbackFile);
		char buf[1024];
		while (fgets(buf, sizeof(buf), fp))
		{
			int len = strlen(buf);
			while (len > 0 && (buf[len-1] == '\n'|| buf[len-1] == '\r'))
			{
				buf[--len] = '\0';
			}

			const char * record = strchr(buf, '\t');
			if (record == NULL)
			{
				continue;
			}
			
			ProcessOnSocialRealtime(++record);
		
			if (++count % 500000 == 0)
			{
				slog_write(LL_NOTICE, "rollbacking %d number of record...", count);
			}
		}

		fclose(fp);
		if (fileid == 0)
		{
			break;
		}
		else
		{
			m_rollbackFile[0] = '\0';
		}
	}

	slog_write(LL_NOTICE, "finish rollback %d record cost %lu s", count, time(NULL) - cur);
	return 0;
}

void * RealTimeRWIndex::CreateIndex(void * param)
{
	pthread_detach(pthread_self());
	
	RealTimeRWIndex * index = (RealTimeRWIndex *) (param);
	slogThrTag = "CreateIndex";

	if (index->m_queueHost[0] == '\0')
	{
		slog_write(LL_WARNING, "no realtime queue");
		return NULL;
	}

	if(index->m_restore and index->m_rollbackFile != NULL)
	{
	    index->Restore();
	}

	if (index->m_rollback_oneday)
	{
	    index->Rollback();
	}

	void *dis = memq_run(index->m_queueHost, index->m_queue_thread_num, index->m_queue_size_mb);
	if (dis == NULL)
	{
		slog_write(LL_FATAL, "memq_run error");
		exit(EXIT_FAILURE);
	}

	DALog * log = Resource::getResource<DALog> ("../log/rollback");
	if (log == NULL)
	{
		slog_write(LL_FATAL, "init record log failed");
		exit(EXIT_FAILURE);
	}

	LOG_HANDLE recordlog = log->raw();

	time_t pretime = time(NULL);
	while (g_is_running)
	{
		time_t curtime = time(NULL);
		if ((index->m_autoDumpWindow > 0) && (curtime - pretime > index->m_autoDumpWindow))
		{
			char chacheDumpFile[1024];
			snprintf(chacheDumpFile, sizeof(chacheDumpFile), "%s.auto.%lu", index->m_indexDumpFile, curtime);
			if (dumpe_memory_cache(index->pimp, chacheDumpFile) != 0)
			{
				slog_write(LL_WARNING, "index auto dump[%s] error", chacheDumpFile);
			}
			pretime = curtime;
		}

		char * data = (char *) memq_request(dis);
		if (data != NULL)
		{
			int len = strlen(data);
			while (len > 0 && (data[len-1] == '\n'|| data[len-1] == '\r'))
			{
				data[--len] = '\0';
			}

			if (index->ProcessOnSocialRealtime(data) == 0)
			{
				seLogEx(recordlog, "%s", data);
			}
			else
			{
				seErrLogEx(recordlog, "%s", data);
			}

			free(data);
			data = NULL;
		}
	}

	if (dis != NULL)
	{
		memq_stop(dis);
		dis = NULL;
		slog_write(LL_FATAL, "Critical! Stop proccess the online queue!");
	}

	if (index->m_supportDump)
	{
		dumpe_memory_cache(index->pimp, index->m_indexDumpFile);
	}

	return NULL;
}

int RealTimeRWIndex::MarkDeleteItemEx(char * data, size_t size, void * param)
{
	gs::attitude * key = (gs::attitude *) param;
	gs::attitude * atti = (gs::attitude *) data;
	for (size_t i = sizeof(gs::attitude); i <= size; i += sizeof(gs::attitude), ++atti)
	{
		if (atti->mid == key->mid && atti->action_type == key->action_type)
		{
			atti->t = 0;
			return 1;
		}
	}

	return 0;
}

