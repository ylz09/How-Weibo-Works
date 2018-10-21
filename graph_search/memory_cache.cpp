/**
 * @file memory_cache.cpp
 * @brief 
 * 内存版cache实现，cache全部在内存中
 **/

#include <errno.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <slog.h>

#include "memory_cache.h"

#pragma pack(push)
#pragma pack(4) //设置结构体4字节对齐

struct mc_index_node
{
	uint64_t key;			/**< item key       */

	unsigned next;			/**< hash chain       */
	unsigned left;			/**< LRU chain       */
	unsigned right;

	unsigned data_size;
	unsigned data_left;		/**< variable data size chain     */
	unsigned data_right;

    int32_t ctime;				/**< 最新赞的时间 */
};

struct mc_hash_table
{
	unsigned size;
	unsigned * table;
};

struct memory_cache
{
	unsigned capacity;
	unsigned unit_size;

	unsigned head;
	unsigned tail;

	unsigned rest_node;
	unsigned rest_head;
    
    unsigned time_limit;
	int lru; 

	mc_hash_table hash;
	mc_index_node * pnodes;
	char * pdata;
};

inline static void clean_cache_node(mc_index_node * pnode)
{
	pnode->key = 0;
	pnode->next = NILL;
	pnode->left = NILL;
	pnode->right = NILL;
}

unsigned * memory_cache_getpos(memory_cache * pcache, uint64_t key)
{
	if (pcache == NULL)
	{
		return NULL;
	}

    unsigned hashpos = key % pcache->hash.size;
    unsigned * ptmppos = pcache->hash.table+hashpos;
    
    while (*ptmppos != NILL)
    {
		if (*ptmppos >= pcache->capacity)
		{
			return NULL;
		}
        
        mc_index_node * pin = pcache->pnodes + *ptmppos;
        if (pin->key == key)
		{
			break;
		}
		
		ptmppos = &(pin->next);
    }

    return ptmppos;
}

static int cache_LRU_append(memory_cache * pcache, unsigned curpos)
{
	if (pcache == NULL || curpos == NILL || curpos >= pcache->capacity)
	{
		return -1;
	}

	if ((pcache->head == NILL && pcache->tail != NILL) || (pcache->head != NILL && pcache->tail == NILL))
	{
		return -1;
	}

	mc_index_node * pin = pcache->pnodes + curpos;
	/// add first node to LRU bidirectional chain
	if (pcache->head == NILL)
	{
        pcache->head = curpos;
        pcache->tail = curpos;
        pin->left = NILL;
        pin->right = NILL;
    }
    else
    {
        mc_index_node * pold_head = pcache->pnodes + pcache->head;
        pold_head->left = curpos;
        pin->left = NILL;
        pin->right = pcache->head;
        pcache->head = curpos;
    }
	
    return 0;
}

static int cache_LRU_rm(memory_cache * pcache, unsigned curpos)
{
	if (pcache == NULL || curpos == NILL || curpos >= pcache->capacity)
	{
		return -1;
	}

    unsigned * pright_of_left = NULL;
	unsigned * pleft_of_right = NULL;
	
    mc_index_node * pcurnode = pcache->pnodes + curpos;
    if (pcurnode->left == NILL)
    {
    	pright_of_left = &(pcache->head);
    }
    else
    {
        mc_index_node  * pcurnode_left = pcache->pnodes + pcurnode->left;
        pright_of_left = &(pcurnode_left->right);
    }
	
    if (pcurnode->right == NILL)
    {
        pleft_of_right = &(pcache->tail);
    }
    else
    {
        mc_index_node * pcurnode_right = pcache->pnodes + pcurnode->right;
        pleft_of_right = &(pcurnode_right->left);
    }

    *pright_of_left = pcurnode->right;
    *pleft_of_right = pcurnode->left;
    
    pcurnode->left = NILL;
    pcurnode->right = NILL;

    return 0; 
}

static int clear_memory_cache_item(memory_cache * pcache, unsigned * pcurpos)
{
	if (pcache == NULL || pcurpos == NULL)
	{
		return -1;
	}

	if (*pcurpos == NILL)
	{
		return CACHE_NOTIN;
	}

	unsigned curpos = *pcurpos;
	mc_index_node * phead = pcache->pnodes + curpos;

	/// remove the cache item from hash item chain
	*pcurpos = phead->next;

    /// remove the cache item from bidirectional chain(LRU)
	if (cache_LRU_rm(pcache, curpos) != 0)
	{
		return -1;
	}
	
	pcache->rest_node += (phead->data_size-1)/pcache->unit_size+1;
	if (pcache->rest_head == NILL)
	{
		pcache->rest_head = curpos;
	}
	else
	{
		unsigned tail = phead->data_left;
		mc_index_node * ptail = pcache->pnodes+tail;
		mc_index_node * prest_head = pcache->pnodes+pcache->rest_head;
		unsigned rest_tail = prest_head->data_left;
		mc_index_node * prest_tail = pcache->pnodes+rest_tail;
		
		ptail->data_right = pcache->rest_head;
		prest_head->data_left = tail;

		phead->data_left = rest_tail;
		prest_tail->data_right = curpos;

		pcache->rest_head = curpos;
	}
    
    return 0;
}

int set_memory_cache_twindow(memory_cache * pcache, time_t time_limit)
{
	if (pcache == NULL)
	{
		return -1;
	}

	pcache->time_limit = time_limit;

	return 0;
}

int set_memory_cache_paging(memory_cache * pcache, int lru)
{
	pcache->lru = lru;
	return 0;
}

memory_cache * create_memory_cache(unsigned hash_num, unsigned unit_size, time_t time_limit, unsigned capacity)
{
	memory_cache * pcache = (memory_cache *) malloc(sizeof(memory_cache));
	if (pcache == NULL)
	{
		goto failed;
	}

	memset(pcache, 0, sizeof(*pcache));

	if (hash_num % 2 == 0)
	{
		++hash_num;
	}

	if (capacity == 0)
	{
		capacity = hash_num;
	}
	pcache->capacity = capacity;
	pcache->unit_size = unit_size;

	pcache->hash.table = (unsigned *) malloc(sizeof(unsigned) * hash_num);
	if (pcache->hash.table == NULL)
	{
		goto failed;
	}
	pcache->hash.size = hash_num;

	pcache->pnodes = (mc_index_node *) malloc(sizeof(mc_index_node) * capacity);
	if (pcache->pnodes == NULL)
	{
		goto failed;
	}

	pcache->pdata = (char *) malloc(size_t(unit_size)*capacity);
	if (pcache->pdata == NULL)
	{
		goto failed;
	}

	clean_memory_cache(pcache);

    pcache->time_limit = time_limit;
	if (time_limit == (unsigned) -1)
	{
		pcache->lru = 1;
	}

	return pcache;

failed:

	del_memory_cache(pcache);
	return NULL;
}

shared_mcache * create_shared_mcache(key_t skey, unsigned hash_num, unsigned unit_size,
		time_t time_limit, unsigned capacity)
{
	if (hash_num % 2 == 0)
	{
		++hash_num;
	}

	if (capacity == 0)
	{
		capacity = hash_num;
	}

	size_t total_size = (sizeof(shared_mcache) + sizeof(unsigned) * hash_num + 
			sizeof(mc_index_node) * capacity + size_t(unit_size)*capacity);

	bool exist = true;
	int id = shmget(skey, total_size, 0666);
	if (id < 0)
	{
		slog_write(LL_WARNING, "shmget(%u,%lu,0666) error %m", skey, total_size, errno);
		id = shmget(skey, total_size, IPC_CREAT|0666);
		if (id < 0)
		{
			slog_write(LL_FATAL, "shmget(%u,%lu,IPC_CREAT|0666) error %m", skey, total_size, errno);
			return NULL;
		}
		exist = false;
	}

	slog_write(LL_NOTICE, "shmget(%u,%lu,06666) return %d", total_size, skey, id);

	char * shm = (char *) shmat(id, NULL, 0);
	if (shm == (char *) -1)
	{
		slog_write(LL_FATAL, "shmat(%d,NULL,0) error %m", id, errno);
		return NULL;
	}

	size_t curpos = 0;
	shared_mcache * pcache = (memory_cache *) (shm + curpos);
	curpos += sizeof(shared_mcache);
	if (pcache == NULL)
	{
		goto failed;
	}

	if (!exist || pcache->capacity != capacity || pcache->unit_size != unit_size ||
			pcache->hash.size != hash_num)
	{
		memset(pcache, 0, sizeof(*pcache));
	}

	pcache->hash.table = (unsigned *) (shm + curpos);
	curpos += sizeof(unsigned) * hash_num;
	if (pcache->hash.table == NULL)
	{
		goto failed;
	}

	pcache->pnodes = (mc_index_node *) (shm + curpos);
	curpos += sizeof(mc_index_node) * capacity;
	if (pcache->pnodes == NULL)
	{
		goto failed;
	}

	pcache->pdata = (char *) (shm + curpos);
	curpos += size_t(unit_size)*capacity;
	if (pcache->pdata == NULL)
	{
		goto failed;
	}

	if (!exist || pcache->capacity != capacity || pcache->unit_size != unit_size ||
			pcache->hash.size != hash_num)
	{
		pcache->capacity = capacity;
		pcache->unit_size = unit_size;
		pcache->hash.size = hash_num;

		clean_memory_cache(pcache);
		
		pcache->time_limit = time_limit;
		if (time_limit == (unsigned) -1)
		{
			pcache->lru = 1;
		}
	}

	return pcache;

failed:

	if (id >= 0)
	{
		del_shared_mcache(id);
	}
	return NULL;
}

void del_shared_mcache(int shmid)
{
	shmctl(shmid, IPC_RMID, 0);
}

void del_memory_cache(memory_cache * pcache)
{
	if (pcache != NULL)
	{
		SFREE(pcache->pnodes);
		SFREE(pcache->pdata);
		SFREE(pcache);
	}
}

void clean_memory_cache(memory_cache * pcache)
{
	if (pcache != NULL)
	{
		pcache->head = NILL;
		pcache->tail = NILL;

		pcache->rest_node = pcache->capacity;
		pcache->rest_head = 0L;

		memset(pcache->hash.table, 0xFF, sizeof(unsigned)*pcache->hash.size);

		mc_index_node * pnode = pcache->pnodes;
		pnode->data_left = pcache->capacity -1;

		for (unsigned i = 0; i < pcache->capacity -1; ++i)
		{
			clean_cache_node(pnode);

			pnode->data_right = i+1;
			(++pnode)->data_left = i;
		}

		clean_cache_node(pnode);
		pnode->data_right = 0;
	}
}

int add_memory_cache_item(memory_cache * pcache, uint64_t key, void * pdata, size_t size)
{
	if (pcache == NULL || pdata == NULL || size == 0)
	{
		slog_write(LL_WARNING, "%s param error", __func__);
		return -1;
	}
	
	unsigned * pcurpos = memory_cache_getpos(pcache, key);
	assert(pcurpos != NULL);

	/// this cache item exist in cache
	if (*pcurpos != NILL)
	{
		return CACHE_NONE;
	}

	/// this cache item doesn't exist, add it into cache
	unsigned count = (size-1)/pcache->unit_size + 1;	
	if (count > pcache->capacity)
	{
		slog_write(LL_DEBUG, "[%lu] size[%lu] overflow", key, size);
		return -1;
	}

	/// has no full capacity for save
	/// delete some cache items according LRU algorithms
	time_t curtime = time(NULL);
	while (pcache->rest_node < count)
	{
		mc_index_node * ptmpin = pcache->pnodes + pcache->tail;
		if (pcache->time_limit != (unsigned) -1 && curtime - ptmpin->ctime <= pcache->time_limit)
		{
			slog_write(LL_WARNING, "realtime index is overflow");
		}

		unsigned * pos = memory_cache_getpos(pcache, ptmpin->key);	
		if (clear_memory_cache_item(pcache, pos) != 0)
		{
			slog_write(LL_WARNING, "delete some memory cache item error");
			return -1;
		}
	}
	
	/// hash has been modified, so need to get the new position
	pcurpos = memory_cache_getpos(pcache, key);
	assert(pcurpos != NULL);

	unsigned head = pcache->rest_head;
	unsigned curpos = head;
	mc_index_node * pin = pcache->pnodes + curpos;
	unsigned rest_tail = pin->data_left;
	mc_index_node * prest_tail = pcache->pnodes + rest_tail;
	
	unsigned cursize = 0;
	char * pdst = (char *) pdata;
	while (cursize < size)
	{
		pcache->rest_node--;
	
		pin = pcache->pnodes + curpos;
		if (cursize == 0)
		{
			pin->next = NILL;
			pin->key = key;
			pin->data_size = size;
	
			/// add the cache item into hash chain
			*pcurpos = curpos;

			/// add the cache item into LRU bi-chain
			cache_LRU_append(pcache, curpos);

			pin->ctime = uint32_t (time(NULL));
		}

		/// copy data into data area
		if (cursize + pcache->unit_size < size)
		{
			memcpy(pcache->pdata + (size_t)pcache->unit_size*curpos, pdst, pcache->unit_size);
			pdst += pcache->unit_size;
			cursize += pcache->unit_size;

			curpos = pin->data_right;
			assert(curpos < pcache->capacity);
		}
		else
		{
            memset(pcache->pdata + (size_t)pcache->unit_size*curpos, 0, pcache->unit_size);
			memcpy(pcache->pdata + (size_t)pcache->unit_size*curpos, pdst, size-cursize);
			cursize = size;
		}
	}
	
	if (curpos == rest_tail)
	{
		pcache->rest_head = NILL;
	}
	else
	{
		pin = pcache->pnodes + curpos;
		pcache->rest_head = pin->data_right;
		prest_tail->data_right = pin->data_right;
		pin = pcache->pnodes + pcache->rest_head;
		pin->data_left = rest_tail;
	}
	
	pin = pcache->pnodes + curpos;
	pin->data_right = head;

	pin = pcache->pnodes + head;
	pin->data_left = curpos;

    return 0;
}

int operator_memory_cache_item(memory_cache *pcache, uint64_t key, void * param, InnerIndexOperator func)
{
	unsigned * pcurpos = memory_cache_getpos(pcache, key);
	assert(pcurpos != NULL);
	unsigned curpos = *pcurpos;

	/// the cache item not exist in hash
    if (curpos == NILL)
    {
        return CACHE_NOTIN;
    }

	if (pcache->time_limit != (unsigned) -1)
    {
        time_t cur = time(NULL);

        if (cur - pcache->pnodes[curpos].ctime > pcache->time_limit)
		{
            clear_memory_cache_item(pcache, pcurpos);
            return CACHE_NOTIN;
		}
    }

	unsigned head = curpos;
    do
	{
		if (func(pcache->pdata + (size_t) pcache->unit_size*curpos, pcache->unit_size, param) != 0)
		{
			break;
		}
		mc_index_node * pin = pcache->pnodes + curpos;
		curpos = pin->data_right;
		assert(curpos < pcache->capacity);
	} while (curpos != head);

	return 0;
}

int seek_memory_cache_item(memory_cache *pcache, uint64_t key, void * pdata, size_t size, int * len)
{
	unsigned * pcurpos = memory_cache_getpos(pcache, key);
	assert(pcurpos != NULL);
	unsigned curpos = *pcurpos;

	/// the cache item not exist in hash
    if (curpos == NILL)
    {
        return CACHE_NOTIN;
    }

	if (pcache->time_limit != (unsigned) -1)
    {
        time_t cur = time(NULL);

        if (cur - pcache->pnodes[curpos].ctime > pcache->time_limit)
		{
            return CACHE_NOTIN;
		}
    }

    if (pcache->lru)
    {
        /// LRU page strategy, implement this code must be locked
        if (cache_LRU_rm(pcache, curpos) != 0)
        {
			slog_write(LL_WARNING, "cache_LRU_rm called in %s error", __func__);
            return -1;
        }

        if (cache_LRU_append(pcache, curpos) != 0)
        {
			slog_write(LL_WARNING, "cache_LRU_append called in %s error", __func__);
            return -1;
        }
    }
	
	/// default FIFO page strategy

	char * pdst = (char *) pdata;
	unsigned cursize = 0;
	unsigned head = curpos;
    do
	{
		if (cursize + pcache->unit_size < size)
		{
			memcpy(pdst, pcache->pdata + (size_t) pcache->unit_size*curpos, pcache->unit_size);

			mc_index_node * pin = pcache->pnodes + curpos;
			curpos = pin->data_right;
			pdst += pcache->unit_size;
			cursize += pcache->unit_size;
			assert(curpos < pcache->capacity);
		}
		else
		{
			memcpy(pdst, pcache->pdata + (size_t)pcache->unit_size*curpos, size-cursize);
			cursize = size;
		}
	} while (curpos != head && cursize < size);

	if (len != NULL)
	{
		*len = cursize;
	}

    return 0;
}

/// no need to modify time since it use add_memory_cache_item (create new time) to update
/// 修改一个已经存在的元素内容，如果不存在则不修改
int modify_memory_cache_item(memory_cache *pcache, uint64_t key, void * pdata, size_t size)
{
	if (pcache == NULL || pdata == NULL)
	{
		return -1;
	}

	if (rm_memory_cache_item(pcache, key) != 0)
	{
		return -1;
	}

	return add_memory_cache_item(pcache, key, pdata, size);
}

/// 如果不存在，则新增一个，如果存在，先删除再新增
int update_memory_cache_item(memory_cache *pcache, uint64_t key, void * pdata, size_t size)
{
	if (pcache == NULL || pdata == NULL)
	{
		return -1;
	}

	unsigned * pcurpos = memory_cache_getpos(pcache, key);
	if (pcurpos != NULL && *pcurpos < pcache->capacity)
	{
		/// remove the indexnode from the cache
		if (clear_memory_cache_item(pcache, pcurpos) != 0)
		{
			return -1;
		}
	}

	return add_memory_cache_item(pcache, key, pdata, size);
}

int rm_memory_cache_item(memory_cache *pcache, uint64_t key)
{
	if (pcache == NULL)
	{
		return -1;
	}
    
	unsigned * pcurpos = memory_cache_getpos(pcache, key);
	if (pcurpos == NULL || *pcurpos >= pcache->capacity)
	{
		return -1;
	}
	
    /// remove the indexnode from the cache
    return clear_memory_cache_item(pcache, pcurpos);
}

static inline int fwrite_uint(FILE * fp, unsigned * value, size_t num = 1)
{
	if (fp == NULL || value == NULL)
	{
		return -1;
	}

	if (fwrite(value, sizeof(*value), num, fp) != num)
	{
		return -1;
	}

	return 0;
}

static inline int fread_uint(FILE * fp, unsigned * value, size_t num = 1)
{
	if (fp == NULL || value == NULL)
	{
		return -1;
	}

	if (fread(value, sizeof(*value), num, fp) != num)
	{
		return -1;
	}

	return 0;
}

int dumpe_memory_cache(memory_cache *pcache, const char *filename)
{
	if (pcache == NULL || filename == NULL)
	{
		return -1;
	}

	FILE * fp = fopen(filename, "wb");
	if (fp == NULL)
	{
		return -1;
	}

	int ret = -1;
	unsigned capacity = pcache->capacity;

	if (fwrite_uint(fp, &pcache->capacity) != 0 ||
			fwrite_uint(fp, &pcache->unit_size) != 0 ||
			fwrite_uint(fp, &pcache->head) != 0 ||
			fwrite_uint(fp, &pcache->tail) != 0 ||
			fwrite_uint(fp, &pcache->rest_node) != 0 ||
			fwrite_uint(fp, &pcache->rest_head) != 0 ||
            fwrite_uint(fp, &pcache->time_limit) != 0 ||
            fwrite_uint(fp, reinterpret_cast<unsigned *> (&pcache->lru)) != 0)
	{
		goto failed;
	}

	if (fwrite_uint(fp, &pcache->hash.size) != 0 ||
			fwrite_uint(fp, pcache->hash.table, pcache->hash.size) != 0)
	{
		goto failed;
	}

	if (fwrite(pcache->pnodes, sizeof(pcache->pnodes[0]), capacity, fp) != capacity)
	{
		goto failed;
	}

	if (fwrite(pcache->pdata, pcache->unit_size, capacity, fp) != capacity)
	{
		goto failed;
	}

	ret = 0;

failed:

	if (fp != NULL)
	{
		fclose(fp);
	}

	return ret;
}

memory_cache * load_memory_cache(const char * filename)
{
	if (filename == NULL)
	{
		return NULL;
	}

	unsigned capacity = 0;

	memory_cache * pcache = (memory_cache *) malloc(sizeof(memory_cache));
	if (pcache == NULL)
	{
		return NULL;
	}

	memset(pcache, 0, sizeof(*pcache));

	FILE * fp = fopen(filename, "rb");

	if (fp == NULL)
	{
		goto failed;
	}

	if (fread_uint(fp, &pcache->capacity) != 0 ||
			fread_uint(fp, &pcache->unit_size) != 0 ||
			fread_uint(fp, &pcache->head) != 0 ||
			fread_uint(fp, &pcache->tail) != 0 ||
			fread_uint(fp, &pcache->rest_node) != 0 ||
			fread_uint(fp, &pcache->rest_head) != 0 ||
            fread_uint(fp, &pcache->time_limit) != 0 ||
            fread_uint(fp, (unsigned *) &pcache->lru) != 0)
	{
		goto failed;
	}

	capacity = pcache->capacity;

	if (fread_uint(fp, &pcache->hash.size) != 0)
	{
		goto failed;
	}

	pcache->hash.table = (unsigned *) malloc(sizeof(unsigned) * pcache->hash.size);
	if (pcache->hash.table == NULL)
	{
		goto failed;
	}

	if (fread_uint(fp, pcache->hash.table, pcache->hash.size) != 0)
	{
		goto failed;
	}

	pcache->pnodes = (mc_index_node *) malloc(sizeof(mc_index_node) * capacity);
	if (pcache->pnodes == NULL)
	{
		goto failed;
	}

	if (fread(pcache->pnodes, sizeof(mc_index_node), capacity, fp) != capacity)
	{
		goto failed;
	}

	pcache->pdata = (char *) malloc(size_t(pcache->unit_size) * capacity);
	if (pcache->pdata == NULL)
	{
		goto failed;
	}

	if (fread(pcache->pdata, pcache->unit_size, capacity, fp) != capacity)
	{
		goto failed;
	}

	fclose(fp);

	return pcache;

failed:

	if (pcache != NULL)
	{
		del_memory_cache(pcache);
	}

	if (fp != NULL)
	{
		fclose(fp);
	}

	return NULL;
}

#pragma pack(pop)//恢复对齐状态

