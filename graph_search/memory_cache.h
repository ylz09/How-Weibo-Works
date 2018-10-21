#ifndef  __MEMORY_CACHE_H_
#define  __MEMORY_CACHE_H_

#include <sys/ipc.h>
#include <sys/shm.h>
#include <time.h>

#include "cache_utility.h"

struct memory_cache;

typedef struct memory_cache shared_mcache;

typedef int (*InnerIndexOperator) (char * buf, size_t bufsize, void * param);

memory_cache * create_memory_cache(unsigned hash_num, unsigned unit_size,
		time_t time_limit = -1, unsigned capacity = 0);

shared_mcache * create_shared_mcache(key_t skey, unsigned hash_num, unsigned unit_size,
		time_t time_limit = -1, unsigned capacity = 0);

void clean_memory_cache(memory_cache * pmc);

void del_memory_cache(memory_cache * pmc);

void del_shared_mcache(int shmid);

unsigned * memory_cache_getpos(memory_cache * pcache, uint64_t key);

int operator_memory_cache_item(memory_cache *pmc, uint64_t key, void * param, InnerIndexOperator func);

int seek_memory_cache_item(memory_cache *pmc, uint64_t key, void * pdata, size_t size, int * len = NULL);

int add_memory_cache_item(memory_cache * pmc, uint64_t key, void * pdata, size_t size);

int modify_memory_cache_item(memory_cache *pmc, uint64_t key, void * pdata, size_t size);

int update_memory_cache_item(memory_cache *pcache, uint64_t key, void * pdata, size_t size);

int rm_memory_cache_item(memory_cache *pmc, uint64_t key);

int dumpe_memory_cache(memory_cache *pcache, const char *filename);

memory_cache * load_memory_cache(const char * filename);

#endif  //__MEMORY_CACHE_H_


