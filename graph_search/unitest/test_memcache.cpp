#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <stdio.h>
#include <openssl/md5.h>
#include <gtest/gtest.h>

#include "memory_cache.h"

extern int set_memory_cache_paging(memory_cache * pcache, int lru);

memory_cache * pcache = NULL;
#define uchar unsigned char

class TestCache: public ::testing::Test
{

protected:

	static void SetUpTestCase()
	{
		pcache = create_memory_cache(1001, 512, 1);
		ASSERT_TRUE(pcache != NULL);
	}

	static void TearDownTestCase()
	{
		del_memory_cache(pcache);
	}

	virtual void SetUp()
	{
	}

	virtual void TearDown()
	{
		clean_memory_cache(pcache);
	}
};

TEST_F(TestCache, create_memory_cache__1)
{
	memory_cache * pcache = create_memory_cache(11, 64);
	EXPECT_TRUE(pcache);
	del_memory_cache(pcache);
}

TEST_F(TestCache, clean_memory_cache__1)
{
	int64_t uid = 21112;
	char data[512] = "test_test";
	int ret = add_memory_cache_item(pcache, uid, data, 512);
	EXPECT_TRUE(ret == 0);

	clean_memory_cache(pcache);
	unsigned * pcurpos = memory_cache_getpos(pcache, uid);
	EXPECT_TRUE(pcurpos != NULL);
	EXPECT_TRUE(*pcurpos == unsigned (-1));
}

TEST_F(TestCache, del_memory_cache__1)
{
	memory_cache * pcache = NULL;
	del_memory_cache(pcache);
}

TEST_F(TestCache, add_memory_cache_item__1)
{
	uint64_t md5 = 1111;
	char wuhua[1024] = "wuhua";

	BEGIN_PERFORMANCE(add_memory_cache_item);

	EXPECT_TRUE(add_memory_cache_item(pcache, md5, wuhua, 1024) == 0);
	EXPECT_TRUE(add_memory_cache_item(pcache, md5, wuhua, 1024) == CACHE_NONE);

	END_PERFORMANCE(add_memory_cache_item);
}

TEST_F(TestCache, seek_memory_cache_item__1)
{
	uint64_t md5 = 2222;
	char wuhua[1024] = "wuhua";
	int ret = add_memory_cache_item(pcache, md5, wuhua, 1024);
	EXPECT_TRUE(ret == 0);

	char data[1024];
	ret = seek_memory_cache_item(pcache, md5, data, 1024);
	EXPECT_TRUE(ret == 0);
	EXPECT_TRUE(strcmp(wuhua, data) == 0);

	md5 = 1;
	ret = seek_memory_cache_item(pcache, md5, data, 1024);
	EXPECT_TRUE(ret == CACHE_NOTIN);
}

TEST_F(TestCache, seek_memory_cache_item__2)
{
    uint64_t md5 = 22222;
    char wuhua[1024] = "wuhua";
    EXPECT_TRUE(add_memory_cache_item(pcache, md5, wuhua, 1024) == 0); 

    char data[1024];
    EXPECT_TRUE(seek_memory_cache_item(pcache, md5, data, 1024) == 0); 
    EXPECT_TRUE(strcmp(wuhua, data) == 0); 

    sleep(2);
    EXPECT_TRUE(seek_memory_cache_item(pcache, md5, data, 1024) != 0); 

    md5 = 1;
    EXPECT_TRUE(seek_memory_cache_item(pcache, md5, data, 1024) == CACHE_NOTIN);
}



TEST_F(TestCache, modify_memory_cache_item__1)
{
	uint64_t md5 = 22222;
	char data[] = "天津大学北京大学";
	size_t size = strlen(data);
	int ret = add_memory_cache_item(pcache, md5, data, size);
	EXPECT_TRUE(ret == 0);

	strcpy(data, "北京大学天津大学");
	ret = modify_memory_cache_item(pcache, md5, data, strlen(data));
	EXPECT_TRUE(ret == 0);

	char buf[1024];
	ret = seek_memory_cache_item(pcache, md5, buf, 512);
	EXPECT_TRUE(ret == 0);
	EXPECT_TRUE(strcmp(buf, data) == 0);
}

TEST_F(TestCache, rm_memory_cache_item__1)
{
	uint64_t md5 = 22222;
	char wuhua[1024] = "wuhua";
	int ret = add_memory_cache_item(pcache, md5, wuhua, 1024);
	EXPECT_TRUE(ret == 0);

	ret = rm_memory_cache_item(pcache, md5);
	EXPECT_TRUE(ret == 0);

	ret = rm_memory_cache_item(pcache, md5);
	EXPECT_TRUE(ret == -1);
}

TEST_F(TestCache, rm_memory_cache_item__2)
{
	uint64_t md5 = 111111;
	char wuhua[1024] = "wuhua";

	EXPECT_TRUE(seek_memory_cache_item(pcache, md5, wuhua, 1024) != 0);
	EXPECT_TRUE(add_memory_cache_item(pcache, md5, wuhua, 1024) == 0);
	EXPECT_TRUE(seek_memory_cache_item(pcache, md5, wuhua, 1024) == 0);
	EXPECT_TRUE(rm_memory_cache_item(pcache, md5) == 0);
	EXPECT_TRUE(seek_memory_cache_item(pcache, md5, wuhua, 1024) != 0);
}

TEST_F(TestCache, seek_memory_cache_item__3)
{
	char buf[512] = "lalala";
	uint64_t md5 = 222222;

	memory_cache * pcache = create_memory_cache(4, 512, 1);
	ASSERT_TRUE(pcache != NULL);

	for (int i = 0; i < 5; ++i)
	{
		md5 = i;
		EXPECT_TRUE(add_memory_cache_item(pcache, md5, buf, sizeof(buf)) == 0);
	}

	md5 = 23;

	seek_memory_cache_item(pcache, md5, buf, sizeof(buf));

	EXPECT_TRUE(seek_memory_cache_item(pcache, md5, buf, sizeof(buf)) != 0);

	EXPECT_TRUE(add_memory_cache_item(pcache, md5, buf, sizeof(buf)) == 0);
	EXPECT_TRUE(seek_memory_cache_item(pcache, md5, buf, sizeof(buf)) == 0);
	sleep(2);

	EXPECT_TRUE(seek_memory_cache_item(pcache, md5, buf, sizeof(buf)) != 0);
	EXPECT_TRUE(add_memory_cache_item(pcache, md5, buf, sizeof(buf)) == 0);

	set_memory_cache_paging(pcache, 1);
}

TEST_F(TestCache, dumpe_memory_cache__1)
{
	memory_cache * pcache = create_memory_cache(10001, 1024);
	for (int i = 0; i < 100; ++i)
	{
		char data[1024];
		for (size_t k = 0; k < sizeof(data); ++k)
		{
			data[k] = (k % ('z'-'a')) + 'a';
		}

		for (int j = 0; j < 100; ++j)
		{
			uint64_t md5 = i*i;
			add_memory_cache_item(pcache, md5, data, 1024);
		}
	}

	BEGIN_PERFORMANCE(dumpe_memory_cache);
	ASSERT_TRUE(dumpe_memory_cache(pcache, "./data/mc.dump") == 0);
	END_PERFORMANCE(dumpe_memory_cache);

	del_memory_cache(pcache);
}

TEST_F(TestCache, load_memory_cache__1)
{
	memory_cache * pcache = load_memory_cache("./data/mc.dump");

	BEGIN_PERFORMANCE(load_memory_cache);
	ASSERT_TRUE(pcache != NULL);
	END_PERFORMANCE(load_memory_cache);

	del_memory_cache(pcache);
}

TEST_F(TestCache, update_memory_cache_item__1)
{
}

using ::testing::Test;

TEST_F(Test, create_shared_mcache__1)
{
	shared_mcache * shmc = create_shared_mcache(2, 70000027, 120, -1, 700000000);
	ASSERT_TRUE(shmc != NULL);
	int64_t uid = 21112;
	char data[512] = "test_test";
	int ret = add_memory_cache_item(shmc, uid, data, strlen(data));
	EXPECT_TRUE(ret == 0 || ret == CACHE_NONE);

	int len = 0;
	char buf[1024];
	buf[0] = '\0';
	seek_memory_cache_item(shmc, uid, buf, sizeof(buf), &len);
	printf("%s\n", buf);
}

