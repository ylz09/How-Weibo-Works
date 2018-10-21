#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <stdio.h>
#include <openssl/md5.h>
#include <gtest/gtest.h>

#include <memq.h>

#include "realtime_rw_index.h"

using ::testing::Test;

TEST_F(Test, RealTimeRWIndex_1)
{
	RealTimeRWIndex index;
}

TEST_F(Test, Load__1)
{
	RealTimeRWIndex index;
	EXPECT_TRUE(index.Load("./", "realtime"));
}


