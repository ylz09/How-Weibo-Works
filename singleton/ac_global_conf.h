#ifndef _AC_SRC_UTILS_AC_GLOBAL_CONF_H_
#define _AC_SRC_UTILS_AC_GLOBAL_CONF_H_
#include <pthread.h>
#include <vector>
#include <map>
#include <string>
#include <cstring>
#include "ac_type.h"
#include "config.h"
#define NO_USE_INT -0xab
#define NO_USE_FLOAT -100.55
#define NO_USE_STR  "NOUSE"
/*
   @by ylz09
   @2016-01-12 
   @PRO: All the global variables are stored in the singleton. Using conf_get_xxx() to get the value
   @CON: The newly added configuration variable need to modify the enum and insert it to the map
*/ 

// All the names are capitalized as our coding norm 
enum ConfInt
{
	/* common*/
	COMMENT_SHOW_NUM,
	/* For hot search */
	INTV_MAX_FOLLOW_CONF,
	SOCIAL_UP_FOLLOWER_CONF,
	HOT_GRAY_SWITCH,
	TIME_3_FWNUM_THRE,
	TIME_2_FWNUM_THRE,
	TIME_1_FWNUM_THRE,
	/* Comman search  */
	RESORT_DEGRADE,
	STOP_RESORT_DEGRADE,
	USE_AUTO_RANKING,
	//for new source scatter
	NEW_SRC_DELAY_TIME,
	SRC_DEL_THRESHOLD,
	NEW_SRC_DELAY_PAGES,
    	CHECK_SOURCE_ZONE_NORMAL_TYPE,
    	CHECK_SOURCE_ZONE_DICT_TYPE,
    	CHECK_SOURCE_ZONE_NORMAL_DIGIT_TYPE,
    	CHECK_SOURCE_ZONE_DICT_DIGIT_TYPE,
    	CHECK_SOURCE_ZONE_QI_SOURCE,
    	CHECK_SOURCE_ZONE_QI_OR,
    	CHECK_SOURCE_ZONE_QI_ZONE,
    	CHECK_PIC_SOURCE_DICT,
    	CHECK_PIC_SOURCE_NORMAL,
    	CHECK_SOURCE_ZONE_NORMAL_USE_ACDICT,
    	CHECK_SOURCE_ZONE_DICT_USE_ACDICT,
	//Add parameters before INT_MAX_NUM
	INT_MAX_NUM
};
enum ConfFloat
{
	/* ac_hot_rank.cpp */
	CTYPE_USER_BUFF,
	WHITELIKENUM_RATIO,
	FLOAT_MAX_NUM
};
enum ConfStr
{
	/* abtest just for test*/
	ABTEST_JX_SWITCH,
	STR_MAX_NUM
};

class AcGlobalConf
{
public:
	static AcGlobalConf* Instance()
	{
		if (m_instance == NULL)
		{
			pthread_mutex_lock(&m_mutex);
			if (m_instance == NULL)
			{
				m_instance = new AcGlobalConf();
			}
			pthread_mutex_unlock(&m_mutex);
		}
		return m_instance;
	}
	int load(ACServer* server);
	int get_int(ConfInt index, int def)
	{
		if(m_int_conf[index] != NO_USE_INT)
			return m_int_conf[index];
		else
			return def;
	}

	float get_float(ConfFloat index, float def)
	{
		if(m_float_conf[index] - NO_USE_FLOAT <= 1e-4 && m_float_conf[index] - NO_USE_FLOAT >= -1e-4 )
			return def;
		else
			return m_float_conf[index];
	}
	char* get_str(ConfStr index, char* def)
	{
		if(strcmp(m_str_conf[index], NO_USE_STR) != 0)
			return m_str_conf[index];
		else
			return def;
	}
	int Init();
private:
	AcGlobalConf(){}
	AcGlobalConf(const AcGlobalConf &);
	AcGlobalConf & operator=(const AcGlobalConf &);
private:
	static AcGlobalConf* m_instance;
	static pthread_mutex_t m_mutex;
	std::vector<int> m_int_conf;
	std::vector<float> m_float_conf;
	std::vector<char*> m_str_conf;
	/* use when restart */
	std::map<std::string, ConfInt> conf_name_map_index_int; 
	std::map<std::string, ConfFloat> conf_name_map_index_float; 
	std::map<std::string, ConfStr> conf_name_map_index_str; 
	int InitInt();
	int InitFloat();
	int InitStr();
};

int int_from_conf(ConfInt index, int def = NO_USE_INT);
float float_from_conf(ConfFloat index, float def = NO_USE_FLOAT);
char* str_from_conf(ConfStr index, char*  def = NO_USE_STR);
#endif

