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
	SUSPECT_DELAY_TIME,
	QI20_DELAY_TIME,
	WHITE_PROTECT,
	MANU_HOT_MAX,
	USE_SIMHASH2,
	UID_DUP_IN_COUNT,
	WHITE_BLACK_LIKE_SWITCH,
	GET_DC_DOC_NUM,
	FILM_TIME_THR,
	FILM_VALIDFWNUM_THR,
	FILM_VALIDLIKENUM_THR,
	SOCIAL_TIME_THR,
	TIMELEVEL_4_THR,
	CTYPE_TIMEL_THR,
	FWVALID_FANS,
	VFANS_VALIDFW,
	VFANS_VALIDFW_2,
	VFANS_UNVALIDFW,
	VFANS_VALIDLIKENUM,
	VFANS_VALIDLIKENUM_2,
	VFANS_UNVALIDLIKENUM,
	US_1_SWITCH,
	LEAST_NUM_THR,
	SEVTYPE_VALIDFANS_THRE,
	VFANS_US2,
	VFWNM_US2,
	UNVFWNM_US2,
	FWNUM_FILTER_PERCENT,
	MAX_FILTER_FWNUM_THR,
	DATA_FILTER_PROTECT_WEIBO_NUM,
	TODAY_DAYS_NUM,
	LATER_DAYS_NUM,
	TODAY_DIVIDE_LATERDAYS_THR_1,
	TODAY_DIVIDE_LATERDAYS_THR_2,
	WEIBO_NUM_DAILY_THR_1,
	WEIBO_NUM_DAILY_THR_2,
	WEIBO_NUM_DAILY_THR_3,
	TODAY_WEIBO_NUM_THR,
	HOT_INTERVAL_SPAN_0,
	HOT_INTERVAL_SPAN_1,
	HOT_INTERVAL_SPAN_2,
	HOT_INTERVAL_SPAN_3,
	TIMEL_4_SPAN_0,
	TIMEL_4_SPAN_1,
	TIMEL_4_SPAN_2,
	HOT_US_1_SIMHASH_INDEX,
	HOT_US_1_SIMHASH_STOP,
	DUP_DEL_PER,
	HOT_NEED_FILTER_THRESHOLD,
	TIMEDIFF_THR_0,
	TIMEDIFF_THR_1,
	TIMEDIFF_THR_2,
	TIMEDIFF_THR_3,
	US_TRANSFER_AC,
	MAX_DISPLAY_NUM_GRAY,
	HOT_GRAY_SWITCH,
	TIME_3_FWNUM_THRE,
	TIME_2_FWNUM_THRE,
	TIME_1_FWNUM_THRE,
	/* Comman search  */
	RESORT_DEGRADE,
	STOP_RESORT_DEGRADE,
	DUMP_PROFILTER_COUNT,
	REALTIME_HOTQUERY_CHEAT_CHECK,
	FIRST_PAGE_KEEP_NUM,
	PIC_CHEAT_DELAY_TIME,
	USE_UID_SOURCE_BLACK_SOURCE,
	PIC_CHEAT_ACT_WEIBO_COUNT,
	CLUSTER_TIMEL_LIMIT,
	UID_SOCIAL_PAGE,
	UID_SOCIAL_NUM,
	UID_SOCIAL_USE_GRAY,
	UID_SOCIAL_NUM_GRAY_UID,
	UID_SOCIAL_NUM_GRAY,
	RESULT_KEEP_NUM,
	TOP_N_DEGREE,
	VALID_FORWARD_FWNUM,
	HOT_QUERY_STOP_DUPRECALL,
	KEEP_TOP_N,
	MAX_ACT_RERANK_DOC_DEGRADE,
	CLUSTER_TOP_BUFF,
	RERANK_UPDATE_DEGRADE_STATE2_PER,
	RERANK_UPDATE_DEGRADE_STATE1_PER,
	CLUSTER_TOP_BUFF_2,
	CLUSTER_TOP_BUFF_1,
	RECALL_QI_LOW_DOC_TIME_INTERVAL_NUM,
	RECALL_QI_LOW_DOC_TIME_INTERVAL,
	RECALL_QI_LOW_DOC_TIME_INTERVAL_NUM_WEEK,
	RECALL_QI_LOW_DOC_TIME_INTERVAL_WEEK,
	RECALL_QI_LOW_DOC_TIME_INTERVAL_NUM_MONTH,
	RECALL_QI_LOW_DOC_TIME_INTERVAL_MONTH,
	CLUSTER_FWNUM_VALID,
	CLUSTER_ROOT_VALID_NUM,
	CLUSTER_REPOST_VALID_NUM,
	TOO_FEW_LIMIT_DOC_NUM,
	UID_SOCIAL_NUM_BUF,
	MIN_TOPN_NUM_FOR_RECALL,
	MIN_NUM_SKIP_DUP,
	CLUSTER_VALIDFANS_NUM,
	FWNUM_UPDATE_CLUSTER_TIME,
	UID_SOURCE_COUNT,
	UID_SOURCE_TIME_PASS,
	AC_USE_SOURCE_DICT,
	HOT_CHEAT_CHECK_QI_BITS,
	RERANKING_LATENCY_CTRL_TIMEOUT,
	RERANKING_LATENCY_DEGRADE_CTRL_TIMEOUT,
    	INTENTION_STRATEGY_NUM,
	FOLLOW_TIMELINESS_4_TIMESPAN,
	FOLLOW_TIMELINESS_3_TIMESPAN,
	FOLLOW_TIMELINESS_2_TIMESPAN,
	FOLLOW_TIMELINESS_1_TIMESPAN,
	FOLLOW_TIMELINESS_0_TIMESPAN,
    	CATEGORY_RESULT_HOT,
    	CATEGORY_RESULT_TIMELINE,
	BLACK_LIST_CHECK_SWITCH,
	OPEN_BC_BLACK_CHECK,
	USE_NEW_HOT_PIC_SOURCE,
    	NEW_HOT_PIC_SOURCE_PIC_AS_VIDEO,
    	HOT_PIC_SOURCE_PASS_BY_VALIDFANS,
	IS_DA_CONTROL,
	USE_AUTO_RANKING,
    	HOT_PIC_SOURCE_CHECK_DIGITATTR,
	CLUSTER_FIRST_PAGE_KEEP_NUM_AFTER_CLUSTER,
    	CLUSTER_ROOT_VALID_NUM_HOT,
    	CLUSTER_REPOST_VALID_NUM_HOT,
	CLUSTER_FIRST_PAGE_KEEP_NUM_ACTCLUSTER,
	CLUSTER_FIRST_PAGE_KEEP_NUM_DEGREE,
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
	/* 启动时使用 */
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

