#include "ac_global_conf.h"

AcGlobalConf* AcGlobalConf::m_instance = NULL;
pthread_mutex_t AcGlobalConf::m_mutex = PTHREAD_MUTEX_INITIALIZER;
int AcGlobalConf::InitInt()
{
	/* common */
	conf_name_map_index_int.insert(std::make_pair("comment_show_num", INTV_MAX_FOLLOW_CONF));
	/* ac_hot_sort.cpp */
	conf_name_map_index_int.insert(std::make_pair("intv_max_follow_conf", INTV_MAX_FOLLOW_CONF));
	conf_name_map_index_int.insert(std::make_pair("social_up_follower_conf", SOCIAL_UP_FOLLOWER_CONF));
	/* ac_hot_rank.cpp */
	conf_name_map_index_int.insert(std::make_pair("social_time_thr",SOCIAL_TIME_THR));
	conf_name_map_index_int.insert(std::make_pair("timelevel_4_thr",TIMELEVEL_4_THR));
	/* ac_sort.cpp */
	conf_name_map_index_int.insert(std::make_pair("resort_degrade",RESORT_DEGRADE));
	conf_name_map_index_int.insert(std::make_pair("stop_resort_degrade",STOP_RESORT_DEGRADE));
	conf_name_map_index_int.insert(std::make_pair("ac_use_source_dict",AC_USE_SOURCE_DICT));
	conf_name_map_index_int.insert(std::make_pair("hot_cheat_check_qi_bits",HOT_CHEAT_CHECK_QI_BITS));
	conf_name_map_index_int.insert(std::make_pair("intention_strategy_num", INTENTION_STRATEGY_NUM));
	conf_name_map_index_int.insert(std::make_pair("reranking_latency_ctrl_timeout",RERANKING_LATENCY_CTRL_TIMEOUT));
	conf_name_map_index_int.insert(std::make_pair("reranking_latency_degrade_ctrl_timeout",RERANKING_LATENCY_DEGRADE_CTRL_TIMEOUT));
	conf_name_map_index_int.insert(std::make_pair("follow_timeliness_4_timespan", FOLLOW_TIMELINESS_4_TIMESPAN));
	conf_name_map_index_int.insert(std::make_pair("follow_timeliness_3_timespan", FOLLOW_TIMELINESS_3_TIMESPAN));
	conf_name_map_index_int.insert(std::make_pair("follow_timeliness_2_timespan", FOLLOW_TIMELINESS_2_TIMESPAN));
	conf_name_map_index_int.insert(std::make_pair("follow_timeliness_1_timespan", FOLLOW_TIMELINESS_1_TIMESPAN));
	conf_name_map_index_int.insert(std::make_pair("follow_timeliness_0_timespan", FOLLOW_TIMELINESS_0_TIMESPAN));
	conf_name_map_index_int.insert(std::make_pair("matched_category_result_hot", CATEGORY_RESULT_HOT));
	conf_name_map_index_int.insert(std::make_pair("matched_category_result_timeline", CATEGORY_RESULT_TIMELINE));
	conf_name_map_index_int.insert(std::make_pair("black_list_check_switch", BLACK_LIST_CHECK_SWITCH));
	conf_name_map_index_int.insert(std::make_pair("open_bc_black_check", OPEN_BC_BLACK_CHECK));
	conf_name_map_index_int.insert(std::make_pair("use_new_hot_pic_source", USE_NEW_HOT_PIC_SOURCE));
	conf_name_map_index_int.insert(std::make_pair("new_hot_pic_source_pic_as_video", NEW_HOT_PIC_SOURCE_PIC_AS_VIDEO));
	conf_name_map_index_int.insert(std::make_pair("hot_pic_source_pass_by_validfans", HOT_PIC_SOURCE_PASS_BY_VALIDFANS));
	conf_name_map_index_int.insert(std::make_pair("is_da_control", IS_DA_CONTROL));
	conf_name_map_index_int.insert(std::make_pair("use_auto_ranking", USE_AUTO_RANKING));
	conf_name_map_index_int.insert(std::make_pair("hot_pic_source_check_digitattr", HOT_PIC_SOURCE_CHECK_DIGITATTR));
	conf_name_map_index_int.insert(std::make_pair("cluster_first_page_keep_num_after_cluster",CLUSTER_FIRST_PAGE_KEEP_NUM_AFTER_CLUSTER));
	conf_name_map_index_int.insert(std::make_pair("cluster_root_valid_num_hot",CLUSTER_ROOT_VALID_NUM_HOT));
	conf_name_map_index_int.insert(std::make_pair("cluster_repost_valid_num_hot",CLUSTER_REPOST_VALID_NUM_HOT));
	conf_name_map_index_int.insert(std::make_pair("cluster_first_page_keep_num_actcluster",CLUSTER_FIRST_PAGE_KEEP_NUM_ACTCLUSTER));
	conf_name_map_index_int.insert(std::make_pair("cluster_first_page_keep_num_degree",CLUSTER_FIRST_PAGE_KEEP_NUM_DEGREE));
	conf_name_map_index_int.insert(std::make_pair("new_src_delay_time",NEW_SRC_DELAY_TIME));
	conf_name_map_index_int.insert(std::make_pair("src_del_threshold",SRC_DEL_THRESHOLD));
	conf_name_map_index_int.insert(std::make_pair("new_src_delay_pages",NEW_SRC_DELAY_PAGES));
	conf_name_map_index_int.insert(std::make_pair("check_source_zone_normal_type", CHECK_SOURCE_ZONE_NORMAL_TYPE));
	conf_name_map_index_int.insert(std::make_pair("check_source_zone_dict_type", CHECK_SOURCE_ZONE_DICT_TYPE));
	conf_name_map_index_int.insert(std::make_pair("check_source_zone_normal_digit_type", CHECK_SOURCE_ZONE_NORMAL_DIGIT_TYPE));
	conf_name_map_index_int.insert(std::make_pair("check_source_zone_dict_digit_type", CHECK_SOURCE_ZONE_DICT_DIGIT_TYPE));
	conf_name_map_index_int.insert(std::make_pair("check_source_zone_qi_source", CHECK_SOURCE_ZONE_QI_SOURCE));
	conf_name_map_index_int.insert(std::make_pair("check_source_zone_qi_or", CHECK_SOURCE_ZONE_QI_OR));
	conf_name_map_index_int.insert(std::make_pair("check_source_zone_qi_zone", CHECK_SOURCE_ZONE_QI_ZONE));
	conf_name_map_index_int.insert(std::make_pair("check_pic_source_dict", CHECK_PIC_SOURCE_DICT));
	conf_name_map_index_int.insert(std::make_pair("check_pic_source_normal", CHECK_PIC_SOURCE_NORMAL));
	conf_name_map_index_int.insert(std::make_pair("check_source_zone_normal_use_acdict", CHECK_SOURCE_ZONE_NORMAL_USE_ACDICT));
	conf_name_map_index_int.insert(std::make_pair("check_source_zone_dict_use_acdict", CHECK_SOURCE_ZONE_DICT_USE_ACDICT));
	m_int_conf = std::vector<int>(INT_MAX_NUM, NO_USE_INT);
	return 0;
}

int AcGlobalConf::InitFloat()
{
	/* ac_hot_rank.cpp */
	conf_name_map_index_float.insert(std::make_pair("ctype_user_buff",CTYPE_USER_BUFF));
	conf_name_map_index_float.insert(std::make_pair("whitelikenum_ratio",WHITELIKENUM_RATIO));
	m_float_conf = std::vector<float>(FLOAT_MAX_NUM, NO_USE_FLOAT);
	return 0;
}

int AcGlobalConf::InitStr()
{
	conf_name_map_index_str.insert(std::make_pair("abtest_jx_switch",ABTEST_JX_SWITCH));
	m_str_conf = std::vector<char*>(STR_MAX_NUM, NO_USE_STR);
	return 0;
}




int AcGlobalConf::Init()
{
	InitInt();
	InitFloat();
	InitStr();
	return 0;
}

int AcGlobalConf::load(ACServer* server)
{
	Init();
	std::map<std::string, ConfInt>::iterator it = conf_name_map_index_int.begin();
	for (;it != conf_name_map_index_int.end(); it++)
	{
		if (it->second < INT_MAX_NUM)
		{
			m_int_conf[it->second] = conf_get_int(server->conf_current, const_cast<char*>((it->first).c_str()), NO_USE_INT);
		}
	}
	std::map<std::string, ConfFloat>::iterator it_f = conf_name_map_index_float.begin();
	for (;it_f != conf_name_map_index_float.end(); it_f++)
	{
		if (it_f->second < FLOAT_MAX_NUM)
		{
			m_float_conf[it_f->second] = conf_get_float(server->conf_current, const_cast<char*>((it_f->first).c_str()), NO_USE_FLOAT);
		}
	}
	std::map<std::string, ConfStr>::iterator it_s = conf_name_map_index_str.begin();
	for (;it_s != conf_name_map_index_str.end(); it_s++)
	{
		if (it_s->second < STR_MAX_NUM)
		{
			m_str_conf[it_s->second] = conf_get_str(server->conf_current, const_cast<char*>((it_s->first).c_str()), NO_USE_STR);
		}
	}
	return 0;
}
int int_from_conf(ConfInt index, int def)
{
	return AcGlobalConf::Instance()->get_int(index, def);
}

float float_from_conf(ConfFloat index, float def)
{
	return AcGlobalConf::Instance()->get_float(index, def);
}

char* str_from_conf(ConfStr index, char* def)
{
	return AcGlobalConf::Instance()->get_str(index, def);
}
