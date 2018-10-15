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
	conf_name_map_index_int.insert(std::make_pair("suspect_delay_time", SUSPECT_DELAY_TIME));
	conf_name_map_index_int.insert(std::make_pair("qi20_delay_time", QI20_DELAY_TIME));
	conf_name_map_index_int.insert(std::make_pair("white_protect",WHITE_PROTECT));
	conf_name_map_index_int.insert(std::make_pair("manu_hot_max",MANU_HOT_MAX));
	conf_name_map_index_int.insert(std::make_pair("use_simhash2",USE_SIMHASH2));
	conf_name_map_index_int.insert(std::make_pair("uid_dup_in_count",UID_DUP_IN_COUNT));
	conf_name_map_index_int.insert(std::make_pair("white_black_like_switch",WHITE_BLACK_LIKE_SWITCH));
	conf_name_map_index_int.insert(std::make_pair("get_dc_doc_num",GET_DC_DOC_NUM));
	conf_name_map_index_int.insert(std::make_pair("film_time_thre",FILM_TIME_THR));
	conf_name_map_index_int.insert(std::make_pair("film_validfwnum_thr",FILM_VALIDFWNUM_THR));
	conf_name_map_index_int.insert(std::make_pair("film_validlikenum_thr",FILM_VALIDLIKENUM_THR));
	/* ac_hot_rank.cpp */
	conf_name_map_index_int.insert(std::make_pair("social_time_thr",SOCIAL_TIME_THR));
	conf_name_map_index_int.insert(std::make_pair("timelevel_4_thr",TIMELEVEL_4_THR));
	conf_name_map_index_int.insert(std::make_pair("ctype_timel_thr",CTYPE_TIMEL_THR));
	conf_name_map_index_int.insert(std::make_pair("fwvalid_fans",FWVALID_FANS));
	conf_name_map_index_int.insert(std::make_pair("vfans_validfw",VFANS_VALIDFW));
	conf_name_map_index_int.insert(std::make_pair("vfans_validfw_2",VFANS_VALIDFW_2));
	conf_name_map_index_int.insert(std::make_pair("vfans_unvalidfw",VFANS_UNVALIDFW));
	conf_name_map_index_int.insert(std::make_pair("vfans_validlikenum",VFANS_VALIDLIKENUM));
	conf_name_map_index_int.insert(std::make_pair("vfans_validlikenum_2",VFANS_VALIDLIKENUM_2));
	conf_name_map_index_int.insert(std::make_pair("vfans_unvalidlikenum",VFANS_UNVALIDLIKENUM));
	conf_name_map_index_int.insert(std::make_pair("us_1_switch", US_1_SWITCH));
	conf_name_map_index_int.insert(std::make_pair("least_num_thr",LEAST_NUM_THR));
	conf_name_map_index_int.insert(std::make_pair("sevtype_validfans_thre",SEVTYPE_VALIDFANS_THRE));
	conf_name_map_index_int.insert(std::make_pair("vfans_us2",VFANS_US2));
	conf_name_map_index_int.insert(std::make_pair("vfwnm_us2",VFWNM_US2));
	conf_name_map_index_int.insert(std::make_pair("unvfwnm_us2",UNVFWNM_US2));
	conf_name_map_index_int.insert(std::make_pair("fwnum_filter_percent",FWNUM_FILTER_PERCENT));
	conf_name_map_index_int.insert(std::make_pair("max_filter_fwnum_thr",MAX_FILTER_FWNUM_THR));
	conf_name_map_index_int.insert(std::make_pair("data_filter_protect_weibo_num",DATA_FILTER_PROTECT_WEIBO_NUM));
	conf_name_map_index_int.insert(std::make_pair("today_days_num",TODAY_DAYS_NUM));
	conf_name_map_index_int.insert(std::make_pair("later_days_num",LATER_DAYS_NUM));
	conf_name_map_index_int.insert(std::make_pair("today_divide_laterdays_thr_1",TODAY_DIVIDE_LATERDAYS_THR_1));
	conf_name_map_index_int.insert(std::make_pair("today_divide_laterdays_thr_2",TODAY_DIVIDE_LATERDAYS_THR_2));
	conf_name_map_index_int.insert(std::make_pair("weibo_num_daily_thr_1",WEIBO_NUM_DAILY_THR_1));
	conf_name_map_index_int.insert(std::make_pair("weibo_num_daily_thr_2",WEIBO_NUM_DAILY_THR_2));
	conf_name_map_index_int.insert(std::make_pair("weibo_num_daily_thr_3",WEIBO_NUM_DAILY_THR_3));
	conf_name_map_index_int.insert(std::make_pair("today_weibo_num_thr",TODAY_WEIBO_NUM_THR));
	conf_name_map_index_int.insert(std::make_pair("hot_interval_span_0",HOT_INTERVAL_SPAN_0));
	conf_name_map_index_int.insert(std::make_pair("hot_interval_span_1",HOT_INTERVAL_SPAN_1));
	conf_name_map_index_int.insert(std::make_pair("hot_interval_span_2",HOT_INTERVAL_SPAN_2));
	conf_name_map_index_int.insert(std::make_pair("hot_interval_span_3",HOT_INTERVAL_SPAN_3));
	conf_name_map_index_int.insert(std::make_pair("timel_4_span_0",TIMEL_4_SPAN_0));
	conf_name_map_index_int.insert(std::make_pair("timel_4_span_1",TIMEL_4_SPAN_1));
	conf_name_map_index_int.insert(std::make_pair("timel_4_span_2",TIMEL_4_SPAN_2));
	conf_name_map_index_int.insert(std::make_pair("hot_us_1_simhash_index",HOT_US_1_SIMHASH_INDEX));
	conf_name_map_index_int.insert(std::make_pair("hot_us_1_simhash_stop",HOT_US_1_SIMHASH_STOP));
	conf_name_map_index_int.insert(std::make_pair("dup_del_per",DUP_DEL_PER));
	conf_name_map_index_int.insert(std::make_pair("hot_need_filter_threshold",HOT_NEED_FILTER_THRESHOLD));
	conf_name_map_index_int.insert(std::make_pair("timediff_thr_0",TIMEDIFF_THR_0));
	conf_name_map_index_int.insert(std::make_pair("timediff_thr_1",TIMEDIFF_THR_1));
	conf_name_map_index_int.insert(std::make_pair("timediff_thr_2",TIMEDIFF_THR_2));
	conf_name_map_index_int.insert(std::make_pair("timediff_thr_3",TIMEDIFF_THR_3));
	conf_name_map_index_int.insert(std::make_pair("us_transfer_ac",US_TRANSFER_AC));
	conf_name_map_index_int.insert(std::make_pair("max_display_num_gray",MAX_DISPLAY_NUM_GRAY));
	conf_name_map_index_int.insert(std::make_pair("hot_gray_switch",HOT_GRAY_SWITCH));
	conf_name_map_index_int.insert(std::make_pair("time_3_fwnum_thre",TIME_3_FWNUM_THRE));
	conf_name_map_index_int.insert(std::make_pair("time_2_fwnum_thre",TIME_2_FWNUM_THRE));
	conf_name_map_index_int.insert(std::make_pair("time_1_fwnum_thre",TIME_1_FWNUM_THRE));
	/* ac_sort.cpp */
	conf_name_map_index_int.insert(std::make_pair("resort_degrade",RESORT_DEGRADE));
	conf_name_map_index_int.insert(std::make_pair("stop_resort_degrade",STOP_RESORT_DEGRADE));
	conf_name_map_index_int.insert(std::make_pair("dump_profilter_count",DUMP_PROFILTER_COUNT));
	conf_name_map_index_int.insert(std::make_pair("realtime_hotquery_cheat_check",REALTIME_HOTQUERY_CHEAT_CHECK));
	conf_name_map_index_int.insert(std::make_pair("first_page_keep_num",FIRST_PAGE_KEEP_NUM));
	conf_name_map_index_int.insert(std::make_pair("pic_cheat_delay_time",PIC_CHEAT_DELAY_TIME));
	conf_name_map_index_int.insert(std::make_pair("use_uid_source_black_source",USE_UID_SOURCE_BLACK_SOURCE));
	conf_name_map_index_int.insert(std::make_pair("pic_cheat_act_weibo_count",PIC_CHEAT_ACT_WEIBO_COUNT));
	conf_name_map_index_int.insert(std::make_pair("cluster_timel_limit",CLUSTER_TIMEL_LIMIT));
	conf_name_map_index_int.insert(std::make_pair("uid_social_page",UID_SOCIAL_PAGE));
	conf_name_map_index_int.insert(std::make_pair("uid_social_num",UID_SOCIAL_NUM));
	conf_name_map_index_int.insert(std::make_pair("uid_social_use_gray",UID_SOCIAL_USE_GRAY));
	conf_name_map_index_int.insert(std::make_pair("uid_social_num_gray_uid",UID_SOCIAL_NUM_GRAY_UID));
	conf_name_map_index_int.insert(std::make_pair("uid_social_num_gray",UID_SOCIAL_NUM_GRAY));
	conf_name_map_index_int.insert(std::make_pair("result_keep_num",RESULT_KEEP_NUM));
	conf_name_map_index_int.insert(std::make_pair("top_n_degree",TOP_N_DEGREE));
	conf_name_map_index_int.insert(std::make_pair("valid_forward_fwnum",VALID_FORWARD_FWNUM));
	conf_name_map_index_int.insert(std::make_pair("hot_query_stop_duprecall",HOT_QUERY_STOP_DUPRECALL));
	conf_name_map_index_int.insert(std::make_pair("keep_top_n",KEEP_TOP_N));
	conf_name_map_index_int.insert(std::make_pair("max_act_rerank_doc_degrade",MAX_ACT_RERANK_DOC_DEGRADE));
	conf_name_map_index_int.insert(std::make_pair("cluster_top_buff",CLUSTER_TOP_BUFF));
	conf_name_map_index_int.insert(std::make_pair("rerank_update_degrade_state2_per",RERANK_UPDATE_DEGRADE_STATE2_PER));
	conf_name_map_index_int.insert(std::make_pair("rerank_update_degrade_state1_per",RERANK_UPDATE_DEGRADE_STATE1_PER));
	conf_name_map_index_int.insert(std::make_pair("cluster_top_buff_2",CLUSTER_TOP_BUFF_2));
	conf_name_map_index_int.insert(std::make_pair("cluster_top_buff_1",CLUSTER_TOP_BUFF_1));
	conf_name_map_index_int.insert(std::make_pair("recall_qi_low_doc_time_interval_num",RECALL_QI_LOW_DOC_TIME_INTERVAL_NUM));
	conf_name_map_index_int.insert(std::make_pair("recall_qi_low_doc_time_interval",RECALL_QI_LOW_DOC_TIME_INTERVAL));
	conf_name_map_index_int.insert(std::make_pair("recall_qi_low_doc_time_interval_num_week",RECALL_QI_LOW_DOC_TIME_INTERVAL_NUM_WEEK));
	conf_name_map_index_int.insert(std::make_pair("recall_qi_low_doc_time_interval_week",RECALL_QI_LOW_DOC_TIME_INTERVAL_WEEK));
	conf_name_map_index_int.insert(std::make_pair("recall_qi_low_doc_time_interval_num_month",RECALL_QI_LOW_DOC_TIME_INTERVAL_NUM_MONTH));
	conf_name_map_index_int.insert(std::make_pair("recall_qi_low_doc_time_interval_month",RECALL_QI_LOW_DOC_TIME_INTERVAL_MONTH));
	conf_name_map_index_int.insert(std::make_pair("cluster_fwnum_valid",CLUSTER_FWNUM_VALID));
	conf_name_map_index_int.insert(std::make_pair("cluster_root_valid_num",CLUSTER_ROOT_VALID_NUM));
	conf_name_map_index_int.insert(std::make_pair("cluster_repost_valid_num",CLUSTER_REPOST_VALID_NUM));
	conf_name_map_index_int.insert(std::make_pair("too_few_limit_doc_num",TOO_FEW_LIMIT_DOC_NUM));
	conf_name_map_index_int.insert(std::make_pair("uid_social_num_buf",UID_SOCIAL_NUM_BUF));
	conf_name_map_index_int.insert(std::make_pair("min_topN_num_for_recall",MIN_TOPN_NUM_FOR_RECALL));
	conf_name_map_index_int.insert(std::make_pair("min_num_skip_dup",MIN_NUM_SKIP_DUP));
	conf_name_map_index_int.insert(std::make_pair("cluster_validfans_num",CLUSTER_VALIDFANS_NUM));
	conf_name_map_index_int.insert(std::make_pair("fwnum_update_cluster_time",FWNUM_UPDATE_CLUSTER_TIME));
	conf_name_map_index_int.insert(std::make_pair("uid_source_count",UID_SOURCE_COUNT));
	conf_name_map_index_int.insert(std::make_pair("uid_source_time_pass",UID_SOURCE_TIME_PASS));
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
