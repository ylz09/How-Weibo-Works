/***************************************************************************
 * 
 * Copyright (c) 2014 Sina Weibo, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file ac_hot_rank.cpp
 * @author qiupengfei(pengfei15@staff.sina.com.cn)
 * @date 2014/05/29 10:24:04
 * @brief 
 *  
 **/
#include "ac_hot_rank.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dacore/auto_reference.h>
#include "pthread.h"
#include "slog.h"
#include "ac_so.h"
#include "resort.h"
#include "bitdup.h"
#include "ac_utils.h"
#include "ac_caller_man.h"
#include "ac_php.h"
#include "code_converter.h"
#include "ac_intervene_dict.h"
#include "dacore/resource.h"
#include "ac_hot_ext_data_parser.h"
#include "ac_hot_sort_model.h"
#include "ac_source_id_dict.h"
#include "ac_global_conf.h"
#include "intention_def.h"
#include "ac/src/plugins_weibo/search_dict.h"
#include "ac/src/plugins_weibo/csort_vip_base.h"
using namespace std;
#define PRIU64 "%lu"
#define PRI64 "%ld"
/* 数据筛选阈值 */
const int FORWARD_THRE_DEFALT = 4;
/* 数据统计天数 */
const int TIME_CYCLE = 360;
/* 精选最大展示微博数 */
const int MAX_DISPLAY_NUM = 3;
/* 较新微博提权各种阈值 */
const unsigned THRE_6 = 4;
const unsigned THRE_24 = 8;
const unsigned THRE_48 = 10;
const unsigned THRE_WEEK = 15;
const int TIMESPAN_6 = 6 * 3600;
const int TIMESPAN_24 = 24 * 3600;
const int TIMESPAN_48 = 48 * 3600;
const int TIMESPAN_WEEK = 7 * 24 * 3600;
const int TIME_VALUE_EXTRA = 100000000;
const int MAX_PROMOTE_NUM = 10;
/* 提权降权 */
const float Famous_USER_BUFF = 0.5;
const float BLUE_V_USER_DEBUFF = 0.5;
/* 日志等级参数 */
#define HOT_SCORE_LOG_LEVEL 1
#define HOT_ANTISPAM_LOG_LEVEL 0
#define SHOW_DEBUG_LOG_LEVEL_MAIN 0
#define SHOW_DEBUG_NEWLINE 1

CsortVipBase g_csort_vip_base;
extern "C" 
{
	extern ACServer *g_server;
}

int AcHotWeiboRank::reset()
{
    _hot_weibo_list.clear();
    return 0;
}

int AcHotWeiboRank::do_hot_rank(QsInfo* info, int classifier, int resort_flag, int dup_flag, Ac_hot_statistic & ac_hot_data)
{
    _rank_type = RankType(info);
    /* 降级时不做精选微博排序/翻页 */
    if (info->second_search != 0 ||
        (_rank_type == RANK_SOCIAL && info->parameters.start != 0)) {
        SLOG_DEBUG("start=%d, no hot weibo", info->parameters.start);
        return 0;
    }
	/* 微博还原原始分数，注意最后要还原回来 */
	restore_docscore(info);

	/* 更新社会化时间和最大粉丝数 */
	update_socialtime(info);

    /* 标记那些不满足精选微博标准的结果 */
    mark_bad_result(info, classifier);
    
	/* 用户级筛选 */
	mark_user_unsatisf(info, ac_hot_data);

	/* 根据转发阈值，进一步标记转发不足的微博 */
	mark_fwnum_insuff(info);

    /* 根据分类信息，删选类别没有命中的微博 */
    adjust_category_result(info, int_from_conf(CATEGORY_RESULT_HOT, 40), 1);

    /* 统计各时段优质结果数 */
    stat_result(info);

    /* 根据stat_result的结果，对较新的结果提权 */
    time_result_promote(info);

	/* 分数还原回来 */
	back_docscore(info);

	/* 区间有序，大顺序按区间，区间内按分数排序 */
	sort_by_time_interval(info);

    /* 进行过滤和去重处理，去重要求顺序不会发生大的变化，所以应该放到最后 */
    filter_and_cluster(info, classifier, resort_flag, dup_flag);

    /* 如果是综合排序，标记热门微博，（前面这段描述的目前已经没用了），处理综合排序精选逻辑（us后移至ac）*/
    mark_hot_weibo_for_social(info, classifier);

    return 0;
}
/*
static int NoUse(QsInfo* info)
{
    __gnu_cxx::hash_map<uint64_t, std::vector<SocialDocValue> >::iterator it;
	for (it = info->social_follows_info.begin(); it != info->social_follows_info.end(); it++)
	{
		if ((it->second).size() > 1)
		{
			AC_WRITE_LOG(info, "[inner]\n");
			for (int i = 0; i < (it->second).size(); i++)
			{
				AC_WRITE_LOG(info, "[mid:%lu, uid:%lu, time:%lu, %lu]\n", it->first, GET_SOCIAL_DOC_UID((it->second)[i]), (it->second)[i].time, (it->second)[i].action_type);
			}
			AC_WRITE_LOG(info, "[inner]\n");
		}
		else
			AC_WRITE_LOG(info, "[mid:%lu, uid:%lu, time:%lu, %lu]\n", it->first, GET_SOCIAL_DOC_UID((it->second)[0]), (it->second)[0].time, (it->second)[0].action_type);

	}
	return 0;
}
*/
int AcHotWeiboRank::restore_docscore(QsInfo* info)
{
	if (info == NULL) return 0;
	QsInfoBC *bcinfo = &info->bcinfo;
	if (bcinfo == NULL) return 0;
    for (int i = 0; i < bcinfo->get_doc_num(); i++)
    {
        SrmDocInfo* doc = bcinfo->get_doc_at(i);
		if (doc == NULL || doc->value == 0)
			continue;
		if (doc->value > 10000000)
		{
			doc->value -= 10000000;
			doc->scorenofilter = 1;
		}
	}
	return 0;
}

int AcHotWeiboRank::update_socialtime(QsInfo* info)
{
	/* 类数据成员，初始化为1，后面要用作除数 */
    _max_validfans = 1;
	AC_WRITE_LOG(info, " [fit_timeliness: %d]", info->fit_timeliness);
	QsInfoBC *bcinfo = &info->bcinfo;
	AC_WRITE_LOG(info, " [sorted bef:%d]", bcinfo->get_doc_num());
    int inten_user_type = intention_user_type(info->intention);
	int social_time_thr = int_from_conf(SOCIAL_TIME_THR, 2);
	int timelevel_4_thr = int_from_conf(TIMELEVEL_4_THR, 1);
	time_t cur_time = time(NULL);
	/* 类成员函数保存当前时间 */
	_cur_time = cur_time;
	int timel = info->getTimeliness();
    for (int i = 0; i < bcinfo->get_doc_num(); i++)
    {
        SrmDocInfo* doc = bcinfo->get_doc_at(i);
		if (doc == NULL || doc->value == 0)
			continue;
        if (doc->validfans && doc->validfans > _max_validfans) 
		{
            _max_validfans = doc->validfans;
        }
		UpdateFamousConcernInfo(info, doc, inten_user_type);
		/* query_log 时效性为4的query，发布时间一小时内的更新社会化时间，否则只取发布时间，其他时效性都更新 */
		if (timel < 4 || cur_time - doc->rank <= timelevel_4_thr * 3600)
		{
			UpdataTimeOne(info, doc, inten_user_type, cur_time);
		}
		/* 修正社会化时间，时间最多提权2天 */
		/* time_old ！=0 说明更新了社会化时间 */
		if (doc->time_old != 0)
		{
			time_t diff = (doc->rank > doc->time_old) ? (doc->rank - doc->time_old) : 0;
			doc->rank = (diff < unsigned ((social_time_thr * 24 * 3600))) ? doc->rank : (doc->time_old + social_time_thr * 24 * 3600);
		}
		/* 修正结束 */
	}
	AC_WRITE_LOG(info," [max_vfans:%lu]", _max_validfans);
	return 0;
}

/* 微博热度的一个评价标准 */
static uint64_t GetHotNum(const AcExtDataParser& attr) 
{
    float hot_num = attr.get_likenum() * 0.3 + attr.get_cmtnum() * 0.3 + attr.get_fwnum();
    return uint64_t(hot_num);
}

/* 转发数，有效赞数和有效粉丝数达到各自阈值则豁免 */
static int PassByFwnum(const AcExtDataParser& attr, SrmDocInfo* doc) 
{
   // uint64_t fwnum, uint64_t cmtnum, uint32_t validfwnm,
   //         uint64_t whitelike_num) {
   //         attr.get_fwnum()
   //         attr.get_cmtnum(), attr.get_validfwnm(), attr.get_whitelikenum()
    //fwnum, cmtnum, attr.get_validfwnm(), attr.get_whitelikenum())) {
#define PASS_BY_FWNUM_NUM 300
#define PASS_BY_CMTNUM_NUM 300
#define PASS_BY_LIKE_NUM 300
#define PASS_BY_VALIDFANS_NUM 500000
    int64_t fwnum = attr.get_fwnum();
    //int64_t cmtnum = attr.get_cmtnum();
    int64_t validfwnm = attr.get_validfwnm();
	int64_t like_num = attr.get_likenum();
	int white_black_like_switch = int_from_conf(WHITE_BLACK_LIKE_SWITCH, 0);
    int64_t whitelike_num = attr.get_whitelikenum();
    int64_t valid_fans_level = attr.get_valid_fans_level();
	int white_protect = int_from_conf(WHITE_PROTECT, 1);
    if (validfwnm > 1)
	{
        fwnum = fwnum * validfwnm / 1000;
        if (fwnum > PASS_BY_FWNUM_NUM) 
		{
            return 1;
        }
    }
	if (white_black_like_switch == 0)
	{
		if (whitelike_num > PASS_BY_LIKE_NUM)
		{
			return 2;
		}
	}
	else if (whitelike_num > 1)
	{
		like_num = like_num * whitelike_num / 1000;
		if (like_num > PASS_BY_LIKE_NUM)
			return 2;
	}

	if (PassByWhite(attr) && white_protect)
	{
		return 3;
	}
	/*
    if (cmtnum > PASS_BY_CMTNUM_NUM) {
        return 3;
    }
	*/
    if (valid_fans_level) 
	{
        return 4;
	}
    if (doc->validfans && doc->validfans > PASS_BY_VALIDFANS_NUM) 
	{
        return 5;
    }
    return 0;
}

int AcHotWeiboRank::mark_bad_result(QsInfo* info, int classifier)
{
    /* 来源打压词典 */
    SourceIDDict* dict = Resource::Reference<SourceIDDict>("source_id.dict");
    if (dict == NULL)
    {
        SLOG_WARNING("load source_id.dict fail");
    }
	auto_reference<DASearchDict>  uids_ctype("uids_ctype");
	if (!(*uids_ctype))
	{
		SLOG_FATAL("uids_ctype dict is unavaliable!");
	}
    int timel = info->getTimeliness();
    uint64_t qtype = info->intention;
    //gettimelevel(info, &timel, &qtype);
    AC_WRITE_LOG(info, " [timel:%d, intention:%lu]", timel, qtype);
    timel = info->fit_timeliness;
    AC_WRITE_LOG(info, " [fit:timel:%d]", timel);
    /*
	   AC_WRITE_LOG(info, " [VALUE:%lu, %d, %d, %d]",
                _max_validfans, _time_to_promote, _time_promote_num, _promote_fwnum);
	*/
	float ctype_user_buff = float_from_conf(CTYPE_USER_BUFF, 0.2);
	AC_WRITE_LOG(info, "ctype_user_buff:%f", ctype_user_buff);
	int ctype_timel_thr = int_from_conf(CTYPE_TIMEL_THR, 2);
    /* 先打压低质 */
    for (int i = 0; i < info->bcinfo.get_doc_num(); i++)
    {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (doc == NULL)
			continue;
        if (doc->docid == info->parameters.debugid) 
		{
            AC_WRITE_LOG(info, " [hot_score_in:%lu, %u]", doc->docid, doc->value);
        }
        AcExtDataParser attr(doc);
		/* 群体作弊直接过滤 */
		uint64_t group_cheat = attr.get_group_cheat();
		if (!PassByWhite(attr) && group_cheat) {
			doc->value = 0;
			if (doc->docid == info->parameters.debugid) 
			{
				AC_WRITE_LOG(info, "[%lu, group_cheat and not white vip or weibo]", doc->docid);
			}
			if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
			{
				AC_WRITE_LOG(info, "[%lu, group_cheat and not white vip or weibo]", doc->docid);
			}
			continue;
		}
		uint64_t fwnum = attr.get_fwnum();
        uint64_t cmtnum = attr.get_cmtnum();
		uint64_t likenum = attr.get_likenum();
        //uint64_t hotnum = GetHotNum(attr);
        uint64_t qi = attr.get_qi();
        uint64_t uid = GetDomain(doc);
        uint32_t vip_type = GetVipType(qtype, timel);
        uint32_t check_vip_type = g_csort_vip_base.CheckFamousUser(uid, vip_type);
        uint64_t digit_attr = attr.get_digit_attr();
		/* 精选库目前只有原创， 所以下面两个if没作用 */
        if (GET_FORWARD_FLAG(digit_attr) && !(check_vip_type & (1 << VIP_USERS_INFLUENCE)))
        {
            /* 转发结果在这里也是低质 */
            doc->value = 0;
            if (doc->docid == info->parameters.debugid) 
			{
                AC_WRITE_LOG(info, "value=0,%d,%lu", __LINE__, doc->docid);
            }
            if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
			{
                AC_WRITE_LOG(info, " [%d,%lu]", __LINE__, doc->docid);
            }
            continue;
        }
        if (GET_FORWARD_FLAG(digit_attr) && check_vip_type & (1 << VIP_USERS_INFLUENCE)) 
		{
            if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
			{
                AC_WRITE_LOG(info, " [forward:%lu,influ_user:%lu]", doc->docid, uid);
                DOC_WRITE_ACDEBUG(doc, " forward ");
            }
            if (doc->docid == info->parameters.debugid) 
			{
                AC_WRITE_LOG(info, " [forward:%lu,influ_user:%lu]", doc->docid, uid);
            }
        }
		/* 打压企业蓝v，提权其他认证类型，尤其是名人 */
        uint64_t vtype = attr.get_verified_type();
        if (attr.get_verified() && vtype == 2 && !PassByWhite(attr)) 
		{
            double score = 1.0 * doc->value * BLUE_V_USER_DEBUFF;
            if (score > doc->value) 
			{
                score = doc->value;
            }
            doc->value = uint32_t(doc->value - score);
            if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
			{
                AC_WRITE_LOG(info, " s_debuf:%0.1f, vtype:%lu", score, vtype);
                DOC_WRITE_ACDEBUG(doc, " s_debuf:%0.1f, vtype:%lu", score, vtype);
            }
        } 
		else 
		{
            if (doc->validfans) 
			{
                float score = doc->value * 1.0 * doc->validfans / _max_validfans;
                if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
				{
                    AC_WRITE_LOG(info, " s_fans:%0.1f, %u, %lu, orig:%u", score, doc->validfans, _max_validfans, doc->value);
                    DOC_WRITE_ACDEBUG(doc, " s_fans:%0.1f, %u, %lu, orig:%u", score, doc->validfans, _max_validfans, doc->value);
                }
                doc->value = uint32_t(doc->value + score);
                if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
				{
                    AC_WRITE_LOG(info, " s_fans:%0.1f, %u, %lu, %u", score, doc->validfans, _max_validfans, doc->value);
                    DOC_WRITE_ACDEBUG(doc, " s_fans:%0.1f, %u, %lu, %u", score, doc->validfans, _max_validfans, doc->value);
                }
            }
            uint32_t vip_type_del_influe = check_vip_type;
            if (check_vip_type & (1 << VIP_USERS_INFLUENCE)) 
			{
                vip_type_del_influe = check_vip_type ^ (1 << VIP_USERS_INFLUENCE);
            }
            double score = 0.0;
            if (vip_type_del_influe) 
			{
                score = 1.0 * doc->value * Famous_USER_BUFF;
                doc->value = uint32_t(doc->value + score);
                if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
				{
                    AC_WRITE_LOG(info, " s_vip:%0.1f, ori:%d, d:%d",
                                score, check_vip_type, vip_type_del_influe);
                    DOC_WRITE_ACDEBUG(doc, " s_vip:%0.1f, ori:%d, d:%d",
                                score, check_vip_type, vip_type_del_influe);
                }
            }
            if (doc->docid == info->parameters.debugid) 
			{
                AC_WRITE_LOG(info, " [s_buf:%lu,vip:%0.1f, ori:%d, d:%d]",
                            doc->docid, score, check_vip_type, vip_type_del_influe);
            }
        }
		double buff_value = 0.0;
		/* ctype_user_switch = 2 时不走这个策略 
		                     = 1 时走所有用户
							 = 0 时走2和6
		*/
		if (uid && *uids_ctype && timel <= ctype_timel_thr)
		{
			//uint64_t cuid = info->parameters.cuid;
            //if (ctype_user_switch == 1 || cuid == 0 ||(cuid > 0 && ctype_user_switch == 0 &&( (cuid / 10) % 10 == 2 ||(cuid / 10) % 10 == 6 )))
			//{
			int ctype = uids_ctype->Search(uid);
			if (ctype != -1)
			{
				buff_value = 1.0 * doc->value * ctype_user_buff;
				doc->value = uint32_t(doc->value + buff_value);
				AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
						" [docid:%lu, Buff by ctype:%d, buff:%f, after:%u]", doc->docid, ctype, buff_value, doc->value);
			}
			//}
		}
        if (PassByFwnum(attr, doc)) 
		{
            if (doc->docid == info->parameters.debugid) 
			{
                AC_WRITE_LOG(info, "PassByFwnum:%d,%lu, fw:%lu,cmt:%lu,likenum:%lu, valid:%lu,white:%lu,value:%u,validfans:%d",
                            __LINE__, doc->docid, fwnum, cmtnum,likenum, attr.get_validfwnm(),
                            attr.get_whitelikenum(), doc->value, doc->validfans);
            }
            if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
			{
                AC_WRITE_LOG(info, "PassByFwnum:%d,%lu, fw:%lu,cmt:%lu,likenum:%lu, valid:%lu,white:%lu,value:%u,validfans:%d",
                            __LINE__, doc->docid, fwnum, cmtnum,likenum, attr.get_validfwnm(),
                            attr.get_whitelikenum(), doc->value, doc->validfans);
            }
            continue;
        }
		/* 优质微博判断 */
        bool is_good = is_good_doc(info, doc, classifier, qi, fwnum, cmtnum, dict);
        /* 综合序的精选微博要求更严格一些,这个暂时没用,参数不会传SOCIAL */
        if (is_good && _rank_type == RANK_SOCIAL && !IsHotDoc(doc, timel))
        {
            if (doc->docid == info->parameters.debugid) 
			{
                AC_WRITE_LOG(info, "%d,%lu", __LINE__, doc->docid);
            }
            if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
			{
                AC_WRITE_LOG(info, " %d,%lu", __LINE__, doc->docid);
            }
            is_good = false;
        }
        if (doc->docattr & (0x1ll << 37) ||
            doc->docattr & (0x1ll << 37) ||
            doc->docattr & (0x1ll << 38) ||
            doc->docattr & (0x1ll << 40) ||
            doc->docattr & (0x1ll << 41)) 
		{
            if (doc->docid == info->parameters.debugid) 
			{
                AC_WRITE_LOG(info, "docattr %d,%lu %lu", __LINE__, doc->docid, doc->docattr);
            }
            if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
			{
                AC_WRITE_LOG(info, " docattr %d,%lu %lu", __LINE__, doc->docid, doc->docattr);
            }
            is_good = false;
        }
        if (!is_good)
        {
            SLOG_DEBUG("notgood: id:%u, value=%u, uid=%llu, rank=%u", doc->docid, doc->value, GetDomain(doc), doc->rank);
            doc->value = 0;
            if (doc->docid == info->parameters.debugid) 
			{
                AC_WRITE_LOG(info, "not good %d,%lu", __LINE__, doc->docid);
            }
            if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
			{
                AC_WRITE_LOG(info, " not good %d,%lu", __LINE__, doc->docid);
            }
        } 
		else 
		{
            if (doc->docid == info->parameters.debugid) 
			{
                AC_WRITE_LOG(info, "good %d,%lu", __LINE__, doc->docid);
            }
            if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) {
                AC_WRITE_LOG(info, " good %d,%lu,%u", __LINE__, doc->docid, doc->value);
            }
        }
    }
    info->bcinfo.sort();
    if (dict != NULL)
    {
        dict->UnReference();
    }
    return 0;
}

/* 综合搜索前三条精选展示，要求更加严格，反作弊 */
static int AntiSpamDoc(QsInfo* info, SrmDocInfo *doc) 
{
    AcExtDataParser attr(doc); 
    uint64_t validfw = attr.get_validfwnm();
    uint64_t unvifwnm = attr.get_unvifwnm();
    uint64_t like_num = attr.get_likenum();
	int white_black_like_switch = int_from_conf(WHITE_BLACK_LIKE_SWITCH, 0);
    uint64_t whitelike_num = attr.get_whitelikenum();
    uint64_t blacklike_num = attr.get_blacklikenum();
    uint64_t valid_likenum = 0, unvalid_likenum = 0;
    if (white_black_like_switch == 0 && like_num) 
	{
        valid_likenum = whitelike_num * 1000 / like_num;
        unvalid_likenum = blacklike_num * 1000 / like_num;
    }
	else if (white_black_like_switch == 1)
	{
        valid_likenum = whitelike_num;
        unvalid_likenum = blacklike_num;
	}
    uint64_t intention = info->intention;
	uint64_t digit_attr = attr.get_digit_attr();
    int fwvalid_fans = int_from_conf(FWVALID_FANS, 500); 
    int vfans_validfw = int_from_conf(VFANS_VALIDFW, 100); 
    int vfans_validfw_2 = int_from_conf(VFANS_VALIDFW_2, 150); 
    int vfans_unvalidfw = int_from_conf(VFANS_UNVALIDFW, 80); 
    int vfans_validlikenum = int_from_conf(VFANS_VALIDLIKENUM, 100); 
    int vfans_validlikenum_2 = int_from_conf(VFANS_VALIDLIKENUM_2, 150); 
    int vfans_unvalidlikenum = int_from_conf(VFANS_UNVALIDLIKENUM, 80); 
	/* 如果是热词并且是带有图片并且是综合搜索 */
    if ((digit_attr & 0x1) && (INTENTION_HOTSEARCH_WORD & intention) &&
         info->parameters.getUs() == 1 &&
         doc->validfans < unsigned(fwvalid_fans)) 
	{
         if (validfw < unsigned(vfans_validfw) || 
             valid_likenum < unsigned(vfans_validlikenum) ||
             (unvifwnm > unsigned(vfans_unvalidfw) &&
              validfw >= unsigned(vfans_validfw) &&
              validfw < unsigned(vfans_validfw_2)) ||
             (unvalid_likenum > unsigned(vfans_unvalidlikenum) &&
              valid_likenum >= unsigned(vfans_validlikenum) &&
              valid_likenum < unsigned(vfans_validlikenum_2))) 
		 { 
             doc->value = 0;
             AC_WRITE_DOC_DEBUG_LOG(info, doc, HOT_ANTISPAM_LOG_LEVEL, SHOW_DEBUG_NEWLINE,
                         " [bad_vfans:%lu,vfans:%u,fw:%lu,unfw:%lu, white:%lu, black:%lu, filter:%lu]",
                         doc->docid, doc->validfans, validfw, unvifwnm, valid_likenum, unvalid_likenum, digit_attr);
         }
		 else 
		 {
             AC_WRITE_DOC_DEBUG_LOG(info, doc, HOT_ANTISPAM_LOG_LEVEL, SHOW_DEBUG_NEWLINE,
                         " [good_vfans:%lu,vfans:%u,fw:%lu,unfw:%lu, white:%lu, black:%lu, filter:%lu]",
                         doc->docid, doc->validfans, validfw, unvifwnm, valid_likenum, unvalid_likenum, digit_attr);
         }
     }
    return 0;
}

static int hot_cheat_check_qi_type(QsInfo *info, Ac_hot_statistic &ac_hot_data, SrmDocInfo *doc, uint64_t qi,
            int check_type) 
{
    bool hit_source = false, hit_zone = false, hit_or = false;
    if (qi & ac_hot_data.check_source_zone_qi_zone) 
        hit_zone = true;
    if (qi & ac_hot_data.check_source_zone_qi_source)
        hit_source = true;
    if (qi & ac_hot_data.check_source_zone_qi_or)
        hit_or = true;
    if (check_type == 0) 
	{
        if ((hit_source && hit_zone) || hit_or) 
		{
			AC_WRITE_DOC_DEBUG_LOG(info, doc, HOT_ANTISPAM_LOG_LEVEL, SHOW_DEBUG_NEWLINE,
                "[hit_cheat_check_qi:"PRIU64","PRIU64", hot_cheat_check_qi_bits:%d]",
                doc->docid, qi, ac_hot_data.hot_cheat_check_qi_bits);
			//AC_WRITE_LOG(info,"[hit_cheat_check_qi and:"PRIU64","PRIU64", hot_cheat_check_qi_bits:%d]", doc->docid, qi, ac_hot_data.hot_cheat_check_qi_bits);
            return 1;
        }
    } 
	else if (check_type == 1) 
	{
        if (hit_source || hit_zone || hit_or) 
		{
			AC_WRITE_DOC_DEBUG_LOG(info, doc, HOT_ANTISPAM_LOG_LEVEL, SHOW_DEBUG_NEWLINE,
                "[hit_cheat_check_qi:"PRIU64","PRIU64", hot_cheat_check_qi_bits:%d]",
                doc->docid, qi, ac_hot_data.hot_cheat_check_qi_bits);

			//AC_WRITE_LOG(info,"[hit_cheat_check_qi or:"PRIU64","PRIU64", hot_cheat_check_qi_bits:%d, hit_source:%d, hit_zone:%d, hit_or:%d]", doc->docid, qi, ac_hot_data.hot_cheat_check_qi_bits, hit_source, hit_zone, hit_or);
			return 1;
        }
    }
    return 0;
}

static int hot_cheat_check_del(QsInfo *info, SrmDocInfo *doc,
            uint64_t qi, uint64_t source, uint64_t digit_attr,
            Ac_hot_statistic &ac_hot_data) 
{
    //if (doc->rank > ac_hot_data.delay_time) {
	uint64_t uid = GetDomain(doc); 
	if (ac_hot_data.is_check_open) 
	{
		AC_WRITE_DOC_DEBUG_LOG(info, doc, HOT_ANTISPAM_LOG_LEVEL, SHOW_DEBUG_NEWLINE,
				" [DEL_LOG hot_cheat_check:"PRI64",qi:"PRI64"]", doc->docid, qi);
		doc->value = 0;
	}
    //}
    return 0;
}

static int CheckPicSourceType(QsInfo *info, SrmDocInfo *doc, Ac_hot_statistic &ac_hot_data,
            const AcExtDataParser& attr, uint64_t digit_attr,
            int check_type, int check_digit) 
{
    uint64_t qi = attr.get_qi();
    uint64_t source = attr.get_digit_source();
    // check qi, set in antispam queue
    // check_type == 0: (hit_source and hit_zone) || (hit_short)
    // check_type == 1: hit_source or hit_zone or hit_short
    if (hot_cheat_check_qi_type(info, ac_hot_data, doc, qi, check_type)) 
	{
        hot_cheat_check_del(info, doc, qi, source, digit_attr, ac_hot_data);
        AC_WRITE_LOG(info, " [hot_q,DEL_LOG:hot_qi_del,"PRIU64","PRIU64","PRIU64"]",
                    doc->docid, GetDomain(doc), source);
        return 1;
    }
    // flag pass
    if (!ac_hot_data.ac_use_source_dict) 
        return 0;
    // check ac's dict
    // check_digit==-1, check every weibo
    if ((check_digit == -1) || (digit_attr & check_digit))
	{
        char s_uid[128];
        int len = snprintf(s_uid, 128, "%lld:%lld", GetDomain(doc), source);
        s_uid[len] = 0;
        bool hit_source = false, hit_zone = false;
       if (ac_hot_data.pic_source && ac_hot_data.pic_source->Search(s_uid, len) == -1) 
            hit_source = true;
        if (qi & ac_hot_data.check_source_zone_qi_zone) 
            hit_zone = true;
        if (check_type == 0) 
		{
            if (hit_source && hit_zone) 
			{
                hot_cheat_check_del(info, doc, qi, source, digit_attr, ac_hot_data);
                AC_WRITE_LOG(info, " [hot_q,DEL_LOG:hot_pic_del and,"PRIU64","PRIU64","PRIU64"]",
                            doc->docid, GetDomain(doc), source);
                return 1;
            } 
			else 
			{
				AC_WRITE_DOC_DEBUG_LOG(info, doc, HOT_ANTISPAM_LOG_LEVEL, SHOW_DEBUG_NEWLINE,
                    " [hot_q:"PRIU64","PRIU64","PRIU64",pic_save]",
                            doc->docid, GetDomain(doc), source);
                return 0;
            }
        } 
		else if (check_type == 1) 
		{
            if (hit_source || hit_zone) 
			{
                hot_cheat_check_del(info, doc, qi, source, digit_attr, ac_hot_data);
                AC_WRITE_LOG(info, " [hot_q,DEL_LOG:hot_pic_del or,"PRIU64","PRIU64","PRIU64"]",
                            doc->docid, GetDomain(doc), source);
                return 1;
            } 
			else 
			{
				AC_WRITE_DOC_DEBUG_LOG(info, doc, HOT_ANTISPAM_LOG_LEVEL, SHOW_DEBUG_NEWLINE,
                    " [hot_q:"PRIU64","PRIU64","PRIU64",pic_save]",
                            doc->docid, GetDomain(doc), source);
                return 0;
            }
        }
    }
    return 0;
}


static int CheckPicSource(QsInfo *info, SrmDocInfo *doc, Ac_hot_statistic & ac_hot_data, const AcExtDataParser& attr, uint64_t digit_attr) 
{
    if (doc->rank < ac_hot_data.curtime - ac_hot_data.uid_source_time_pass) 
	{
		AC_WRITE_DOC_DEBUG_LOG(info, doc, HOT_ANTISPAM_LOG_LEVEL, SHOW_DEBUG_NEWLINE,
            " [time < data's time pass CheckPicSource:"PRIU64", "PRIU64", %d]",
            doc->docid, doc->rank, ac_hot_data.uid_source_time_pass);

		//AC_WRITE_LOG(info, " [time < data's time pass CheckPicSource:"PRIU64", "PRIU64", %d]", doc->docid, doc->rank, ac_hot_data.uid_source_time_pass);
        return 0;
    }
    if (doc->validfans >= ac_hot_data.pass_by_validfans) 
	{
		AC_WRITE_DOC_DEBUG_LOG(info, doc, HOT_ANTISPAM_LOG_LEVEL, SHOW_DEBUG_NEWLINE,
            " [validfans:%d >= %d, pass CheckPicSource:"PRIU64"]",
            doc->validfans, ac_hot_data.pass_by_validfans, doc->docid);
		//AC_WRITE_LOG(info, " [validfans:%d >= %d, pass CheckPicSource:"PRIU64"]", doc->validfans, ac_hot_data.pass_by_validfans, doc->docid);
        return 0;
    }
    if (ac_hot_data.check_pic_source_dict_query) 
        return CheckPicSourceType(info, doc, ac_hot_data, attr, digit_attr,
                    ac_hot_data.check_source_zone_dict_type, ac_hot_data.check_source_zone_dict_digit_type);
    return CheckPicSourceType(info, doc, ac_hot_data, attr, digit_attr,
                ac_hot_data.check_source_zone_normal_type, ac_hot_data.check_source_zone_normal_digit_type);
}

int AcHotWeiboRank::mark_user_unsatisf(QsInfo* info, Ac_hot_statistic & ac_hot_data)
{
    AC_WRITE_LOG(info, " [mark_user_unsatisf]");
	int valid_num = 0;
	int spam_del_num = 0;
	int pic_source_del = 0;
	/* us = 1时的反作弊策略  */
	int us_1_switch = int_from_conf(US_1_SWITCH, 1);
	AC_WRITE_LOG(info, "[us_1_switch:%d]", us_1_switch);
	//AC_WRITE_LOG(info, "heihei:%s", str_from_conf(ABTEST_JX_SWITCH));
	int least_num_thr = int_from_conf(LEAST_NUM_THR, 20);
	int white_protect = int_from_conf(WHITE_PROTECT, 1);
	auto_reference<ACStringSet> img_check_query("hot_query.txt");
    std::string key;
    if (info->params.key) {
        key = info->params.key;
    } else {
        key = info->params.query;
    }
    if (*img_check_query && img_check_query->test(key)) 
	{
		if (ac_hot_data.check_pic_source_dict)
			ac_hot_data.check_pic_source_dict_query = true;
	}
	AC_WRITE_LOG(info, "[check_pic_source_dict:%d]",  ac_hot_data.check_pic_source_dict_query);
	for (int i = 0; i < info->bcinfo.get_doc_num(); ++i)
	{
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (NULL == doc || doc->value == 0)
			continue;	
		AcExtDataParser attr(doc); 
		if (PassByWhite(attr) &&  white_protect) 
		{
			if (doc->docid == info->parameters.debugid) 
			{
				AC_WRITE_LOG(info, " %lu, white us=1 pass", doc->docid);
			}
			if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
			{
				AC_WRITE_LOG(info, " %lu, white us=1 pass", doc->docid);
			}
			continue;
		}
		if (us_1_switch == 1)
			AntiSpamDoc(info, doc);
		if (doc->value == 0) 
		{
            ++spam_del_num;
			continue;	
        }
		uint64_t digit_attr = attr.get_digit_attr();
		if (info->parameters.getUs() == 1)
			CheckPicSource(info, doc, ac_hot_data, attr, digit_attr);
		if (doc->value == 0)
		{
			pic_source_del += 1;
			continue;
		}
		valid_num++;
	}
	AC_WRITE_LOG(info, " [spam del:%d]", spam_del_num);
	AC_WRITE_LOG(info, " [pic source del:%d]", pic_source_del);
	if (valid_num <= least_num_thr) 
	{
        AC_WRITE_LOG(info, " [valid_num <= 20]");
        info->bcinfo.sort();
		return 0;
    }
	/* us = 2 时的反作弊策略,从后往前删除 */
	int del_num = 0;
	for (int i = info->bcinfo.get_doc_num() - 1; i >= 0; --i)
	{
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (NULL == doc || doc->value == 0)
			continue;	
		/*  保证剩余20条以上 */
		if(valid_num - del_num <= least_num_thr)
			break;
		AcExtDataParser attr(doc); 
		if (PassByFwnum(attr, doc))
		{ 
			if (doc->docid == info->parameters.debugid) 
			{
				AC_WRITE_LOG(info, " %lu, PassByFwnum user pass", doc->docid);
			}
			if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) {
				AC_WRITE_LOG(info, " %lu, PassByFwnum user pass", doc->docid);
			}
			continue;
		}
		/* 广告 作弊用户直接过滤  bs 层应该已经干预过了 */
		uint64_t qi = attr.get_qi();
		/* 作弊是1042 qi第低位为广告位 */
		if (qi == 1042 || qi & 0x1)
		{
			del_num++;
			doc->value = 0;
			if (doc->docid == info->parameters.debugid) 
			{
				AC_WRITE_LOG(info, " %lu,userselect fail cheat or ad, qi:%lu",doc->docid, qi);
			}
			if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
			{
				AC_WRITE_LOG(info, " %lu,userselect fail cheat or ad, qi:%lu",doc->docid, qi);
			}
			continue;
		}
		uint64_t fwnum = attr.get_fwnum();
		uint64_t cmtnum = attr.get_cmtnum();
		uint64_t like_num = attr.get_likenum();
        uint64_t vtype = attr.get_verified_type();
		int sevtype_validfans_thre = int_from_conf(SEVTYPE_VALIDFANS_THRE, 1500); 
		/* 团体蓝V  有效粉丝数 < 1500 */
		if (attr.get_verified() && vtype == 7 && doc->validfans < unsigned(sevtype_validfans_thre)) 
		{
			del_num++;
			doc->value = 0;
			if (doc->docid == info->parameters.debugid) 
			{
				AC_WRITE_LOG(info, " %lu,userselect utype fail,uid:%lu, utype:%lu, validfans:%u",doc->docid,GetDomain(doc), vtype, doc->validfans);
			}
			if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
			{
				AC_WRITE_LOG(info, " %lu,userselect utype fail,uid:%lu, utype:%lu, validfans:%u",doc->docid,GetDomain(doc), vtype, doc->validfans);
			}
			continue;
		}
		/* 有效粉丝数 20 并且  （有效转发不足千分之50， 或者 无效转发大于 10% 或者  有效赞不足 千分之 50 或者 无效赞 大于 10%） */
		uint64_t validfw = attr.get_validfwnm();
		uint64_t unvifwnm = attr.get_unvifwnm();
		int vfans_us2 = int_from_conf(VFANS_US2, 20); 
		int vfwnm_us2 = int_from_conf(VFWNM_US2, 50); 
		int unvfwnm_us2 = int_from_conf(UNVFWNM_US2, 100); 
		if (doc->validfans < unsigned(vfans_us2) && (validfw < unsigned(vfwnm_us2) || unvifwnm > unsigned(unvfwnm_us2)))
		{
			del_num++;
			doc->value = 0;
			if (doc->docid == info->parameters.debugid) 
			{
				AC_WRITE_LOG(info, " %lu,userselect fail reason2, uid:%lu, validfans:%u, fw:%lu, vfw:%lu, unvfw:%lu",doc->docid,GetDomain(doc), doc->validfans, fwnum, validfw, unvifwnm);
			}
			if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
			{
				AC_WRITE_LOG(info, " %lu,userselect fail reason2, uid:%lu, validfans:%u, fw:%lu, vfw:%lu, unvfw:%lu",doc->docid,GetDomain(doc), doc->validfans, fwnum, validfw, unvifwnm);
			}
			continue;
		}
	}
	AC_WRITE_LOG(info, " [user del:%d]", del_num);
    info->bcinfo.sort();
	return 0;
}

/* 转发数序，少->多 */
static bool my_fwnum_cmp(const SrmDocInfo * a, const SrmDocInfo *b)
{
	AcExtDataParser attr_a(a);
	AcExtDataParser attr_b(b);
	int64_t fwnum_a = attr_a.get_fwnum();
	int64_t fwnum_b = attr_b.get_fwnum();
	if (fwnum_a != fwnum_b)
	{
		return fwnum_a < fwnum_b;   
	}                    
	else if (a->rank != b->rank)
	{
		 return a->rank < b->rank;   
	} 
	else if (a->value != b->value) 
	{
		return a->value < b->value; 
	}                               

	else if (a->docid != b->docid) 
	{
		return a->docid < b->docid; 
	}                               
	else if (a->bcidx != b->bcidx) 
	{
		return a->bcidx > b->bcidx;
	}
	else 
	{                          
		return a->bsidx < b->bsidx; 
	}
}

int AcHotWeiboRank::forward_num_threshold(QsInfo* info, int &valid_num)
{
	QsInfoBC *bcinfo = &info->bcinfo;
	int total = 0;
	time_t cur_time = _cur_time;
	/* 根据转发数排序（倒序）*/
	bcinfo->sort(0, bcinfo->get_doc_num(), my_fwnum_cmp);	
	/* TIME_CYCLE中最旧的时间，用于计算实际跨越天数 */
	_days_count = TIME_CYCLE;
	time_t oldest_time = cur_time;
	for (int i = 0; i < bcinfo->get_doc_num(); ++i)
	{
		SrmDocInfo *doc = bcinfo->get_doc_at(i);
		if (NULL ==  doc)
			continue;	
		if (doc->value == 0)
			continue;
		time_t doc_time = doc->rank;
		/* 时间超过周期不需要统计了 */
		if (cur_time - doc_time > TIME_CYCLE * 3600 * 24)
			continue;
		total++;
		/* 保存最旧的时间 */
		if(doc_time < oldest_time)
			oldest_time = doc_time;
	}
	valid_num = total;
	AC_WRITE_LOG(info, " [satified num in 360 days:%d]", total);
	/* 最旧的时间和目前时间相距离的天数(四舍五入) */
	_days_count = int ((cur_time - oldest_time) / (3600.0f * 24) + 0.5f);
	if (_days_count <= 0)
		_days_count = 1;
	if (_days_count > TIME_CYCLE)
		_days_count = TIME_CYCLE;
	AC_WRITE_LOG(info, " [true days:%d]", _days_count);
	/* 计算转发阈值 */
	/* 80%为 或者 _days_count * 10 那条微博的fwnum中的最大值 */
	int fwnum_filter_percent = int_from_conf(FWNUM_FILTER_PERCENT, 80);
	int fwnum_thr_80_percent_pos = int(total * (1 - fwnum_filter_percent / 100.0f));
	//int fwnum_thr_10_multi_days_counts_pos = total - FORWARD_AVE * _days_count; 
	//AC_WRITE_LOG(info, " [10*days_counts的位置:%d]add2", fwnum_thr_10_multi_days_counts_pos);
	int valid_num_count = 0;
	int max_filter_fwnum_thr = int_from_conf(MAX_FILTER_FWNUM_THR, 100);
	int final_fwnum_thr = max_filter_fwnum_thr;
	int flag = 0;
	//if (fwnum_thr_10_multi_days_counts_pos < 0)
	//	flag++;
	for (int i = 0; i < bcinfo->get_doc_num(); ++i)
	{
		SrmDocInfo *doc = bcinfo->get_doc_at(i);
		if (NULL ==  doc || doc->value == 0 || cur_time - doc->rank > TIME_CYCLE * 3600 * 24)
			continue;	
		if (fwnum_thr_80_percent_pos == valid_num_count)
		{
			flag++;
			AcExtDataParser attr(doc);
			int64_t fwnum = attr.get_fwnum(); 
			if(fwnum < final_fwnum_thr)
			{
				final_fwnum_thr = fwnum;
			}
		}
		/*
		if (fwnum_thr_10_multi_days_counts_pos == valid_num_count)
		{

			flag++;
			AcExtDataParser attr(doc);
			int64_t fwnum = attr.get_fwnum(); 
			AC_WRITE_LOG(info, " [10*days_counts的位置的微博的转发数:%d]add4", fwnum);
			if(fwnum < final_fwnum_thr)
				final_fwnum_thr = fwnum;
		} 
		*/
		valid_num_count++;
		if (flag == 1)
			break;
	}
	AC_WRITE_LOG(info, " [fwnum thre:%d]", final_fwnum_thr);
	return	final_fwnum_thr > FORWARD_THRE_DEFALT ? final_fwnum_thr : FORWARD_THRE_DEFALT; 
}

int AcHotWeiboRank::mark_fwnum_insuff(QsInfo * info)
{
	int left_weibo_num = 0;
	/* 计算数据筛选阈值 */
	int thre = forward_num_threshold(info, left_weibo_num);	
	int data_filter_protect_weibo_num = int_from_conf(DATA_FILTER_PROTECT_WEIBO_NUM, 100);
	if (left_weibo_num < data_filter_protect_weibo_num)
	{
		info->bcinfo.sort();	
		return 0;
	}
	int filter_num = 0;
	if (thre > FORWARD_THRE_DEFALT)
	{
		for (int i = 0; i < info->bcinfo.get_doc_num(); i++)
		{
			SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
			if (NULL == doc)
				continue;
			if (doc->value == 0)
				continue;
			AcExtDataParser attr(doc);
			uint64_t fwnum = attr.get_fwnum();
			/* 豁免依旧 */
			if (PassByFwnum(attr, doc)) 
			{
                if (doc->docid == info->parameters.debugid) 
				{
                    AC_WRITE_LOG(info, " %lu, PassByFwnum FwnumSelect pass", doc->docid);
                }
                if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) {
                    AC_WRITE_LOG(info, " %lu, PassByFwnum FwnumSelect pass", doc->docid);
                }
				continue;
			}
			if (fwnum < (unsigned)thre)	
			{
				filter_num++;
				doc->value = 0;
				if (doc->docid == info->parameters.debugid) 
				{
					AC_WRITE_LOG(info, "fwnum filter %d,%lu", __LINE__, doc->docid);
				}
				if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) 
				{

					AC_WRITE_LOG(info, "fwnum filter %d,%lu", __LINE__, doc->docid);
				}

			}
		}
	}
    info->bcinfo.sort();	
	AC_WRITE_LOG(info, " [data filter num:%d]", filter_num);
	return 1;
}

int AcHotWeiboRank::stat_result(QsInfo* info)
{
	QsInfoBC *bcinfo = &info->bcinfo;
    int hot6 = 0;
    int hot24 = 0;
    int hot48 = 0;
    int hot_week = 0;
    time_t cur_time = _cur_time;
    for (int i = 0; i < bcinfo->get_doc_num(); i++)
    {
        SrmDocInfo* doc = bcinfo->get_doc_at(i);
		if (doc == NULL || doc->value == 0)
			continue;
        AcExtDataParser attr(doc);
        uint64_t hotnum = GetHotNum(attr);
        uint64_t digit_attr = attr.get_digit_attr();
        if (GET_FORWARD_FLAG(digit_attr))
        {
            continue;
        }
        int diff_time = cur_time - doc->rank;
        if (hotnum >= THRE_6 && diff_time < TIMESPAN_6)
        {
            hot6++;
        }
        if (hotnum >= THRE_24 && diff_time < TIMESPAN_24)
        {
            hot24++;
        }
        if (hotnum >= THRE_48 && diff_time < TIMESPAN_48)
        {
            hot48++;
        }
        if (hotnum >= THRE_WEEK && diff_time < TIMESPAN_WEEK)
        {
            hot_week++;
        }
    }
    SLOG_DEBUG("resort_for_time: hot6:%d, hot24:%d, hot48:%d, hot_week:%d", hot6, hot24, hot48,hot_week);
    _time_to_promote = cur_time;
    _time_promote_num = 0;
    _promote_fwnum = THRE_WEEK;
    if (hot6 > 5)
    {
        _time_to_promote = cur_time - TIMESPAN_6;
        _time_promote_num = hot6 / 2;
        _promote_fwnum = THRE_6;
    }
    else if (hot24 > 5)
    {
        _time_to_promote = cur_time - TIMESPAN_24;
        _time_promote_num = hot24 / 2;
        _promote_fwnum = THRE_24;
    }
    else if (hot48 > 5)
    {
        _time_to_promote = cur_time - TIMESPAN_48;
        _time_promote_num = hot48 / 2;
        _promote_fwnum = THRE_48;
    }
    else if (hot_week > 5)
    {
        _time_to_promote = cur_time - TIMESPAN_WEEK;
        _time_promote_num = hot_week / 2;
        _promote_fwnum = THRE_WEEK;
    }
    if (_time_promote_num > MAX_PROMOTE_NUM)
    {
        _time_promote_num = MAX_PROMOTE_NUM;
    }
    return 0;
}
int AcHotWeiboRank::time_result_promote(QsInfo* info)
{
	QsInfoBC *bcinfo = &info->bcinfo;
    /*
	   不提权多条同一用户的结果
	   不提权多条重复的结果
	*/
    uint64_t promote_uid[MAX_PROMOTE_NUM];
    int promote_dup[MAX_PROMOTE_NUM];
    int promote_num = 0;
    int i = 0;
    for (i = 0; i < bcinfo->get_doc_num(); i++)
    {
        SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (doc == NULL || doc->value == 0)
		{
			continue;
        }
        AcExtDataParser attr(doc);
        //uint64_t digit_attr = attr.get_digit_attr();
        uint64_t fwnum = attr.get_fwnum();
        uint64_t hotnum = GetHotNum(attr);
        //uint64_t cmtnum = attr.get_cmtnum();
        //uint64_t qi = attr.get_qi();
        if (doc->rank > unsigned (_time_to_promote) 
                && promote_num < _time_promote_num
                && GetHotNum(attr) >= unsigned(_promote_fwnum))
                //&& doc->value > doc->rank)
        {
            bool found_dup = false;
            for (int j = 0; j < promote_num; j++)
            {
                /* 注意：big uid 没有去重 */
                if (GetDomain(doc) > 1 && promote_uid[j] == GetDomain(doc))
                {
                    found_dup = true;
                    break;
                }
                if (unsigned(promote_dup[j]) == attr.get_dup())
                {
                    found_dup = true;
                    break;
                }
            }
            if (found_dup)
            {
                continue;
            }
            doc->value += TIME_VALUE_EXTRA;
            if (doc->docid == info->parameters.debugid) {
                AC_WRITE_LOG(info, "resort_for_time:%lu,%u", doc->docid, doc->value);
            }
            if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) {
                AC_WRITE_LOG(info, " resort_for_time:%lu,%u", doc->docid, doc->value);
            }
            promote_uid[promote_num] = GetDomain(doc);
            promote_dup[promote_num] = attr.get_dup();
            SLOG_DEBUG("resort_for_time: promoted [uid:%llu, rank:%u, fwnum:%llu, hotnum:%llu]", GetDomain(doc), doc->rank, fwnum, hotnum);
            promote_num++;
			if (promote_num >= _time_promote_num)
				break;
        }
    }
    //SLOG_DEBUG("time_result_promote: resort");
    //bcinfo->sort(0, i);
    //SLOG_DEBUG("time_result_promote: end");
    return 0;
}


int AcHotWeiboRank::stat_resource_timeliness(QsInfo* info)
{
	if (NULL == info)
		return 0;
	QsInfoBC *bcinfo = &info->bcinfo; 
	int today_days_num = int_from_conf(TODAY_DAYS_NUM, 1);
	int later_days_num = int_from_conf(LATER_DAYS_NUM, 7);
	time_t cur_time = _cur_time;
	int today_weibo_num = 0;
	int later_days_weibo_num = 0;
	int total = 0;
	float whitelikenum_ratio = float_from_conf(WHITELIKENUM_RATIO, 0.3);
	int time_3_fwnum_thre = int_from_conf(TIME_3_FWNUM_THRE, 100);
	int time_2_fwnum_thre = int_from_conf(TIME_2_FWNUM_THRE, 10);
	int time_1_fwnum_thre = int_from_conf(TIME_1_FWNUM_THRE, 10);
	/* 这是和展现窗口公用的参数 */
	int timediff_thr_3 = int_from_conf(TIMEDIFF_THR_3, 4);
	int timediff_thr_2 = int_from_conf(TIMEDIFF_THR_2, 30);
	int timediff_thr_1 = int_from_conf(TIMEDIFF_THR_1, 60);
	int white_black_like_switch = int_from_conf(WHITE_BLACK_LIKE_SWITCH, 0);
	std::vector<int> vtype_thre(2,0);
	std::vector<int> over_fwlikethre_num(3,0);
	for (int i = 0; i < bcinfo->get_doc_num(); ++i)
	{
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (NULL == doc)
			continue;
		if (doc->value == 0)
			continue;	
        AcExtDataParser attr(doc);
		uint64_t fwnum = attr.get_fwnum();
		uint64_t like_num = attr.get_likenum();
		time_t doc_time = doc->rank;
		if (cur_time - doc_time > TIME_CYCLE * 3600 * 24)
			continue;
		total++;
		int64_t validfwnm = attr.get_validfwnm();
		int64_t whitelike_num = attr.get_whitelikenum();
		if (white_black_like_switch == 1)
		{
			if (whitelike_num > 1)
				whitelike_num = like_num * whitelike_num / 1000;
			else
				whitelike_num = 0;
		}
		if (validfwnm > 1)
			fwnum = fwnum * validfwnm / 1000;
		else 
			fwnum = 0;
        uint64_t vtype = attr.get_verified_type();
		if (doc_time > cur_time - timediff_thr_3 * 24 * 3600)
		{
			if (attr.get_verified() && vtype == 3)
				vtype_thre[0] += 1;
			if (PassByWhite(attr) == 1)	
				vtype_thre[1] += 1;
			if (fwnum + whitelikenum_ratio * whitelike_num >= time_3_fwnum_thre) 
				over_fwlikethre_num[0] += 1;
		}
		if (doc_time > cur_time - timediff_thr_2 * 24 * 3600)
		{
			if (fwnum + whitelikenum_ratio * whitelike_num >= time_2_fwnum_thre)
				over_fwlikethre_num[1] += 1;
		}
		if (doc_time > cur_time - timediff_thr_1 * 24 * 3600)
		{
			if (fwnum + whitelikenum_ratio * whitelike_num >= time_1_fwnum_thre)
				over_fwlikethre_num[2] += 1;
		}
		if (cur_time - doc_time <= today_days_num * 3600 * 24)
			today_weibo_num++;
		else if(cur_time - doc_time <= (today_days_num + later_days_num) * 3600 * 24)
			later_days_weibo_num++;
	}	
	AC_WRITE_LOG(info, " [resource_timeliness_info:%d,%d,%d,%d,%d]", vtype_thre[0], vtype_thre[1],over_fwlikethre_num[0], over_fwlikethre_num[1], over_fwlikethre_num[2]);
    AC_WRITE_LOG(info, " [valid num aft data filter:%d, todays num:%d, after days num:%d]", total, today_weibo_num, later_days_weibo_num);
	int today_divide_laterdays_thr_1 = int_from_conf(TODAY_DIVIDE_LATERDAYS_THR_1, 3); 
	int today_divide_laterdays_thr_2 = int_from_conf(TODAY_DIVIDE_LATERDAYS_THR_2, 2);
	int weibo_num_daily_thr_1 = int_from_conf(WEIBO_NUM_DAILY_THR_1, 10);
	int weibo_num_daily_thr_2 = int_from_conf(WEIBO_NUM_DAILY_THR_2, 3);
	int weibo_num_daily_thr_3 = int_from_conf(WEIBO_NUM_DAILY_THR_3, 1);
	int today_weibo_num_thr = int_from_conf(TODAY_WEIBO_NUM_THR, 5);
	if ((over_fwlikethre_num[0] >= timediff_thr_3 * weibo_num_daily_thr_1 && vtype_thre[0] + vtype_thre[1] >= 10) || (today_weibo_num >= today_weibo_num_thr && today_weibo_num * later_days_num >= today_divide_laterdays_thr_1 * later_days_weibo_num))
		return 3;
	if ((over_fwlikethre_num[1] >= timediff_thr_2 * weibo_num_daily_thr_2) || (today_weibo_num >= today_weibo_num_thr && today_weibo_num * later_days_num >= today_divide_laterdays_thr_2 * later_days_weibo_num))
		return 2;
	if (over_fwlikethre_num[2] >= timediff_thr_1 * weibo_num_daily_thr_3)
		return 1;
	return 0;
}
	

/* 时间序，新->旧*/
static bool my_time_cmp(const SrmDocInfo * a, const SrmDocInfo *b) 
{
	if (a->rank != b->rank)
	{
		return a->rank > b->rank;   
	}                    
	else if (a->value != b->value) 
	{
		return a->value > b->value; 
	}                               
	else if (a->docid != b->docid) 
	{
		return a->docid > b->docid; 
	}                               
	else if (a->bcidx != b->bcidx) 
	{
		return a->bcidx < b->bcidx;
	}
	else 
	{                          
		return a->bsidx > b->bsidx; 
	}
}


/* 根据时效性，获得时间区间范围 */
static int get_time_zone(int timeliness)
{
	switch (timeliness)
	{
		case 3: 
			{
				int hot_interval_span_3 = int_from_conf(HOT_INTERVAL_SPAN_3, 7);
				return  hot_interval_span_3 * 3600 * 24;
			}
		case 2: 
			{
				int hot_interval_span_2 = int_from_conf(HOT_INTERVAL_SPAN_2, 30);
				return hot_interval_span_2 * 3600 * 24; 
			}
		case 1: 
			{
				int hot_interval_span_1 = int_from_conf(HOT_INTERVAL_SPAN_1, 60);
				return hot_interval_span_1 * 3600 * 24; 
			}
		case 0: 
			{
				int hot_interval_span_0 = int_from_conf(HOT_INTERVAL_SPAN_0, 90);
				return hot_interval_span_0 * 3600 * 24; 

			}
		default: 
			return 30 * 3600 * 24;
	}
}
/* 为了timeliness 4 前三个区间的函数 */
static int get_next_time_zone(int timeliness, int cur_num, int cur_time_zone)
{
	if (timeliness <= 3 || cur_num > 3)
		return cur_time_zone;
	else
	{
		switch (cur_num)
		{
			case 0: 
				{
					int timel_4_span_0 = int_from_conf(TIMEL_4_SPAN_0, 1);
					return timel_4_span_0 * 3600;
				}
			case 1: 
				{
					int timel_4_span_1 = int_from_conf(TIMEL_4_SPAN_1, 3);
					return timel_4_span_1 * 3600; 
				}
			case 2: 
				{
					int timel_4_span_2 = int_from_conf(TIMEL_4_SPAN_2, 20);
					return timel_4_span_2 * 3600; 
				}
			default: 
				{
					int time_4_default = int_from_conf(TIMEL_4_SPAN_2, 720);
					return time_4_default * 3600;
				}
		}
	}
	return 30 * 3600 * 24;
}

int AcHotWeiboRank::back_docscore(QsInfo* info)
{
	if (info == NULL) return 0;
	QsInfoBC *bcinfo = &info->bcinfo;
	if (bcinfo == NULL) return 0;
    for (int i = 0; i < bcinfo->get_doc_num(); i++)
    {
        SrmDocInfo* doc = bcinfo->get_doc_at(i);
		if (doc == NULL || doc->value == 0)
			continue;
		if (doc->scorenofilter == 1)
		{
			doc->value += 10000000;
		}
	}
	return 0;
}

int AcHotWeiboRank::sort_by_time_interval(QsInfo* info)
{
	if (info == NULL)
		return 0;
	/* 这里需要提出value = 0的为后面filter步骤 注意filter步骤中遇到value=0跳过，所以这地方必须加 */
    for (int i = 0; i < info->bcinfo.get_doc_num(); i++)
    {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (doc == NULL)
			continue;
        if (doc->value == 0)
        {
            if (doc->docid == info->parameters.debugid) {
                AC_WRITE_LOG(info, " %d,del %lu", __LINE__, doc->docid);
            }
            if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) {
                AC_WRITE_LOG(info, " %d,del %lu", __LINE__, doc->docid);
            }
            info->bcinfo.del_doc(doc);
        }
    }
	info->bcinfo.refresh();
	/* 资源时效性 */
	int res_timeliness = stat_resource_timeliness(info);
	int final_timeliness = res_timeliness;
    AC_WRITE_LOG(info, " [res timeliness:%d]", res_timeliness);
	/* 拟合时效性 */
    int fit_timeliness = info->fit_timeliness;
	int timel = info->getTimeliness();
	/* 如果querylog时效性和意图时效性有不为0，取query时效性，意图时效性和资源时效性的最大值 */
	if (info->intention != 0 || timel != 0)
	{
		if (fit_timeliness > final_timeliness)
			final_timeliness = fit_timeliness;
		if (timel > final_timeliness)
			final_timeliness = timel;
		if (final_timeliness > 3)
			final_timeliness = 3;
	}
	_final_timeliness = final_timeliness;
    AC_WRITE_LOG(info, " [final timeliness:%d]", final_timeliness);
	int time_zone = 30 * 24 * 3600;
	if (timel > 3)
	{
		time_zone = get_next_time_zone(timel, 0, time_zone);
		AC_WRITE_LOG(info, " [time zone:%d]", time_zone/3600);
	}
	else
	{
		time_zone = get_time_zone(final_timeliness);
		AC_WRITE_LOG(info, " [time zone:%d]", time_zone/(3600 * 24));
	}
	/* 区间排序开始 */
	QsInfoBC *bcinfo = &info->bcinfo;
	int doc_num = bcinfo->get_doc_num();
	bcinfo->sort(0, doc_num, my_time_cmp);

	time_t cur_time = _cur_time;
	int count_docnum = 0;
	int zone_start = 0;
	int zone_end = 0;
	int zone_num = 0;
	/* 通用 */
	for(int i = 0; i < doc_num; ++i)
	{
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (NULL == doc)
		{
			count_docnum++;
			continue;
		}
		time_t doc_time = doc->rank;
	    //AC_WRITE_LOG(info, "\n[docid:%lu, docrank:%lu]\n", doc->docid, doc->rank);
		if(cur_time - doc_time <= time_zone)
		{
			count_docnum++;
		}
		else
		{
			/* 区间开始和结束 */
			zone_end += count_docnum;
			if(zone_start < zone_end)
			{
				/* 这个sort注意下，调用的时候第二个为需要排序个数 - 1 */
				bcinfo->sort(zone_start, zone_end - zone_start);	
				//AC_WRITE_LOG(info, "\n[timezone:%ld - %ld, num:%d]\n", cur_time, cur_time - time_zone, count_docnum);
			}
			/* 重置开始区间 */
			zone_start = zone_end;
			/* num为1（下个区间开始第一个） */
			count_docnum = 1;
			while(cur_time - time_zone > doc_time)
			{
				cur_time = cur_time - time_zone;
				zone_num++;
				time_zone = get_next_time_zone(timel, zone_num ,time_zone);
				if (cur_time < 0 )
				{
					cur_time = 0;
					break;
				}
			}
		}
		/* 如果遍历结束，则再调用一次对剩下的微博排序 */
		if(i == doc_num - 1 && zone_start < doc_num)
		{
			bcinfo->sort(zone_start, doc_num);
		}
		
	}
	return 0;
}

static int HotUs1DupAdd(QsInfo* info, int index, SrmDocInfo *doc, double dup_del_per,
            uint64_t dup, uint64_t cont_sign, uint64_t simhash1, uint64_t domain,
            std::map<uint32_t, HotUs1DupData> *hot_us1_dup_map) {
    HotUs1DupData data;
    data.max_score = doc->value;
    data.max_score_mid = doc->docid;
    data.del_score = uint64_t(doc->value * dup_del_per / 100);
    data.rand_docs = 1;
    data.dup.insert(dup);
    data.cont.insert(cont_sign);
    data.simhash.insert(simhash1);
    data.uid.insert(domain);
    data.docs.insert(make_pair(index, doc));
    hot_us1_dup_map->insert(make_pair(index, data));
	return 0;
}

static void HotUs1Dup(QsInfo* info, int index, SrmDocInfo *doc, double dup_del_per,
            std::map<uint32_t, HotUs1DupData> *hot_us1_dup_map) {
    // size >= 3, value < del_score, i > 30
    AcExtDataParser attr(doc);
    uint64_t dup = attr.get_dup();
    uint64_t cont_sign = attr.get_cont_sign();
    uint64_t simhash1 = attr.get_dup_cont();
    //uint64_t simhash2 = attr.get_simhash2();
    uint64_t domain = GetDomain(doc);
	int hot_us_1_simhash_index = int_from_conf(HOT_US_1_SIMHASH_INDEX, 100);
    if (hot_us1_dup_map->size() == 0) {
        // first add
        HotUs1DupAdd(info, index, doc, dup_del_per, dup, cont_sign, simhash1, domain, hot_us1_dup_map);
    } else {
        std::map<uint32_t, HotUs1DupData>::iterator it = hot_us1_dup_map->begin();
        bool check_dup = false;
        for (; it != hot_us1_dup_map->end(); ++it) {
            // dup
            std::set<uint64_t>::iterator it_dup = it->second.dup.find(dup);
            if (it_dup != it->second.dup.end()) {
                check_dup = true;
                break;
            }
            // cont
            std::set<uint64_t>::iterator it_cont = it->second.cont.find(cont_sign);
            if (it_cont != it->second.cont.end()) {
                check_dup = true;
                break;
            }
            // simhash
            if (index < hot_us_1_simhash_index) {
                std::set<uint64_t>::iterator it_sim = it->second.simhash.begin();
                for (; it_sim != it->second.simhash.end(); ++it_sim) {
                    uint64_t attr_xor = simhash1 ^ (*it_sim);
                    int diff = 0;
                    while (attr_xor > 0) {
                        attr_xor &= (attr_xor - 1);
                        diff++;
                    }
                    if (diff <= 4) {
                        check_dup = true;
                        break;
                    }
                }
                if (check_dup) {
                    break;
                }
            }
            // uid
            std::set<uint64_t>::iterator it_uid = it->second.uid.find(domain);
            if (it_uid != it->second.uid.end()) {
                check_dup = true;
                break;
            }
        }
        if (check_dup && it != hot_us1_dup_map->end()) {
            it->second.dup.insert(dup);
            it->second.cont.insert(cont_sign);
            it->second.uid.insert(domain);
            it->second.simhash.insert(simhash1);
            it->second.docs.insert(make_pair(index, doc));
            if (doc->value >= it->second.del_score) {
                it->second.rand_docs += 1;
            }
        } else {
            // add size
            HotUs1DupAdd(info, index, doc, dup_del_per, dup, cont_sign, simhash1, domain, hot_us1_dup_map);
        }
    }
}

int AcHotWeiboRank::filter_and_cluster(QsInfo* info, int classifier, 
                        int resort_flag, int dup_flag)
{

	int isBidu = 0;
    int deletenum = 0;
    int r_del_num = 0;
    int dup_num  = 0;
    int dup_cont_sign = 0;
    int dup_cont_num = 0;
    int dup_domain_num = 0;
    int resort_num = 0;
	if (strcmp(info->params.sid, "t_search_baidu") == 0) {
		isBidu = 1;
	}
	std::set<uint64_t> dup_set;
	std::set<uint64_t> cont_set;
	std::set<uint64_t> uid_set;
	std::map<uint32_t, HotUs1DupData> hot_us1_dup_map;
    bool us_1 = false;  // info->parameters.getUs() == 1;
	int hot_us_1_simhash_stop = int_from_conf(HOT_US_1_SIMHASH_STOP, 20);
	double dup_del_per = int_from_conf(DUP_DEL_PER, 70);
	int hot_need_filter_threshold = int_from_conf(HOT_NEED_FILTER_THRESHOLD, 20);
    // if (info->parameters.getUs() != 1 &&
    if (info->bcinfo.get_doc_num() < hot_need_filter_threshold) {
        return 0;
    }
    for (int i = 0; i < info->bcinfo.get_doc_num(); i++)
    {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
        resort_num = i;
		if (doc == NULL)
			continue;
        if (doc->value == 0)
        {
            break;
        }
        AcExtDataParser attr(doc);

		uint64_t fwnum = attr.get_fwnum();

		if (isBidu)
			doc->value = fwnum + 1;

		int need_filter = 0;
		int isdupset = 0, isdup_cont = 0;
        if (us_1) {
            // hot only cont, simhash, uid
            HotUs1Dup(info, i, doc, dup_del_per, &hot_us1_dup_map);
            if (hot_us1_dup_map.size() > 3 &&
                i > hot_us_1_simhash_stop) {
                // size >= 3, value < del_score,
                AC_WRITE_LOG(info, " [stop,size:%zu,i:%d]", hot_us1_dup_map.size(), i);
                break;
            }
        } else {
            if (PassByFwnum(attr, doc)) {
                if (doc->docid == info->parameters.debugid) {
                    AC_WRITE_LOG(info, " %lu, PassByFwnum IsNeedFilter", doc->docid);
                }
                if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) {
                    AC_WRITE_LOG(info, " %lu, PassByFwnum IsNeedFilter", doc->docid);
                }
                continue;
            }
            need_filter = IsNeedFilter(info, i, &dup_set, &cont_set, dup_flag, resort_flag, &isdupset, &uid_set, &isdup_cont);
            if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) {
                AC_WRITE_LOG(info, " %lu, uid:%lu", doc->docid, GetDomain(doc));
            }

            if (need_filter) {
                if (doc->docid == info->parameters.debugid) {
                    AC_WRITE_LOG(info, "v=0,need_filter:%d,%lu,dup_set[%zu],cont_set[%zu]",
                                need_filter, doc->docid, dup_set.size(), cont_set.size());
                }
                if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) {
                    AC_WRITE_LOG(info, " v=0,need_filter:%d,%lu,dup_set[%zu],cont_set[%zu]",
                                need_filter, doc->docid, dup_set.size(), cont_set.size());
                }
                doc->value = 0;
                deletenum++;

                if (need_filter == 1) {
                    r_del_num++;
                } else if (need_filter == 2) {
                    dup_num++;
                } else if (need_filter == 3) {
                    dup_cont_sign++;
                } else if (need_filter == 4) {
                    dup_cont_num++;
                } else if (need_filter == 5) {
                    dup_domain_num++;
                }
                if (isdupset) {
                    dup_set.erase(attr.get_dup());
                }
                if (isdup_cont) {
                    cont_set.erase(attr.get_cont_sign());
                }
                continue;
            }
		}
    }
    if (us_1) {
        std::map<uint32_t, HotUs1DupData>::iterator it_delete = hot_us1_dup_map.begin();
        srand((unsigned int)(time((time_t*)NULL)));
        AC_WRITE_LOG(info, " [size:%zu]", hot_us1_dup_map.size());
        int dup_group = 0;
        for (; it_delete != hot_us1_dup_map.end(); ++it_delete) {
            ++dup_group;
            uint32_t rand_docs = it_delete->second.rand_docs;
            if (rand_docs > it_delete->second.docs.size()) {
                rand_docs = it_delete->second.docs.size();
            }
            uint32_t keep_doc_count = rand() % rand_docs;
            AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                        " [g:%d, keep:%d, rand:%d, size:%zu,max_s:%lu,del_s:%lu]",
                        dup_group, keep_doc_count, rand_docs, it_delete->second.docs.size(),
                        it_delete->second.max_score, it_delete->second.del_score);
            uint32_t now_doc_num = 0, pass_doc_num = 0;
            bool keep_ok = false;
            std::map<uint64_t, SrmDocInfo* >::iterator it_map_del = it_delete->second.docs.begin();
            for (; it_map_del != it_delete->second.docs.end(); ++it_map_del) {
                ++pass_doc_num;
                if (it_map_del->second->docid == it_delete->second.max_score_mid) {
                    keep_ok = true;
                    AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                                " [g:%d, keep max:pass:%d, now:%d,%lu:%u]",
                                dup_group, pass_doc_num, now_doc_num, it_map_del->second->docid,
                                it_map_del->second->value);
                    continue;
                }
                if (!keep_ok && pass_doc_num == it_delete->second.docs.size()) {
                    keep_ok = true;
                    AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                                " [g:%d, keep end:pass:%d, now:%d,%lu:%u]",
                                dup_group, pass_doc_num, now_doc_num, it_map_del->second->docid,
                                it_map_del->second->value);
                    continue;
                }
                AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                            " [g:%d, not max, filter_dup:%lu]", dup_group, it_map_del->second->docid);
                if (it_map_del->second->docid == info->parameters.debugid) {
                    DOC_WRITE_ACDEBUG(it_map_del->second, "not max, filter_dup. ");
                }
                info->bcinfo.del_doc(it_map_del->second);
                continue;
                AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                            " [pass:%d, now:%d]", pass_doc_num, now_doc_num);
                if (it_map_del->second->value < it_delete->second.del_score) {
                    AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                                " [filter_dup:%lu,v:%u<%lu]",
                                it_map_del->second->docid, it_map_del->second->value,
                                it_delete->second.del_score);
                    if (it_map_del->second->docid == info->parameters.debugid) {
                        DOC_WRITE_ACDEBUG(it_map_del->second, "filter_dup,v<. ");
                    }
                    info->bcinfo.del_doc(it_map_del->second);
                    continue;
                }
                if (keep_ok) {
                    if (it_map_del->second->docid == info->parameters.debugid) {
                        DOC_WRITE_ACDEBUG(it_map_del->second, "keep_ok, filter_dup. ");
                    }
                    AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                                " [keep_ok,filter_dup:%lu]", it_map_del->second->docid);
                    info->bcinfo.del_doc(it_map_del->second);
                    continue;
                }
                if (now_doc_num != keep_doc_count &&
                    pass_doc_num != it_delete->second.docs.size()) {
                    AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                                " [filter_dup:%lu,not rand]", it_map_del->second->docid);
                    if (it_map_del->second->docid == info->parameters.debugid) {
                        DOC_WRITE_ACDEBUG(it_map_del->second, "filter_dup. ");
                    }
                    info->bcinfo.del_doc(it_map_del->second);
                } else {
                    keep_ok = true;
                    AC_WRITE_LOG(info, " [now_rand_keep:%lu]", it_map_del->second->docid);
                    if (it_map_del->second->docid == info->parameters.debugid) {
                        DOC_WRITE_ACDEBUG(it_map_del->second, "keep.");
                    }
                }
                ++now_doc_num;
            }
        }
    }
	//info->bcinfo.sort(0, resort_num + 1);
	AC_WRITE_LOG(info, " [dup num:%d]", deletenum);
    AC_WRITE_LOG(info, "\tFILTER_HOT:%d r_d=%d d=%d ds=%d dc:%d d_domain=%d r=%d,",
			info->bcinfo.get_doc_num(), r_del_num, dup_num, dup_cont_sign,
            dup_cont_num, dup_domain_num, resort_num);
    return 0;
}



static int get_timediff_from_timeliness(int timeliness, int level = 0)
{
	if (timeliness == 3) 
	{
		int timediff_thr_3 = int_from_conf(TIMEDIFF_THR_3, 4);
		return timediff_thr_3;
	}
	else if (timeliness == 2) 
	{

		int timediff_thr_2 = int_from_conf(TIMEDIFF_THR_2, 30);
		return timediff_thr_2;
	}
	else if (timeliness == 1) 
	{
		if (level == 0)
			return 0;
		else 
		{

			int timediff_thr_1 = int_from_conf(TIMEDIFF_THR_1, 60);
			return timediff_thr_1;

		}
	}
	else
	{			
		if (level == 0)
			return 0;
		else
		{

			int timediff_thr_0 = int_from_conf(TIMEDIFF_THR_0, 90);
			return timediff_thr_0;
		}
	}
	return 0;
}

static float diff_day(time_t a, time_t b)
{
	if (a >= b) 
	{
		return 0.0f;
	}

	return (b - a) / 86400.0f;
}

int AcHotWeiboRank::mark_hot_weibo_for_social(QsInfo* info, int classifer)
{
    int hotnum = 0;
    _hot_weibo_list.clear();
    std::set<uint64_t> uid_set;
    int max_hot_num = HOTFRONTNUM_MOBILE;
    for (int i = 0; i < info->bcinfo.get_doc_num(); i++)
    {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (doc == NULL)
			continue;
        if (_rank_type == RANK_SOCIAL 
                //&& doc->value > doc->rank
                && hotnum < max_hot_num
                && info->second_search == 0
                && info->parameters.getCategory() <= 0
                && uid_set.find(GetDomain(doc)) == uid_set.end())
        {
            hotnum++;
            uid_set.insert(GetDomain(doc));
            info->bcinfo.hot_weibo.push_back(doc);
            continue;
        }
        /* 如果value为0，且是热门排序，删除 */
        if (doc->value == 0 && _rank_type == RANK_HOT)
        {
            if (doc->docid == info->parameters.debugid) {
                AC_WRITE_LOG(info, " %d,del %lu", __LINE__, doc->docid);
            }
            if (info->parameters.debug > HOT_SCORE_LOG_LEVEL) {
                AC_WRITE_LOG(info, " %d,del %lu", __LINE__, doc->docid);
            }
            info->bcinfo.del_doc(doc);
        }
        //如果是综合排序，不满足精选微博条件的结果还要参加实时排序
        else if (_rank_type == RANK_SOCIAL)
        {
            doc->value = doc->rank;
        }
    }
	info->bcinfo.refresh();

	/* us=1逻辑移到ac */
	/* 因为上面得refresh 不用考虑doc == NULL情况 */
	int us_transfer_ac = int_from_conf(US_TRANSFER_AC, 1);
	if (info->parameters.getUs() == 1 && us_transfer_ac == 1)
	{
		/* // 方案-
		int level = 2;
		int display_num = 0;
		time_t cur_time = time(NULL);
		int doc_num = info->bcinfo.get_doc_num();
		int thread = get_timediff_from_timeliness(_timeliness, level);
		for (int i = 0; i < info->bcinfo.get_doc_num(); ++i) 
		{
			if(i < 6)
			{
				SrmDocInfo *doc = info->bcinfo.get_doc_at(i); 
				time_t doc_rank = doc->rank;
				if (diff_day(doc_rank, cur_time) <= thread)
				{
					display_num++;
				}
				else	
					doc->del_flag = 1;
			}
			else
				info->bcinfo.del_doc(doc);	
		}
		if (display_num == 0)
		{
			level--;
			thread = get_timediff_from_timeliness(_timeliness, level);

			for (int i = 0; i < 6; ++i) 
			{
				SrmDocInfo *doc = info->bcinfo.get_doc_at(i); 
				time_t doc_rank = doc->rank;
				if (diff_day(doc_rank, cur_time) <= thread)
				{
					display_num++;
					doc->del_flag = 0;
				}
			}
		}
		info->bcinfo.refresh();
		*/
		uint64_t cuid = info->parameters.cuid;
		int max_display_num = MAX_DISPLAY_NUM;
		int max_display_num_gray = int_from_conf(MAX_DISPLAY_NUM_GRAY, 5);
		int hot_gray_switch = int_from_conf(HOT_GRAY_SWITCH, 1);
        if (hot_gray_switch == 0 || (hot_gray_switch && cuid > 0 && ((cuid / 10) % 10) == 2))
			max_display_num = max_display_num_gray;
		AC_WRITE_LOG(info, " [max_display_num:%d]", max_display_num)
		int level = 2;
		int display_num = 0;
		time_t cur_time = _cur_time;
		int doc_num = info->bcinfo.get_doc_num();
		int thread = get_timediff_from_timeliness(_final_timeliness, level);
		AC_WRITE_LOG(info, "[show thread:%d]", thread);
		for (int i = doc_num > max_display_num ? max_display_num - 1 : doc_num - 1; i >= 0 ; --i) 
		{
			SrmDocInfo *doc = info->bcinfo.get_doc_at(i); 
			if (NULL == doc)
				continue;
			time_t doc_time = doc->rank;
			if (diff_day(doc_time, cur_time) <= thread)
			{
				display_num = i + 1;
				break;
			}
		}
		// because level = 1 is the same with level = 2 ,the following code is no use(previous version level  = 1 the return value is not the same with level = 2)
		//if (display_num == 0)
		//{
		//	int index = -1;
		//	level--;
		//	thread = get_timediff_from_timeliness(_final_timeliness, level);
		//	for (int i = 0; i < doc_num && i < max_display_num; ++i) 
		//	{
		//		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		//		time_t doc_time = doc->rank;
		//		if (diff_day(doc_time, cur_time) <= thread)
		//		{
		//			index = i;
		//			display_num = 1;
		//			break;
		//		}
		//	}
		//	if (index > 0)
		//	{		

		//		SrmDocInfo *doc_first = info->bcinfo.get_doc_at(0);
		//		SrmDocInfo *doc = info->bcinfo.get_doc_at(index);
		//		info->bcinfo.replace(0, doc);
		//		info->bcinfo.replace(index, doc_first);
		//	}
		//}
		if (display_num == max_display_num)
		{
			thread = get_timediff_from_timeliness(_final_timeliness);
			long total_fwnum = 0;
			int supplement_num = 0;
			for (int i = 0; i < info->bcinfo.get_doc_num(); i++)
			{
				SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
				if (NULL == doc)
					continue;
				AcExtDataParser attr(doc);
				int64_t fwnum =attr.get_fwnum();
				if (i < max_display_num)
					total_fwnum += fwnum;
				else if(supplement_num >= MAX_DISPLAY_NUM)
					info->bcinfo.del_doc(doc);
				else
				{
					time_t doc_time = doc->rank;
					if((fwnum * max_display_num >= total_fwnum * 0.5 || fwnum >= 10) && diff_day(doc_time, cur_time) <= thread)
						supplement_num++;
					else
						info->bcinfo.del_doc(doc);
				}
			}
		}
		else
		{
			for (int i = 0; i < info->bcinfo.get_doc_num(); i++)
			{
				SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
				if (i < display_num)
					continue;
				info->bcinfo.del_doc(doc);
			}
		}
		info->bcinfo.refresh();
		// abtest exchange 0 and 1 if abtest
		//if ((info->parameters.abtest & 0x1) && info->bcinfo.get_doc_num() >= 2)
		//{
		//	SrmDocInfo *doc_first = info->bcinfo.get_doc_at(0);
		//	SrmDocInfo *doc = info->bcinfo.get_doc_at(1);
		//	info->bcinfo.replace(0, doc);
		//	info->bcinfo.replace(1, doc_first);
		//}
		for (int i = 0; i < info->bcinfo.get_doc_num(); i++)
		{
			SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
			if (NULL == doc)
				continue;
            AcExtDataParser attr(doc);
			uint64_t unite_0 = *(uint64_t*)((Attr*)(&doc->attr));
			uint64_t unite_1 = *(((uint64_t*)((Attr*)(&doc->attr)))+1);
			AC_WRITE_LOG(info, " [top %d docid:%lu,%lu-%lu-%lu-%lu-%lu-%lu-%lu-%ld-%u-%ld-%d-%lu]",
                        i, doc->docid, unite_0, unite_1, attr.get_uint64_1(), attr.get_uint64_2(),
                        attr.get_uint64_3(), attr.get_uint64_4(), doc->docattr, cur_time - (doc->time_old > 0 ? doc->time_old : doc->rank), doc->validfans, cur_time - doc->rank, _final_timeliness, info->intention); 
		}
	}
    return 0;
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
