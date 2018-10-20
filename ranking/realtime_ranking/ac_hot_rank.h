/***************************************************************************
 * 
 * Copyright (c) 2014 Sina Weibo, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file ac_hot_rank.h
 * @author qiupengfei(pengfei15@staff.sina.com.cn)
 * @date 2014/05/29 09:34:44
 * @brief 精选微博排序，将精选微博排序移到时间排序之前，主要两个目的
 *          1、使得精选微博露出结果排序逻辑与热门排序一致
 *          2、精选微博独立上传给us，不与时间序做排重。 *  
 * @note 精选微博排序，请注意sort的使用，请仔细阅读ac_qs_info_bc.h
 **/
#ifndef AC_HOT_RANK_HOT_H
#define AC_HOT_RANK_HOT_H
#include "ac_hot_sort_model.h"
#include "ac_hot_sort.h"

enum {
    SIMHASH_VIP_TYPE_NONE,
    SIMHASH_VIP_TYPE_BLUEV,
    SIMHASH_VIP_TYPE_OTHER,
};

class AcHotWeiboRank
{
public:
    /**
     * @brief 热门排序入口函数
     *
     * @param [in] info   : QsInfo*
     * @param [in] classifier   : int  在ac_sort.cpp中计算出来的classifer，后续可以考虑删除
     * @return  int 
     * @author qiupengfei
     * @date 2014/05/29 09:38:06
    **/
    int do_hot_rank(QsInfo* info, int classifier, int resort_flag, int dup_flag, Ac_hot_statistic & ac_hot_data);

protected:
	/**
	   *@brief  还原微博原始分数，并且打上标记以便还原回来
	**/
	int restore_docscore(QsInfo* info);
	/**
	  *@brief 还原回来
	**/
	int back_docscore(QsInfo* info);
    /**
     * @brief 更新社会化时间和最大粉丝数
     *
     * @param [in] info   : QsInfo*
     * @return  int 
     * @author qiupengfei
     * @date 2014/07/21 09:40:56
    **/
	int update_socialtime(QsInfo* info);
    /**
     * @brief 标记那些不符合精选微博标准的结果，这里对精选微博和热门排序的标准可能不一致
     *
     * @param [in] info   : QsInfo*
     * @return  int 
     * @author qiupengfei
     * @date 2014/05/29 09:41:12
    **/
    int mark_bad_result(QsInfo* info, int classifier);	
    /**
     * @brief 用户级筛选
     *
     * @param [in] info   : QsInfo*   num  :valid_num
     * @return  int 
     * @author yaozhenjun
     * @date 2015/06/17 14:47:12
    **/
	int mark_user_unsatisf(QsInfo* info, Ac_hot_statistic & ac_hot_data);
    /**
     * @brief 确定转发阈值
     *
     * @param [in] info   : QsInfo*   num  :valid_num
     * @return  int 
     * @author yaozhenjun
     * @date 2015/06/17 14:47:12
    **/
	int forward_num_threshold(QsInfo* info, int &valid_num);
    /**
     * @brief 标记那些不满足转发阈值的结果
     *
     * @param [in] info   : QsInfo*
     * @return  int 
     * @author yaozhenjun
     * @date 2015/06/17 14:47:12
    **/
	int mark_fwnum_insuff(QsInfo * info);
    /**
     * @brief 统计资源时效性
     *
     * @param [in] info   : QsInfo*
     * @param [in] classifier   : int
     * @return  int 
     * @author yaozhenjun
     * @date 2015/06/17 14:47:12
    **/
	int stat_resource_timeliness(QsInfo* info);
    /**
     * @brief 区间有序
     *
     * @param [in] info   : QsInfo*
     * @param [in] classifier   : int
     * @return  int 
     * @author yaozhenjun
     * @date 2015/06/17 14:47:12
    **/
	int sort_by_time_interval(QsInfo* info);
    /**
     * @brief 对结果丰富度进行统计
     *
     * @param [in] info   : QsInfo*
     * @return  int 
     * @author qiupengfei
     * @date 2014/05/29 09:40:56
    **/
    int stat_result(QsInfo* info);
    /**
     * @brief 对较新的结果进行提权
     *
     * @param [in] info   : QsInfo*
     * @return  int 
     * @author qiupengfei
     * @date 2014/05/29 09:42:07
    **/
    int time_result_promote(QsInfo* info);
    /**
     * @brief 去重判断
     *
     * @param [in] info   : QsInfo*
     * @param [in] classifier   : int
     * @return  int 
     * @author qiupengfei
     * @date 2014/05/29 09:43:19
    **/

    int filter_and_cluster(QsInfo* info, int classifier, int resort_flag, int dup_flag);	
    /**
     * @brief 对于综合排序，标记需要传递给us的精选微博
     *
     * @param [in] info   : QsInfo*
     * @param [in] classifer   : int
     * @return  int 
     * @author qiupengfei
     * @date 2014/05/29 09:44:00
    **/
    int mark_hot_weibo_for_social(QsInfo* info, int classifer);
    /**
     * @brief 对于综合排序，将选定的精选微博再插入进去
     *
     * @param [in] info   : QsInfo*
     * @return  int 
     * @author qiupengfei
     * @date 2014/05/29 18:00:46
    **/
    int insert_hot_weibo(QsInfo* info);

    int reset();

protected:
	/* 注意使用前的初始化，后续可以使用构造函数初始化 */
    int _rank_type;
    int _time_to_promote; // 要提权的时间范围
    int _time_promote_num;     //要提权的结果条数
    int _promote_fwnum;   // 要提权的转发数阈值
	int _days_count;   // 满足条件的360天内微博的最久远微博距离当前的天数
	int _final_timeliness;  // 最终时效性
	time_t _cur_time;
    uint64_t _max_validfans;
    std::vector<SrmDocInfo> _hot_weibo_list;
};
#endif  //AC_HOT_RANK_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
