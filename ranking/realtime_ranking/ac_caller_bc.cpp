/***************************************************************************
 * 
 * Copyright (c) 2011 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file ac_da_caller.cpp
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/01/04
 * @version 1.0
 * @brief 
 *  
 **/
#include "ac_cate.h"

#include "ac_caller_bc.h"
#include <fstream>
#include "boost/algorithm/string.hpp"
#include "boost/lexical_cast.hpp"
#include "ac_include.h"
#include "category_filter.h"
#include "ac_truncate.h"
#include "ac_hot_sort_model.h"

extern ACServer *g_server;

const char * AcBcCaller::getName() {
	return "bc";
}

bool AcBcCaller::isSampleIdx(int idx, QsInfo * info) {
	int m_sample_idx = ac_conf_msample_idx(info);
	int d_sample_idx = ac_conf_dsample_idx(info);
	if (idx == m_sample_idx || idx == d_sample_idx) {
		return true;
	}
	return false;
}

int AcBcCaller::bc_pre_sample(QsInfo * info, int idx, char * output, int maxlen) {
	return bc_pre_inner(info, idx, output, maxlen,  0, 1, 1201);
}

int AcBcCaller::bc_pre_normal(QsInfo * info, int idx, char * output, int maxlen) {
	uint32_t i;
	for (i = 0; i < machines.size(); i++) {
		if (machines[i]->idx == idx) {
			break;
		}
	}
	assert(i < machines.size());
    int count = 0;
    count = machines[i]->count_base;
	if (strcmp(get_bcgroup_name(idx), "social_follows") == 0 ||
        strcmp(get_bcgroup_name(idx), "supplement") == 0 ||
        strcmp(get_bcgroup_name(idx), "jx_history") == 0) {
        int us = info->parameters.getUs();
        std::string xsort = info->parameters.getXsort();
        int dup = info->parameters.dup;
        int nofilter = info->parameters.nofilter;
        if (!(us != 0 && (xsort  == std::string("social") || xsort == std::string("hot")) &&
              dup == 1 && nofilter == 0)) {
            AC_WRITE_LOG(info, " {pass idx:%d ret:-1, us:%d, xsort:%s, dup:%d, nofilter:%d}",
                        idx, us, xsort.c_str(), dup, nofilter);
            return -1;
        }
    }
 
    return bc_pre_inner(info, idx, output, maxlen, 0, count, count);
}

int AcBcCaller::done_all_rank(QsInfo * info) {
	return 0;
}

int AcBcCaller::done_all_postrank(QsInfo * info, int * step, uint32_t *addr)
{
    //若命中翻页cache就不需要再做排序了
    if (info->result.type_incache == PAGE_CACHE)
    {
        *step = AC_STEP_DC;
        return BC_DONE;
    }
    else
    {
        return AcCallerBcBase::done_all_postrank(info, step, addr);
    }
}

int AcBcCaller::bc_after(QsInfo *info, int *step) {
	QsInfoBC *bcinfo = &info->bcinfo;    
    int is_resort = GlobalConfSingleton::GetInstance().getAcConf().global.is_resort 
		&& is_need_resort(info);

	if (bcinfo->get_doc_num() <= info->params.start
			|| (info->params.flag & QSPARAMS_FLAG_DOCID)) {
        AC_WRITE_LOG(info, "\t{bc_after NONE num:%d start:%d}",bcinfo->get_doc_num(), info->params.start);
		return -1;
	} else {
		double di_factor = ac_conf_di_factor(info);
		info->dcinfo.docstart = info->params.start;
		info->dcinfo.docnum = (int) (info->params.num * di_factor);
		if (info->dcinfo.docstart + info->dcinfo.docnum > (int)bcinfo->get_doc_num()) {
			info->dcinfo.docnum = bcinfo->get_doc_num() - info->dcinfo.docstart;
		}
	}

    if (info->result.type_incache == PAGE_CACHE)
    {
        AC_WRITE_LOG(info, "\t{bc_after NONE hit_pcache}");
        return 0;
    }
 
    get_category_cache(info);

    bcinfo->sort();

    if (GlobalConfSingleton::GetInstance().getAcConf().global.category_switch == 1) {
        filter_category(info);
    }

    if (is_resort) {
        ac_sort_filter(info);
    }

    //truncate_size(info);
    if (is_resort) {
        ac_sort_social(info);
    }

	if (info->parameters.getXsort() == "social") {
		if (!is_resort) { /**< grade strategy for ac, keep top 3 is hot, and left is time sort */
            info->bcinfo.sort(0, info->bcinfo.get_doc_num(), time_cmp);
		}
	} else if (info->parameters.getXsort() == "time") {
        info->bcinfo.sort(0, info->bcinfo.get_doc_num(), time_cmp);
    } else if (info->parameters.getXsort() == "fwnum") {
		info->bcinfo.sort(0, info->bcinfo.get_doc_num(), fwnum_cmp);
	} else if (info->parameters.getXsort() == "cmtnum") {
		info->bcinfo.sort(0, info->bcinfo.get_doc_num(), cmtnum_cmp);
	}

	return 0;
}

void AcBcCaller::bc_save_result_socialaction(QsInfo *info,
            int idx, BCResponse *response, int *filter_count) {
    SocialDocValue *res_doc = response->sids;
    info->social_doc_cnt = response->count;
	SocialDocValue *res_doc_end = res_doc + info->social_doc_cnt;
    AC_WRITE_LOG(info, " [save_result_socialaction:%d]", info->social_doc_cnt);

    if (info->social_doc_cnt > MAX_SOCIAL_DOC_NUM) {
        info->social_doc_cnt = MAX_SOCIAL_DOC_NUM;
    }
	memmove(info->social_follows_info, res_doc, sizeof(SocialDocValue) * info->social_doc_cnt);

//    for (res_doc = response->sids; res_doc < res_doc_end; res_doc++) {
//        info->social_follows_info.insert(std::make_pair(res_doc->docid, *res_doc));
    for (res_doc = response->sids; res_doc < res_doc_end; res_doc++) 
	{
        if (info->parameters.debug > 2) {
            AC_WRITE_LOG(info, " [s_follow:%lu,%lu,%u]",
                        res_doc->docid, GET_SOCIAL_DOC_UID(*res_doc), res_doc->time);
        }
    }
}

void AcBcCaller::bc_save_result(QsInfo *info, int idx, BCResponse *response, int *filter_count) {
    QsInfoBC *bcinfo = &info->bcinfo;
    char *extra = NULL;
    int extra_size = 0;
    if (response->ext_size) {
        SLOG_DEBUG("[seqid:%u, idx:%d] the len of ext_size is %u",
                info->seqid, idx, response->ext_size);
        extra = (char*) ac_malloc(info, response->ext_size);
        memmove(extra, response->ids + response->count, response->ext_size);
        extra_size = ext_size((void *) extra);
        if (extra_size <= 0) {
            SLOG_DEBUG("[seqid:%u, idx:%d] bc extra size is zero",
                    info->seqid, idx);
        } else {
            SLOG_DEBUG("[seqid:%u, idx:%d] the len of extra is %ld",
                    info->seqid, idx, *(int64_t*)extra);
            merge_cluster(info, idx, extra);
        }
    }
	if (strcmp(get_bcgroup_name(idx), "social_follows") == 0) {
        return bc_save_result_socialaction(info, idx, response, filter_count);
    }

    DocValue *res_doc = response->ids;
    int res_count = response->count;
    DocValue *res_doc_end = res_doc + res_count;

    int count = 0;
    for (res_doc = response->ids; res_doc < res_doc_end; res_doc++) {
        if (res_doc->score == 0) {
            res_doc->score = res_doc->rank;
        }
        
        SrmDocInfo * doc = info->thread->popDoc();
        if (!doc) {
            SLOG_FATAL("[seqid:%u] can not alloc doc");
            suicide();
        }
        fill_doc(doc, res_doc, extra);
        doc->reserved1 = res_doc->reserved1;
        
        doc->bcidx = idx;
		doc->attr_version = get_bcattr_version(idx);
        doc->research = bcinfo->research;

        if (/*strcmp(info->params.sid, "t_search") != 0 &&*/ count >= 2000) {
            continue;
        }
        if (!bcinfo->insert_doc(doc)) {
            count ++;
            /*delete doc;*/
            info->thread->pushDoc(doc);
            *filter_count += 1;
        } 
    }
}
