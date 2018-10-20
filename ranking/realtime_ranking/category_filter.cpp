/***************************************************************************
 * 
 * Copyright (c) 2013 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/

/**
 * @file category_filter.cpp
 * @author hongbin2(hongbin@staff.sina.com.cn)
 * @date 2012/04/26
 * @version 1.0
 * @brief 
 * some staff to fill the category(friends, attentions) tags.
 *  
 **/

#include "jsoncpp/jsoncpp.h"
#include "category_filter.h"
#include "ac_utils.h"
#include "ac_ext_data_parser.h"

static const unsigned int MAX_IDX_NUM = 10;

struct {
	int relation;
	int category;
} category_list[] = {
    { SOCIAL_TRUEFRIEND, CATEGORY_FRIEND },
    { SOCIAL_FOLLOWING, CATEGORY_FOLLOW }};

int filter_category(QsInfo *info) {
	struct timeval start_time;
	gettimeofday(&start_time, NULL);

	QsInfoBC *bcinfo = &info->bcinfo;
	QsInfoSocial *social = &info->socialinfo;

	// already filted friends/ flloows
	if ((info->parameters.getUs() == 1) &&
			((info->parameters.start == 0) || (info->parameters.getPage() == 1))) {
		for (int i = 0; i < bcinfo->get_doc_num(); i++) {
			SrmDocInfo *doc = bcinfo->get_doc_at(i);
            if (doc == NULL) {
                continue;
            }
			UNode *node = NULL;
			HASH_SEEK(social->social_uids, GetDomain(doc), node);
			
			if (node != NULL) {
				SLOG_DEBUG("node->relation:%u", node->relation);
				for (int j = 0; j < (int) (sizeof(category_list)/sizeof(category_list[0]));j++) {
					int rel = category_list[j].relation;
					int cat = category_list[j].category;
					if (!(node->relation & rel)) {
						continue;
					}
					
					// 需要保存这个doc到相应的社会化列表
					assert(cat < MAX_CATEGORY_NUM && "bc->Socials overlinked");
					if (bcinfo->socials[cat].size() < MAX_IDX_NUM) {
						assert(doc);
						bcinfo->socials[cat].insert(doc);
					}
				}
			}
		}
		AC_WRITE_LOG(info, "\t(filter_category:%d %lu %lu)", calc_time_span(&start_time)/1000,
				bcinfo->socials[CATEGORY_FRIEND].size(), bcinfo->socials[CATEGORY_FOLLOW].size());
	}
	
	bcinfo->refresh();
	return bcinfo->get_doc_num();
}

