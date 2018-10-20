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
#include "ac_global_conf.h"
#include "ac/src/plugins_weibo/ac_hot_ext_data_parser.h"
#define SHOW_DEBUG_NEWLINE 2
#define PRINT_RERANK_UPDATE_LOG 2

static const unsigned int MAX_IDX_NUM = 10;

struct {
	int relation;
	int category;
} category_list[] = {
    { SOCIAL_TRUEFRIEND, CATEGORY_FRIEND },
    { SOCIAL_FOLLOWING, CATEGORY_FOLLOW }};

enum DAYS
{
    ONE_HOUR     =  3600,
    FOUR_DAYS    =  3600*24*4,
    THIRTY_DAYS  =  3600*24*30,
    SIXTY_DAYS   =  3600*24*60,
    NINETY_DAYS  =  3600*24*90
};


int check_timelevel(int timel, uint32_t rank)
{
    time_t cur_time = time( NULL );
    
    int tls_4_span = int_from_conf(FOLLOW_TIMELINESS_4_TIMESPAN, ONE_HOUR     );
    int tls_3_span = int_from_conf(FOLLOW_TIMELINESS_3_TIMESPAN, FOUR_DAYS    );
    int tls_2_span = int_from_conf(FOLLOW_TIMELINESS_2_TIMESPAN, THIRTY_DAYS  );
    int tls_1_span = int_from_conf(FOLLOW_TIMELINESS_1_TIMESPAN, SIXTY_DAYS   );
    int tls_0_span = int_from_conf(FOLLOW_TIMELINESS_0_TIMESPAN, NINETY_DAYS  );
    
    switch( timel )
    {
	case 0:
	    return ( cur_time - rank <= tls_0_span ) ? true : false;
	case 1:
	    return ( cur_time - rank <= tls_1_span ) ? true : false;
	case 2:
	    return ( cur_time - rank <= tls_2_span ) ? true : false;
	case 3:
	    return ( cur_time - rank <= tls_3_span ) ? true : false;
	case 4:
	    return ( cur_time - rank <= tls_4_span ) ? true : false;
	default:
	    return true;
    }
    return true;
}

int filter_category(QsInfo *info) {
	struct timeval start_time;
	gettimeofday(&start_time, NULL);

	QsInfoBC *bcinfo = &info->bcinfo;
	QsInfoSocial *social = &info->socialinfo;

	int time_level = 0;
	time_level = info->fit_timeliness;
	
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
						bool valid = check_timelevel(time_level, doc->rank);
						if (valid) 
						{
						    bcinfo->socials[cat].insert(doc);
						}
						else
						{
						    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG , SHOW_DEBUG_NEWLINE,
							    " [timeliness(%d) filter: mid %lu not show as follow card]", time_level, doc->docid);
						}
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

