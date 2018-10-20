/***************************************************************************
 * 
 * Copyright (c) 2012 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/

/**
 * @file ac_sort.c
 * @author xueyun(xueyun@staff.sina.com.cn)
 * @date 2012/02/20 14:43:17
 * @version 1.0
 * @brief 
 *
 *  
 **/

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
#include "jsoncpp/jsoncpp.h"
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
#include "intention_def.h"
#include "ac_hot_ext_data_parser.h"
#include "ac_hot_sort_model.h"
#include "ac_hot_rank.h"
#include "ac_sandbox.h"
using namespace std;
const int HOT_ADJUST_NUM = 3;  //热门微博可干预的条数

#define HOT_SORT_LOG_LEVEL 1
/// data of building
#if defined(__DATE__) && defined(__TIME__)
static const char server_built[] = "\33[40;32m" __DATE__ " " __TIME__ "\33[0m";
#else
static const char server_built[] = "\33[40;31munknown\33[0m";
#endif

#define MAX_DUP_SIZE	(MAX_DOCID_NUM * 3)

enum {
	F_CONTENT = 2,
	F_TAG = 16,
	F_USERTEXT = 34,
	F_TOPIC_WORDS = 40,
	F_NON_TOPIC_WORDS = 41
};

enum {
	FIELD_DUP_LOW = 22,
	FIELD_DUP_HIGH = 23,
	FIELD_DUP_CONT = 31,
	FIELD_DUP_URL = 32,
	FIELD_QI = 27
};

typedef struct {
	void *bitmap;
	int uid_black_type;
	int source_black_type;
	int source_white_type;
} AcSort;

extern ACServer *g_server;
AcSort g_sort;
#define DOC_DEL_CONTINUE empty ++;DOC_DEL(bcinfo,doc->docid,doc->bsidx);continue;
HASH_DEF_BEGIN( HNode)
HASH_DEF_END enum {
	DUP, DUP_CONT, DUP_URL
};

typedef struct {
	HNode nodes[MAX_DUP_SIZE], *index[MAX_DUP_SIZE + 1];
	uint32_t slot, num;
} node_t;

typedef struct {
	uint8_t field_id;
	uint8_t next;
	uint64_t val;
}__attribute__((packed)) ExtraField;

typedef struct {
	uint8_t index[16];
	uint8_t num;
	ExtraField fields[0];
}__attribute__((packed)) ExtraDoc;

typedef enum {
	CALC_A = 1, CALC_B = 2, CALC_AB = 3, CALC_NONE = 11
} CALC_OPT;

#define Max(a,b) ((a) > (b) ? (a) : (b))
#define Min(a,b) ((a) < (b) ? (a) : (b))
#define MAX_TEXT_LEN (600)
#define ABS_DUP_RATIO (0.7)

void ac_resort(QsInfo *info, int ranktype);
static int TransStr(char *src, int lenSrc, char *dest, int lenDest);
static float Modify(float ratio, int samePoint[], int count, int commonStrLen,
		int strLen);
static int CutLeaves(int a1, int a2, int b1, int b2, int *sCur);
static int Caculate(char *strA, int a1, int a2, char *strB, int b1, int b2,
		int *interValue, char *testValue, int *sCur, int *sIndex,
		int sSamePoint[], int *call_count, CALC_OPT opt);
int SequenceMatcher(QsInfo *info, char *A, int lenA, char *B, int lenB,
		float *ratio, int *call_count);
static int ac_dup_multiforward(char *query, int qlen, char *content, int len);
static int ac_dup_multihuati(char *query, int qlen, char *query_da, char *tag,
		int tlen, char *content, int clen, float *hitratio);
static int ac_dup_multiat(char *query, int qlen, char *utext, int len);
static int ac_get_abs_field(SrmDocInfo *doc, int field, int type, char *buffer,
		int *len);
int ac_unset_default(QsInfo *info);
static int ac_print_info(QsInfo *info, char *title, int num, uint64_t docid);
bool is_bad_doc (QsInfo* info, SrmDocInfo *doc, int classifier,uint64_t qi, int fwnum, int cmtnum, 
                        SourceIDDict* source_dict);

uint64_t ac_sort_uid(SrmDocInfo *doc) {
	return GetDomain(doc);
}

#define DOC_VALUE_ZERO (1)

static uint32_t ac_reset_docvalue(uint32_t value) {
	return value;
}

#define TMP_LOG_LEN 1024

int srm_resort_init(QsInfo *info) {
	QsInfoBC *bcinfo = &info->bcinfo;
	Result *results, *rec;
	SrmDocInfo *pdoc = NULL;
	char *buffer = info->buffer + info->buf_pos;
	char *source_white = bitdup_get(g_sort.bitmap, g_sort.source_white_type);

	results = (Result*) buffer;

	int num = bcinfo->get_doc_num();
	num = RESORT_MAX_NUM > num ? num : RESORT_MAX_NUM;

	int i;
	for (i = 0; i < num; i++) {
		pdoc = bcinfo->get_doc_at(i);

		if (pdoc == NULL)
			continue;

		rec = results + i;
		memcpy(&rec->attr, &pdoc->attr, sizeof(rec->attr));
		rec->uid = GetDomain(pdoc);
		rec->digit_time = pdoc->rank;
		rec->delflag = 0;
		rec->score = pdoc->value;
		rec->iswhite = BITDUP_EXIST(source_white, rec->attr.digit_source,
				MAX_SOURCE_SIZE) ? 1 : 0;

	}
	
	resort(results, num, NULL);

	return 0;
}

int RankType(QsInfo *info) {
	int ret = RANK_SOCIAL;
	if (info->parameters.getXsort() == std::string("social")) {
		ret = RANK_SOCIAL;
	} else if (info->parameters.getXsort() == std::string("time")) {
		ret = RANK_TIME;
	} else if (info->parameters.getXsort() == std::string("hot")) {
		ret = RANK_HOT;
	} else if (info->parameters.getXsort() == std::string("fwnum")) {
        ret = RANK_FWNUM;
    } else if (info->parameters.getXsort() == std::string("cmtnum")) {
        ret = RANK_CMTNUM;
    }
	return ret;
}

int ac_sort_social(QsInfo *info) {
	QsInfoBC *bcinfo = &info->bcinfo;

	//return 0;
	assert(info->magic == MAGIC_QSINFO);

	if (bcinfo->get_doc_num() == 0 || RankType(info) != RANK_SOCIAL
			|| info->parameters.nofilter != 0) {
		return 0;
	}

	QsInfoSocial *social = &info->socialinfo;
	if (!social->has_social) {
		return 0;
	}
	struct timeval start_time;
	gettimeofday(&start_time, NULL);

	char log[1024]; //
	int log_len = sprintf(log, "{ac_sort_social:");

	int i;
	int social_count = 0; //
	int friend_count = 0;
	int intv_max_follow = int_from_conf(INTV_MAX_FOLLOW_CONF, INTV_MAX_FOLLOW);
	int social_up_follower = int_from_conf(SOCIAL_UP_FOLLOWER_CONF, SOCIAL_UP_FOLLOWER);
	for (i = 0; i < (int) bcinfo->get_doc_num(); i++) {
		SrmDocInfo *doc = bcinfo->get_doc_at(i);

		if (doc == NULL || doc->getCategory() == DOC_HOT)
			continue;
		UNode *node;
		HASH_SEEK(social->social_uids, GetDomain(doc), node);
		if (node) {
			/*if (isBlueV == 0) {
			 doc->relation = node->relation;
			 } else {
			 doc->relation = node->relation & SOCIAL_FOLLOWING;
			 }*/

			doc->relation = node->relation;

			unsigned int tmp = doc->value > doc->rank ? doc->value : doc->rank;
			if (node->relation & SOCIAL_FOLLOWING) {
				if (social_count < SOCIAL_RESERVE_NUM) {
					doc->value = tmp + INTV_FOL;
				}
				if ((info->intention & INTENTION_COMMENT) != 0
						&& friend_count++ < social_up_follower) {//口碑类，执行强加权
					doc->value = tmp + intv_max_follow;
				}
			}

			if (social_count < SOCIAL_RESERVE_NUM) {
				if (node->relation & SOCIAL_WORKMATE) {//相同公司
					doc->value = tmp + INTV_COMP;
				}
				if (node->relation & SOCIAL_SCHOOLMATE) {//相同学校
					doc->value = tmp + INTV_SCHO;
				}
				if (node->relation & SOCIAL_FRIEND) {//间接好友
					doc->value = tmp + INTV_FRIEND;
				}
			}
			if (doc->value > tmp && doc->getCategory() != DOC_HOT) {
				doc->setCategory(DOC_SOCIAL);//打算社会化标志
                doc->value = doc->rank;
			}
			social_count++;
		}
	}

	if (social_count) {
		bcinfo->sort();
		log_len += sprintf(log + log_len, " social_count:%d", social_count);
	}

	log_len += sprintf(log + log_len, " time:%d}",
			calc_time_span(&start_time) / 1000);
	AC_WRITE_LOG(info, "\t%s", log);


	return 0;
}

//get debug info from extra
static void ac_get_debug(QsInfo *info, QsInfoBC *bcinfo) {
	int i;
	for (i = 0; i < (int) bcinfo->get_doc_num(); i++) {
		SrmDocInfo *doc = bcinfo->get_doc_at(i);

		if (doc == NULL || doc->extra == NULL)
			continue;

		if (bcinfo->bc[doc->bcidx] == NULL) {
			continue;
		}
        if (doc->docid == info->parameters.debugid) {
            AC_WRITE_LOG(info, " [bs_score:%llu, %u]", doc->docid, doc->value);
        }
        AcExtDataParser attr(doc);
        SLOG_DEBUG("%lu, %u, rank:%u, fwnum:%d, cmtnum:%d,"
                " reserved1:%d, uid:%llu, bcidx:%d, value=%u, qi=%d, cont_sign:%u, validfw:%u",
                doc->docid, doc->docid, doc->rank,attr.get_fwnum(),
                attr.get_cmtnum(), doc->reserved1, GetDomain(doc), doc->bcidx, doc->value, attr.get_qi(), attr.get_cont_sign(), attr.get_validfwnm());
        //旧的扩展数据解析方式
        if (doc->reserved1 == 0)
        {
            int flag = bcinfo->bc[doc->bcidx]->flag;

            int head_len = doc->attr_version ? sizeof(AttrExtra) : 0;
            if (flag & BS_FLAG_DATA) {
                doc->docattr = *(uint64_t*) ((char*) doc->extra + head_len);
                head_len += 8;
            } else {
                doc->docattr = 0;
            }

            if ((flag & BS_FLAG_DEBUG) == 0) {
                continue;
            }
            int dbg_len = *(uint16_t*) ((char*) doc->extra + head_len);
            if (dbg_len == 0 || dbg_len > doc->extra_len) {
                continue;
            }
            char *p = (char *) doc->extra + head_len;
            memmove(p, p + 2, dbg_len);
            p[dbg_len] = 0;
            doc->debug = p;
        }
        else
        {
            AcExtDataParser::get_debug_info(info, bcinfo, doc);
        }
	}
}

static int ac_set_flag(QsInfo *info, int *resort_flag, int *dup_flag) {
	*resort_flag = 1;
	*dup_flag = 1;

	if (strstr(info->params.query, "x_cluster:dup") == NULL
			&& !info->parameters.dup) {
		*resort_flag = 0;
		*dup_flag = 0;
	} else {
		if (memcmp(info->params.query, "uid:", 4) == 0 || strstr(
				info->params.query, " uid:") != NULL) {
			*resort_flag = 0;
		} else if (strstr(info->params.query, "digit_source==") != NULL) {
			*resort_flag = 0;
		} else if (strstr(info->params.query, "x_ids:") != NULL) {
			*resort_flag = 0;
		} else if ((info->params.nofilter & 0x1) || (info->params.nofilter
				& 0x2)) {
			*resort_flag = 0;
		}
	}

	return 0;
}

void ac_mask_tail(QsInfo* info) {
	QsInfoBC *bcinfo = &info->bcinfo;

	node_t *htable = (node_t*) (info->buffer + info->buf_pos);
	HASH_INIT(*htable, MAX_DUP_SIZE);

	int doc_ori_total, doc_total;
	doc_ori_total = doc_total = bcinfo->get_doc_num();

	int uid_black_count = 0;
	int source_black_count = 0;
	char *uid_black = bitdup_get(g_sort.bitmap, g_sort.uid_black_type);
	char *source_black = bitdup_get(g_sort.bitmap, g_sort.source_black_type);

	int i;
	int adjust_num = 0;
	for (i = 0; i < doc_total; i++) {
		SrmDocInfo *doc = bcinfo->get_doc_at(i);
		if (doc == NULL)
			continue;

		if (bcCaller.isBlack(info->params.squery, doc->bsidx, doc->docid)) {
			SLOG_DEBUG("docid %lu %u of query %s in black doc_query",
					doc->docid, doc->bsidx, info->params.squery);

			doc->value = ac_reset_docvalue(DOC_VALUE_ZERO);
            if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
				AC_WRITE_LOG(info, " %d,del %lu", __LINE__, doc->docid);
            }
			bcinfo->del_doc(doc);
			adjust_num++;
			continue;
		}
		uint64_t uid = GetDomain(doc);
		if (uid && BITDUP_EXIST(uid_black, uid,/* MAX_UID_SIZE*/0xffffffff)) {
			uid_black_count++;

			doc->value = ac_reset_docvalue(DOC_VALUE_ZERO);
            if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                AC_WRITE_LOG(info, " %d,del %lu", __LINE__, doc->docid);
            }
			bcinfo->del_doc(doc);
			adjust_num++;
			continue;
		}
        AcExtDataParser attr(doc);
		int source = attr.get_digit_source();
		if (source && BITDUP_EXIST(source_black, source, MAX_SOURCE_SIZE)) {
			source_black_count++;

			doc->value = ac_reset_docvalue(DOC_VALUE_ZERO);
            if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                AC_WRITE_LOG(info, " %d,del %lu", __LINE__, doc->docid);
            }
			bcinfo->del_doc(doc);
			adjust_num++;
			continue;
		}
	}

	bcinfo->refresh_weak();
	SLOG_DEBUG("doc_total:%d, the num of adjust results:%d", doc_total,
			adjust_num);
}

bool check_invalid_doc_mid(QsInfo *info, SrmDocInfo *doc, uint64_t uid, 
		__gnu_cxx::hash_map<uint64_t,SrmDocInfo*>& mid_map) {
	// mid去重
	if (mid_map.find(doc->docid) == mid_map.end()) {
		mid_map[doc->docid] = doc;
	} else {
		// 原始存放的doc
		SrmDocInfo* old_doc = mid_map[doc->docid];
		//保留value值大的 或者 rank值大的
		if (doc->value > old_doc->value || (doc->value == old_doc->value && doc->rank > old_doc->rank)) {
			mid_map[doc->docid] = doc;
			if (old_doc->docid == info->parameters.debugid) {
				AC_WRITE_LOG(info, " [%d,mid dup:del %lu]", __LINE__, old_doc->docid);
			}
			if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
				AC_WRITE_LOG(info, " [%d,mid dup:del %lu]", __LINE__, old_doc->docid);
			}
			info->bcinfo.del_doc(old_doc);
		} else {
			if (doc->docid == info->parameters.debugid) {
				AC_WRITE_LOG(info, " [%d,mid dup:del %lu]", __LINE__, doc->docid);
			}
			if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
				AC_WRITE_LOG(info, " [%d,mid dup:del %lu]", __LINE__, doc->docid);
			}
			return true;
		}
	}
    return false;
}

bool check_invalid_doc_mid(QsInfo *info, SrmDocInfo *doc, uint64_t uid, 
		const auto_reference<ACLongSet>& mask_mids, 
		__gnu_cxx::hash_map<uint64_t,SrmDocInfo*>& mid_map) {
	// for mids mask
    if (*mask_mids && mask_mids->find(doc->docid)) {
		DOC_WRITE_ACDEBUG(doc, "in mask_mids");
		if (doc->docid == info->parameters.debugid) {
			AC_WRITE_LOG(info, " {%lu in mask_mids} ", doc->docid);
		}
        return true;
    }
	// mid去重
	if (mid_map.find(doc->docid) == mid_map.end()) {
		mid_map[doc->docid] = doc;
	} else {
		// 原始存放的doc
		SrmDocInfo* old_doc = mid_map[doc->docid];
		//保留value值大的 或者 rank值大的
		if (doc->value > old_doc->value || (doc->value == old_doc->value && doc->rank > old_doc->rank)) {
			mid_map[doc->docid] = doc;
			if (old_doc->docid == info->parameters.debugid) {
				AC_WRITE_LOG(info, " [%d,mid dup:del %lu]", __LINE__, old_doc->docid);
			}
			if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
				AC_WRITE_LOG(info, " [%d,mid dup:del %lu]", __LINE__, old_doc->docid);
			}
			info->bcinfo.del_doc(old_doc);
		} else {
			if (doc->docid == info->parameters.debugid) {
				AC_WRITE_LOG(info, " [%d,mid dup:del %lu]", __LINE__, doc->docid);
			}
			if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
				AC_WRITE_LOG(info, " [%d,mid dup:del %lu]", __LINE__, doc->docid);
			}
			return true;
		}
	}
    return false;
}

bool check_manual_black_uids(QsInfo *info, SrmDocInfo *doc, uint64_t uid,auto_reference<ACLongSet> & uid_dict) {
    if(uid && *uid_dict && uid_dict->find(uid)) {
		if (doc->docid == info->parameters.debugid) {
			AC_WRITE_LOG(info, " [%d,uids_black_manual:del %lu]", __LINE__, doc->docid);
		}
		if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
			AC_WRITE_LOG(info, " [%d,uids_black_manual:del %lu]", __LINE__, doc->docid);
		}
        return true;
    }
	return false;
}

static int  check_doc_is_black(QsInfo *info, SrmDocInfo *doc,
            uint64_t uid,
            const AcExtDataParser& attr,
            const auto_reference<ACLongSet>& uid_dict,
            const auto_reference<DASearchDictSet>& black_uids,
            const auto_reference<DASearchDictSet>& plat_black) {
	if (info == NULL || doc == NULL) {
		return 0;
	}
	if (bcCaller.isBlack(info->params.squery, doc->bsidx, doc->docid)) { 
		SLOG_DEBUG("docid %lu %u of query %s in black doc_query",
				doc->docid, doc->bsidx, info->params.squery);
		if (doc->docid == info->parameters.debugid) {
			AC_WRITE_LOG(info, " [%d,isBlack:del %lu]", __LINE__, doc->docid);
		}
		if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
			AC_WRITE_LOG(info, " [%d,isBlack:del %lu]", __LINE__, doc->docid);
		}
        return 1;
    } 
    if (uid && *uid_dict && uid_dict->find(uid)) {
		if (doc->docid == info->parameters.debugid) {
			AC_WRITE_LOG(info, " [%d,uids_black_manual:del %lu]", __LINE__, doc->docid);
		}
		if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
			AC_WRITE_LOG(info, " [%d,uids_black_manual:del %lu]", __LINE__, doc->docid);
		}
        return 2;
    }
    if (uid && *black_uids) {
        int ret = black_uids->Search(uid);
        if (ret != -1) {
			if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
				DOC_WRITE_ACDEBUG(doc, "delete:in black_uids:%d", ret);
			}
			if (doc->docid == info->parameters.debugid) {
				AC_WRITE_LOG(info, " [del,in black_uids,docid:%llu,%d]", doc->docid, ret);
			}
            return 4;
        }
    }
    if (uid && *plat_black) {
        int ret = plat_black->Search(uid);
        if (ret != -1) {
			if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
				DOC_WRITE_ACDEBUG(doc, "delete:in plat_black:%d", ret);
			}
			if (doc->docid == info->parameters.debugid) {
				AC_WRITE_LOG(info, " [del,in plat_black,docid:%llu,%d]", doc->docid, ret);
			}
            return 6;
        }
    }
	return 0;
}

bool check_invalid_doc(QsInfo *info, SrmDocInfo *doc, 
		uint64_t uid, const AcExtDataParser& attr, 
		bool check_sandbox, time_t delay_time, time_t delay_time_qi20, 
		const auto_reference<ACLongSet>& manual_black_uids, 
		const auto_reference<DASearchDictSet>& black_uids,
        const auto_reference<DASearchDictSet>& plat_black,
        const auto_reference<DASearchDict>& suspect_uids,
        uint64_t source, const auto_reference<SourceIDDict>& black_dict) {
    // for sandbox
    if (check_sandbox && ac_sandbox_doc(info, doc, uid, delay_time,suspect_uids)) {
		DOC_WRITE_ACDEBUG(doc, "in sandbox or suspect_uids");
		if (doc->docid == info->parameters.debugid) {
			AC_WRITE_LOG(info, " {%lu in sandbox or suspect_uids} ", doc->docid);
		}
        return true;
    }
	if (black_dict->find(source)) {
		if (doc->docid == info->parameters.debugid) {
			AC_WRITE_LOG(info, " {%lu in blacksource} ", doc->docid);
		}
        return true;
    }
    // check qi 20
    if (doc->rank >= delay_time_qi20) {
        if (attr.get_qi() & (0x1 << 20)) {
			DOC_WRITE_ACDEBUG(doc, "qi (20b)");
			if (doc->docid == info->parameters.debugid) {
				AC_WRITE_LOG(info, " {%lu in qi(20b)} ", doc->docid);
			}
            return true;
        }
    }   
    // for black
    if (check_doc_is_black(info, doc, uid, attr, manual_black_uids, black_uids, plat_black))
        return true;
    return false;
}
void ac_mask_del(QsInfo *info) {
	if (info == NULL)  return;
	QsInfoBC *bcinfo = &info->bcinfo;

	node_t *htable = (node_t*) (info->buffer + info->buf_pos);
	HASH_INIT(*htable, MAX_DUP_SIZE);

	char log[TMP_LOG_LEN];
	int log_len = sprintf(log, "[ac_sort:");

	int doc_ori_total, doc_total;
	doc_ori_total = doc_total = bcinfo->get_doc_num();

	/*	
	if (bcinfo->flag && bcinfo->get_doc_num() > 1200) {//
		doc_ori_total = doc_total = bcinfo->get_doc_num() - LEFT_COUNT;
		log_len += sprintf(log + log_len, " hold");
	} else {
		doc_ori_total = doc_total = bcinfo->get_doc_num();
		log_len += sprintf(log + log_len, " unhold");
	}
	*/

	int uid_black_count = 0;
	int source_black_count = 0;
	char *uid_black = bitdup_get(g_sort.bitmap, g_sort.uid_black_type);
	//char *source_black = bitdup_get(g_sort.bitmap, g_sort.source_black_type);
    bool check_sandbox = true;
	
//    std::string key;
//    if (info->params.key) {
//        key = info->params.key;
//    } else {
//        key = info->params.query;
//    }
//	auto_reference<ACStringSet> sandbox_query("sandbox_query");
//	if (*sandbox_query == NULL) {
//        check_sandbox = false;
//	} else {
//		if (!(info->intention & INTENTION_STAR) && !sandbox_query->test(key)) {
//			check_sandbox = false;
//		}
//	}
    int bc_check_black = int_from_conf(BLACK_LIST_CHECK_SWITCH, 1);

    time_t time_now = time(NULL);
	time_t delay_time = time_now - int_from_conf(SUSPECT_DELAY_TIME, 172800);
	time_t delay_time_qi20 = time_now - int_from_conf(QI20_DELAY_TIME, 604800);
    auto_reference<SourceIDDict> black_dict("source_id.dict");
	auto_reference<ACLongSet> mask_mids("mask_mids");
	auto_reference<ACLongSet> manual_black_uids("uids_black_manual.txt");
	auto_reference<DASearchDictSet> black_uids("uids_black");
	//auto_reference<DASearchDictSet> dup_black_uids("dup_uids_black");
	auto_reference<DASearchDictSet> plat_black("plat_black");
	auto_reference<DASearchDict> suspect_uids("suspect_uids");
	int i = 0;
	int deleted_num = 0;
	__gnu_cxx::hash_map<uint64_t,SrmDocInfo*> mid_map;
	int white_protect = int_from_conf(WHITE_PROTECT, 0);
	for (i = 0; i < doc_total; i++) {
		SrmDocInfo *doc = bcinfo->get_doc_at(i);
		if (doc == NULL)
			continue;	
		uint64_t uid = GetDomain(doc);
		/* mid去重和人工干预黑微博检查 */
		if(check_invalid_doc_mid(info, doc, uid, mask_mids, mid_map)) {
			bcinfo->del_doc(doc);
			deleted_num++;
			continue;
		}
        AcExtDataParser attr(doc);
		/* uid白名单微博豁免 */
		if (PassByWhite(attr) &&  white_protect) {
            if (doc->docid == info->parameters.debugid) {
                AC_WRITE_LOG(info, " [%d, uid or weibo white:%lu, uid:%lu, white_weibo:%d]", __LINE__, doc->docid, uid, attr.get_white_weibo());
            }
            if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                AC_WRITE_LOG(info, " [%d, uid or weibo white:%lu, uid:%lu, white_weibo:%d]", __LINE__, doc->docid, uid, attr.get_white_weibo());
			}
			continue;
		}
		/* 人工黑uid名单检查 */
		if(check_manual_black_uids(info, doc, uid, manual_black_uids)) {
			bcinfo->del_doc(doc);
			deleted_num++;
			continue;
		}
		/* 把这逻辑挪到csort里面再删除掉 */
		if (doc->rank >= delay_time_qi20) {
			if (attr.get_qi() & (0x1 << 20)) {
				DOC_WRITE_ACDEBUG(doc, "qi (20b)");
				if (doc->docid == info->parameters.debugid) {
					AC_WRITE_LOG(info, " {%lu in qi(20b)} ", doc->docid);
				}
				bcinfo->del_doc(doc);
				deleted_num++;
				continue;
			}
		}   
		if (bc_check_black == 0 && check_invalid_doc(info, doc, uid, attr, check_sandbox, delay_time, delay_time_qi20, 
					manual_black_uids, black_uids, plat_black, suspect_uids, attr.get_digit_source(), black_dict)) {
			bcinfo->del_doc(doc);
			deleted_num++;
			continue;
		}
		if (uid && BITDUP_EXIST(uid_black, uid,/* MAX_UID_SIZE*/0xffffffff)) {
			uid_black_count++;
            if (doc->docid == info->parameters.debugid) {
                AC_WRITE_LOG(info, " [%d, uid_black:del %lu, uid:%llu]", __LINE__, doc->docid, uid);
            }
            if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                AC_WRITE_LOG(info, " [%d, uid_black:del %lu, uid:%llu]", __LINE__, doc->docid, uid);
            }
			bcinfo->del_doc(doc);
			deleted_num++;
			continue;
		}
//        int source = attr.get_digit_source();
//		if (source && BITDUP_EXIST(source_black, source, MAX_SOURCE_SIZE)) {
//			source_black_count++;
//			if (doc->docid == info->parameters.debugid) {
//				AC_WRITE_LOG(info, " [%d,source_black:del %lu]", __LINE__, doc->docid);
//			}
//			if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
//				AC_WRITE_LOG(info, " [%d,source_black:del %lu]", __LINE__, doc->docid);
//            }
//			bcinfo->del_doc(doc);
//			deleted_num++;
//			continue;
//		}
    }
	int total_delete_num = bcinfo->refresh();
	doc_total -= total_delete_num;

	SLOG_DEBUG("doc_total:%d, deleted_num:%d total_delete_num:%d",
			doc_total, deleted_num, total_delete_num);

	if (uid_black_count) {
		if (log_len < TMP_LOG_LEN)
			log_len += sprintf(log + log_len, " uid:%d", uid_black_count);
	}
	if (source_black_count) {
		if (log_len < TMP_LOG_LEN)
			log_len += sprintf(log + log_len, " source:%d", source_black_count);
	}
}


void ac_adjust_hot_weibo(QsInfo* info, uint64_t* docid_list)
{
    std::vector<SrmDocInfo*>& hot_weibo = info->bcinfo.hot_weibo;
    for (int i = 0; i < HOT_ADJUST_NUM; i++)
    {
        if (docid_list[i] <= 1)
        {
            continue;
        }
        uint64_t docid = docid_list[i];
        SrmDocInfo* newdoc = NULL;
        for (int j = 0; j < hot_weibo.size(); j++)
        {
            if (hot_weibo[j]->docid == docid)
            {
                newdoc = hot_weibo[j];
                hot_weibo.erase(hot_weibo.begin() + j);
                SLOG_DEBUG("adjust result mid:%lu found in hot_weibo order:%d", docid, j);
                break;
            }
        }
        //没有找到，新建一个
        if (newdoc == NULL)
        {
            SLOG_DEBUG("adjust result mid:%lu not found in hot_weibo", docid);
			newdoc = info->thread->popDoc();
			if (newdoc != NULL) {
				newdoc->reset();
				newdoc->docid = docid;
				newdoc->value = VALUE_EXTRA + VALUE_EXTRA - i;
			}
        }
        newdoc->setCategory(DOC_ADJUST);
        hot_weibo.insert(hot_weibo.begin() + i, newdoc);
    }
}

int ac_user_recmd(QsInfo *info) {
	QsInfoBC *bcinfo = &info->bcinfo;

	char *query = (char *) info->parameters.getKey().c_str();
	if (query == NULL)
		return -1;

	Query2ClassDict *dict = Resource::Reference<Query2ClassDict>(
			"query_class.dict");
	if (dict == NULL)
		return -2;

	uint64_t docid_list[HOT_ADJUST_NUM + 1];

	int has0, has1, hasn;
	has0 = has1 = 0, hasn = 0;

	int i;
	for (i = 0; i < HOT_ADJUST_NUM; i++) {
		docid_list[i] = dict->find(query, i);

		if (docid_list[i] == 0)
			has0 = 1;
		else if (docid_list[i] == 1)
			has1 = 1;
		else
			hasn = 1;
	}

	if (dict != NULL) {
		dict->UnReference();
	}
    SLOG_DEBUG("has0:%d, has1:%d, hasn:%d", has0, has1, hasn);

	if (has1 && !has0) {//屏蔽+只出推荐需求
		//先关闭所有三条热门
        info->bcinfo.hot_weibo.clear();
        //插入人工干预的微博
        ac_adjust_hot_weibo(info, docid_list); 
	} else if (!has1 && hasn) {//推荐需求
        ac_adjust_hot_weibo(info, docid_list);
	} 


	return 0;
}


void gettimelevel(QsInfo *info, int* timel, int* qtype){
	if (info->json_intention == NULL || info->json_intention->text == NULL)
		return ;

	char *intent = info->json_intention->text;
	int intentlen = info->json_intention->curLen;

	char *temp = strstr(intent, "attribute_info");
	if (temp != NULL) {
		char *timeliness = strstr(temp, "timeliness");
		if (timeliness != NULL) {
			while (!isdigit(*timeliness) && timeliness - intent < intentlen)
				timeliness++;

			if (timeliness - intent < intentlen && isdigit(*timeliness))
				*timel = atoi(timeliness);
		}
	}

	temp = strstr(intent, "text_info");
	if (temp != NULL) {
		char *text_info = strstr(temp, "type");
		if (text_info != NULL) {
			while (!isdigit(*text_info) && text_info - intent < intentlen)
				text_info++;

			if (text_info - intent < intentlen && isdigit(*text_info))
				*qtype = atoi(text_info);
		}
	}
}

static int manu_set_data(QsInfo *info, SrmDocInfo *doc) {
    doc->reset();
    doc->m_category = DOC_CATEGORY_ADJUST;
    return 0;
}

int ac_unset_default(QsInfo *info) {
	if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
		AC_WRITE_LOG(info, " [ac_unset_default::num:%d]", info->bcinfo.get_doc_num());
	}
	char *query = (char *) info->parameters.getKey().c_str();
	if (query == NULL) {
		return 0;
    }
	Query2ClassDict *dict = Resource::Reference<Query2ClassDict>("query_class.dict");
	if (dict == NULL) {
		if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
			AC_WRITE_LOG(info, " [AcInterventionAdd dict==NULL]");
		}
		return 0;
	}
	int num = info->bcinfo.get_doc_num();
	uint64_t dc_docid[num];
	uint64_t result_docid[num];
    bool manu_result[num];
    for (int i = 0; i < 20 && i < num; ++i) {
        manu_result[i] = false;
    }
    for (int i = 0; i < 20 && i < num; ++i) {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
        result_docid[i] = 0;
		if (doc == NULL) {
            dc_docid[i] = 0;
			continue;
		}
        dc_docid[i] = doc->docid;
        if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
            AC_WRITE_LOG(info, " [dc_docid:%llu, %d]", dc_docid[i], i);
        }
    }
    int manu_hot_max = int_from_conf(MANU_HOT_MAX, 3);
	uint64_t docid_list[manu_hot_max + 1];
	for (int i = 0; i < manu_hot_max; i++) {
		docid_list[i] = dict->find(query, i);
    }
	if (dict != NULL) {
		dict->UnReference();
	}
    bool refresh = 0;
	for (int i = 0; i < num && i < manu_hot_max; i++) {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (doc == NULL) {
			continue;
		}
        if (docid_list[i]) {
            result_docid[i] = docid_list[i];
            manu_result[i] = true;
            if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                AC_WRITE_LOG(info, " [manu_docid:%llu, %d]", result_docid[i], i);
            }
            refresh = 1;
        }
	}
    if (refresh) {
        for (int i = 0, j = 0; i < 20 && i < num && j < 20 && j < num; ++i) {
            if (result_docid[i]) {
                continue;
            }
            result_docid[i] = dc_docid[j++];
        }
        for (int i = 0; i < num && i < 20; i++) {
            SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
            if (doc == NULL || result_docid[i] == 0) {
                continue;
            }
            if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                AC_WRITE_LOG(info, " [result:%llu, %d]", result_docid[i], i);
            }
            if (manu_result[i]) {
              manu_set_data(info, doc);
            }
            doc->docid = result_docid[i];
        }
    }
	return 0;
}

int ac_abs_dup(QsInfo *info) {
    AC_WRITE_LOG(info, " [ac_abs_dup::num:%d] ", info->bcinfo.get_doc_num());
    ac_unset_default(info);
	return 0;
}

int IsHotDoc(SrmDocInfo *doc, int time_level) {
	if (doc == NULL || doc->extra == NULL) // || doc->value < doc->rank)
		return 0;
    int curtime = time(NULL);
    if (GlobalConfSingleton::GetInstance().getAcConf().global.select_hot_weibo_from_history == 0)
    {
        //time_level=3, 只出1周内结果
        if (time_level >= 3 && (curtime - doc->rank > 7 * 24 * 3600))
        {
            return 0;
        }
        //否则只出半年内结果
        else if (curtime - doc->rank > 180 * 24 * 3600)
        {
            return 0;
        }
    }
    AcExtDataParser attr(doc);

	//( 2==verified_type || 5==verified_type || 6==verified_type)
	int vfield = attr.get_verified();
	int vtype = attr.get_verified_type();
	int fwnum = attr.get_fwnum();

	int isHot = 0;
	//原创或黄v用户
	if (!(attr.get_digit_attr() & (0x1 << 2))) {
		//黄v用户
		if (vfield && (vtype == 0 || vtype == 1) && fwnum >= 4) {
			isHot = 1;
		} else if (vfield && !(vtype == 2 || vtype == 5 || vtype == 6) && fwnum
				>= 16) {
			isHot = 1;
		} else if (fwnum >= 9) {
			isHot = 1;
		}
	}
	return isHot;
}

int IsExpandDoc(SrmDocInfo *doc) {
	if (NULL == doc) {
		return 0;
	}

	uint32_t curtime = time(NULL);
	uint32_t wtime = ((Rank*) (&doc->rank))->digit_time;

	if (((doc->docattr >> 31) & 0x1) && doc->value > VALUE_EXTRA && curtime
			- wtime < 3 * 86400) {
		return 1;
	}

	return 0;
}

static int SimhashVType(QsInfo *info, SrmDocInfo *doc) {
    AcExtDataParser attr(doc);
    uint64_t vtype = attr.get_verified_type();
    if (attr.get_verified() && vtype >= 1 && vtype <= 7) {
        return SIMHASH_VIP_TYPE_BLUEV;
    }
    return SIMHASH_VIP_TYPE_OTHER;
}

int IsNeedFilter(QsInfo *info, int curdoc, std::set<uint64_t> *dup_set, std::set<uint64_t>* cont_set,
		int dup_flag, int resort_flag, int *isdupset, std::set<uint64_t> *uid_set, int* isdup_cont) {
	int need_filter = 0;

	if (resort_flag) {
		Result *results = (Result*) (info->buffer + info->buf_pos);
		if (results + curdoc != NULL && results[curdoc].delflag && need_filter
				== 0)
			need_filter = 1;
	}
	SrmDocInfo *doc = info->bcinfo.get_doc_at(curdoc);
	if (doc == NULL) {
		return need_filter;
	}

	if (dup_flag) {
        AcExtDataParser attr(doc);
        //dup签名和内容签名的去重移到simhash签名之前，或许可以改善一些性能
        //根据dup签名去重，主要解决完全一样的结果
        uint64_t dup = attr.get_dup();
		if (dup && need_filter == 0) {
			if (dup_set->find(dup) != dup_set->end()) {
				need_filter = 2;
                if (doc->docid == info->parameters.debugid) {
                    AC_WRITE_LOG(info, " [%d,dup_set:already:%llu, docid:%lld]", __LINE__, dup, doc->docid);
                }
                if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                    AC_WRITE_LOG(info, " [%d,dup_set:already:%llu, docid:%lld]", __LINE__, dup, doc->docid);
                }
                return need_filter;
			} else {
                dup_set->insert(dup);
                if (doc->docid == info->parameters.debugid) {
                    AC_WRITE_LOG(info, " [%d,dup_set:insert:%llu, docid:%lld]", __LINE__, dup, doc->docid);
                }
                if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                    AC_WRITE_LOG(info, " [%d,dup_set:insert:%llu, docid:%lld]", __LINE__, dup, doc->docid);
                }
                *isdupset = 1;
				//}
			}
		}
        uint64_t cont_sign = attr.get_cont_sign();
        if (cont_set != NULL
                && cont_sign != 0 
                && need_filter == 0)
        {
            if (cont_set->find(cont_sign) != cont_set->end())
            {
                need_filter = 3;
                if (doc->docid == info->parameters.debugid) {
                    AC_WRITE_LOG(info, " [%d,cont_set:already:%llu, docid:%lld]", __LINE__, cont_sign, doc->docid);
                }
                if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                    AC_WRITE_LOG(info, " [%d,cont_set:already:%llu, docid:%lld]", __LINE__, cont_sign, doc->docid);
                }
                return need_filter;
            }
            else
            {
                cont_set->insert(cont_sign);
                if (doc->docid == info->parameters.debugid) {
                    AC_WRITE_LOG(info, " [%d,cont_set:insert:%llu, docid:%lld]", __LINE__, cont_sign, doc->docid);
                }
                if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                    AC_WRITE_LOG(info, " [%d,cont_set:insert:%llu, docid:%lld]", __LINE__, cont_sign, doc->docid);
                }
                *isdup_cont = 1;
            }
        }
		if (need_filter == 0 && attr.get_dup_cont() != 0 && curdoc < 10 * 20) {
            int use_simhash2 = int_from_conf(USE_SIMHASH2, 0);
			uint64_t dup_cont = attr.get_dup_cont();
			uint64_t simhash2 = attr.get_simhash2();
			int j, k;
            int vip_type = SimhashVType(info, doc);
			for (j = 0, k = 0; j < curdoc && k < 20; j++) {
				SrmDocInfo *doc_j = info->bcinfo.get_doc_at(j);
				if (doc_j == NULL || doc_j->extra == NULL)
					continue;

				if (doc_j->value < 1)
					continue;
				k++;
                /*
                int vip_type_j = SimhashVType(info, doc_j);
                if (vip_type != vip_type_j) {
					continue;
                }
                */
                AcExtDataParser attr_j(doc_j);
				uint64_t attr_xor = dup_cont ^ attr_j.get_dup_cont();

				int diff = 0;
				while (attr_xor > 0) {
					attr_xor &= (attr_xor - 1);
					diff++;
				}
                if (diff <= 4) {
                    need_filter = 4;
                    if (info->parameters.debug == 2) {
                        AC_WRITE_LOG(info, " %llu simhash_dup: dup_docid:%llu", doc->docid, doc_j->docid);
                    }
                    DOC_WRITE_ACDEBUG(doc_j, " s_dup:%llu, diff:%d", doc->docid, diff);
                    return need_filter;
                }
                /*
                 * for second simhash
				uint64_t attr_xor_2 = simhash2 ^ attr_j.get_simhash2();
				int diff_2 = 0;
				while (attr_xor_2 > 0) {
					attr_xor_2 &= (attr_xor_2 - 1);
					diff_2++;
				}
                if (!use_simhash2) {
                    diff_2 = 0;
                }

                if (info->parameters.debug == 2) {
                    AC_WRITE_LOG(info, " %llu simhash:%llu:%llu,diff:%d", doc->docid, dup_cont, attr_j.get_dup_cont(), diff);
                }
				if (SIMHASH_VIP_TYPE_BLUEV == vip_type) {
                    if (diff <= 4 && diff_2 <= 4) {
                        if (info->parameters.debug == 2) {
                            AC_WRITE_LOG(info, " %llu simhash_dup: BV dup_docid:%llu", doc->docid, doc_j->docid);
                        }
                        DOC_WRITE_ACDEBUG(doc_j, " s_dup_v:%llu, diff:%d", doc->docid, diff);
                        need_filter = 4;
                        return need_filter;
                    }
                } else {
                    if (diff <= 4 && diff_2 <= 4) {
                        need_filter = 4;
                        if (info->parameters.debug == 2) {
                            AC_WRITE_LOG(info, " %llu simhash_dup: dup_docid:%llu", doc->docid, doc_j->docid);
                        }
                        DOC_WRITE_ACDEBUG(doc_j, " s_dup:%llu, diff:%d", doc->docid, diff);
                        return need_filter;
                    }
				}
                */
			}
		}
        //uid去重
        uint64_t domain = GetDomain(doc);
        if (domain && need_filter == 0 && curdoc < 10*20) {
            int uid_max = int_from_conf(UID_DUP_IN_COUNT, 5);
            if (uid_set->size() >= uid_max) {
                uid_set->clear();
            }
            if (uid_set->find(domain) != uid_set->end()) {
                need_filter = 5;
                return need_filter;
            } else {
                uid_set->insert(domain);
            }
        }

	}

	return need_filter;
}

/**
 * @brief 判断微博是否满足精选微博的标准
 *
 * @param [in] doc   : SrmDocInfo*
 * @param [in] classifier   : int
 * @param [in] qi   : uint64_t
 * @param [in] fwnum   : uint64_t
 * @param [in] cmtnum   : uint64_t
 * @param [in] dict   : SourceIDDict*
 * @return  bool 
 * @author qiupengfei
 * @date 2014/05/22 09:09:54
**/
bool is_good_doc(QsInfo* info, SrmDocInfo* doc, int classifier, uint64_t qi, uint64_t fwnum, uint64_t cmtnum, SourceIDDict* dict)
{
    int curtime = time(NULL);
    AcExtDataParser attr(doc);
    uint64_t valid_fw = attr.get_validfwnm();
    uint64_t unvalid_fw = attr.get_unvifwnm();
    //有效转发比例太低，作弊可能性大
    /*
    if (valid_fw > 1)
    {
        int valid_fw_num = fwnum * valid_fw / 1000.0;
        if (valid_fw < 100 && valid_fw_num < 50) {
            if (doc->docid == info->parameters.debugid) {
                AC_WRITE_LOG(info, " [not_good,%d,%lu,%d,%d]", __LINE__, doc->docid, valid_fw, valid_fw_num);
            }
            if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                AC_WRITE_LOG(info, " [not_good,%d,%lu,%d,%d]", __LINE__, doc->docid, valid_fw, valid_fw_num);
            }
            return false;
        }
    }
    */
    if (unvalid_fw > 1 && unvalid_fw > 700)
    {
        if (doc->docid == info->parameters.debugid) {
            AC_WRITE_LOG(info, " [not_good,%d,%lu,%d]", __LINE__, doc->docid, unvalid_fw);
        }
        if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
            AC_WRITE_LOG(info, " [not_good,%d,%lu,%d]", __LINE__, doc->docid, unvalid_fw);
        }
         return false;
        return false;
    }
	uint64_t likenum = attr.get_likenum();
	int white_black_like_switch = int_from_conf(WHITE_BLACK_LIKE_SWITCH, 0);
	if (white_black_like_switch == 0)
	{
		if (likenum) {
			uint64_t white_likenum = attr.get_whitelikenum() * 1000 / likenum;
			uint64_t black_likenum = attr.get_blacklikenum() * 1000 / likenum;
			/*
			   if (white_likenum != 0)
			   {
			   int valid_fw_num = likenum * white_likenum / 1000.0;
			   if (white_likenum < 100 && valid_fw_num < 50) {
			   if (doc->docid == info->parameters.debugid) {
			   AC_WRITE_LOG(info, " [not_good,%d,%lu,%d,%d]", __LINE__, doc->docid,white_likenum, valid_fw_num);
			   }
			   if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
			   AC_WRITE_LOG(info, " [not_good,%d,%lu,%d,%d]", __LINE__, doc->docid,white_likenum, valid_fw_num);
			   }
			   return false;
			   }
			   }
			 */
			if (black_likenum != 0 && black_likenum > 700)
			{
				if (doc->docid == info->parameters.debugid) {
					AC_WRITE_LOG(info, " [not g_good,%d,%lu,%d]", __LINE__, doc->docid, black_likenum);
				}
				if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
					AC_WRITE_LOG(info, " [not g_good,%d,%lu,%d]", __LINE__, doc->docid, black_likenum);
				}
				return false;
			}
		}
	}
	else
	{
		uint64_t white_likenum = attr.get_whitelikenum();
		uint64_t black_likenum = attr.get_blacklikenum();
		/*
		   if (white_likenum != 0)
		   {
		   int valid_fw_num = likenum * white_likenum / 1000.0;
		   if (white_likenum < 100 && valid_fw_num < 50) {
		   if (doc->docid == info->parameters.debugid) {
		   AC_WRITE_LOG(info, " [not_good,%d,%lu,%d,%d]", __LINE__, doc->docid,white_likenum, valid_fw_num);
		   }
		   if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
		   AC_WRITE_LOG(info, " [not_good,%d,%lu,%d,%d]", __LINE__, doc->docid,white_likenum, valid_fw_num);
		   }
		   return false;
		   }
		   }
		 */
		if (black_likenum != 0 && black_likenum > 700)
		{
			if (doc->docid == info->parameters.debugid) {
				AC_WRITE_LOG(info, " [not g_good,%d,%lu,%d]", __LINE__, doc->docid, black_likenum);
			}
			if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
				AC_WRITE_LOG(info, " [not g_good,%d,%lu,%d]", __LINE__, doc->docid, black_likenum);
			}
			return false;
		}
	}
	if (fwnum < 4 ||
			(fwnum < 9 && doc->rank < curtime - 3600*24*365))
	{
		if (doc->docid == info->parameters.debugid) {
			AC_WRITE_LOG(info, " [not_good,%d,%llu,%d,%d]", __LINE__, doc->docid, fwnum, likenum);
		}
		if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
			AC_WRITE_LOG(info, " [not_good,%d,%llu,%d,%d]", __LINE__, doc->docid, fwnum, likenum);
		}
		return false;
    }
    bool is_bad = is_bad_doc(info, doc, classifier, qi, fwnum,cmtnum, dict);
    if (is_bad)
    {
        return false;
    }
    //qi 14位为1，表示挖坟结果
    if (GET_DUP_FLAG_OF_INDEX(qi) && fwnum < 9)
    {
        if (doc->docid == info->parameters.debugid) {
            AC_WRITE_LOG(info, " [not_good,%d,%llu,%d]", __LINE__, doc->docid, qi);
        }
        if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
            AC_WRITE_LOG(info, " [not_good,%d,%llu,%d]", __LINE__, doc->docid, qi);
        }
        return false;
    }
    return true; 
}

void ac_resort_default(QsInfo *info, int dup_flag, int resort_flag, int classifier) {
}


bool is_bad_doc (QsInfo* info, SrmDocInfo *doc, int classifier,uint64_t qi, int fwnum, int cmtnum, 
                        SourceIDDict* source_dict){
    bool is_bad = false;
    //判断低质结果时不考虑第63位，term命中多次被认为是作弊
	uint64_t docattr_bak = doc->docattr;
    doc->docattr &= 0x7fffffffffffffff;
    /*
	if(classifier>1){
		if ((doc->docattr > (0x1ll << 50) && (((doc->docattr & (0x1ll << 58)) == 0 && (doc->docattr
							& (0x1ll << 62)) == 0)))
					|| (((qi & 63) < 48 || (qi&128) ) && fwnum == 0 && ((doc->docattr&0x100000000ll)==0))) {
            if (doc->docid == info->parameters.debugid) {
                AC_WRITE_LOG(info, " [bad:%d,%llu,%lld,qi:%d,fwnum:%d]", __LINE__, doc->docid, doc->docattr, qi, fwnum);
            }
            if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                AC_WRITE_LOG(info, " [bad:%d,%llu,%lld,qi:%d,fwnum:%d]", __LINE__, doc->docid, doc->docattr, qi, fwnum);
            }
            is_bad = true;
		}
	}
	else if(classifier==1){
		if ((doc->docattr > (0x1ll << 50) && (((doc->docattr & (0x1ll << 58)) == 0 && (doc->docattr
							& (0x1ll << 62)) == 0)))
					|| (((qi & 63) < 48 ) && fwnum == 0 && ((doc->docattr&0x100000000ll)==0))) {
            if (doc->docid == info->parameters.debugid) {
                AC_WRITE_LOG(info, " [bad:%d,%llu,%lld,qi:%d,fwnum:%d]", __LINE__, doc->docid, doc->docattr, qi, fwnum);
            }
            if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                AC_WRITE_LOG(info, " [bad:%d,%llu,%lld,qi:%d,fwnum:%d]", __LINE__, doc->docid, doc->docattr, qi, fwnum);
            }
            is_bad = true;
		}
	}
	else{
	}
    */
    if ((doc->docattr > (0x1ll << 50) && (((doc->docattr & (0x1ll << 58)) == 0 && (doc->docattr
                        & (0x1ll << 62)) == 0)))
                || (((qi & 63) < 32 ) && fwnum == 0 && ((doc->docattr&0x100000000ll)==0))) {
        if (doc->docid == info->parameters.debugid) {
            AC_WRITE_LOG(info, " [bad:%d,%llu,%lld,qi:%d,fwnum:%d]", __LINE__, doc->docid, doc->docattr, qi, fwnum);
        }
        if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
            AC_WRITE_LOG(info, " [bad:%d,%llu,%lld,qi:%d,fwnum:%d]", __LINE__, doc->docid, doc->docattr, qi, fwnum);
        }
        is_bad = true;
    }
    //恢复第63位
    doc->docattr = docattr_bak;
    
    AcExtDataParser attr(doc);
    uint64_t vtype = attr.get_verified_type();
    uint64_t source = attr.get_digit_source();
    //若是非白名单结果，且内容是个挖坟结果，标记为低质
    /*
    if (!is_bad && GET_DUP_FLAG_OF_INDEX(qi) 
            && GET_NOT_WHITE_FLAG(qi)
            && fwnum == 0)
    {
        if (doc->docid == info->parameters.debugid) {
            AC_WRITE_LOG(info, " [bad:%d,%llu, qi:%d,fwnum:%d]", __LINE__, doc->docid, qi, fwnum);
        }
        if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
            AC_WRITE_LOG(info, " [bad:%d,%llu, qi:%d,fwnum:%d]", __LINE__, doc->docid, qi, fwnum);
        }
        is_bad = true;
    }
    //打压某些蓝v用户
    if (!is_bad 
            && attr.get_verified()
            && vtype != 1 && vtype != 0 && vtype != 3
            && fwnum < 4)
    {
        if (doc->docid == info->parameters.debugid) {
            AC_WRITE_LOG(info, " [bad:%d,%llu,%d,fwnum:%d]", __LINE__, doc->docid, vtype, fwnum);
        }
        if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
            AC_WRITE_LOG(info, " [bad:%d,%llu,%d,fwnum:%d]", __LINE__, doc->docid, vtype, fwnum);
        }
        is_bad = true;
    }
    //打压某些来源的微博
    if (!is_bad && source_dict != NULL)
    {
        int mask_sid = 0;
        mask_sid = source_dict->find(source); 
        //如果在词典中，且转发数小于4，且（是个挖坟结果，或不在白名单）
        if (mask_sid && fwnum < 4
                && (GET_DUP_FLAG_OF_INDEX(qi) || GET_NOT_WHITE_FLAG(qi)))
        {
            if (doc->docid == info->parameters.debugid) {
                AC_WRITE_LOG(info, " [bad:%d,%llu,%d,fwnum:%d,qi:%d]", __LINE__, doc->docid, source, fwnum, qi);
            }
            if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
                AC_WRITE_LOG(info, " [bad:%d,%llu,%d,fwnum:%d,qi:%d]", __LINE__, doc->docid, source, fwnum, qi);
            }
            is_bad = true;
        }
    }
    */
    //qi第0位表示广告，第1位表示垃圾活动微博
    if ( (qi & 0x3) || (qi & 0x12000) )
    {
        if (doc->docid == info->parameters.debugid) {
            AC_WRITE_LOG(info, " [bad:%d,%llu, qi:%d]", __LINE__, doc->docid, qi);
        }
        if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
            AC_WRITE_LOG(info, " [bad:%d,%llu, qi:%d]", __LINE__, doc->docid, qi);
        }
        is_bad = true;
    }
    return is_bad;
}

/**
 * @brief 在热门排序和综合排序之前，根据检索结果中的质量情况，提一些较新的结果到前面，满足时效性
 *
 * @param [in] info   : QsInfo*
 * @return  int 
 * @author qiupengfei
 * @date 2014/05/08 14:13:57
**/
static int resort_for_time(QsInfo* info, SourceIDDict* dict, int classifier)
{
    const int THRE_6 = 4;
    const int THRE_24 = 8;
    const int THRE_48 = 10;
    const int THRE_WEEK = 15;
    const int TIMESPAN_6 = 6 * 3600;
    const int TIMESPAN_24 = 24 * 3600;
    const int TIMESPAN_48 = 48 * 3600;
    const int TIMESPAN_WEEK = 7 * 24 * 3600;
    const int TIME_VALUE_EXTRA = 100000000;
    const int MAX_PROMOTE_NUM = 10;
	QsInfoBC *bcinfo = &info->bcinfo;
    int hot6 = 0;
    int hot24 = 0;
    int hot48 = 0;
    int hot_week = 0;

    int curtime = time(NULL);
    for (int i = 0; i < bcinfo->get_doc_num(); i++)
    {
        SrmDocInfo* doc = bcinfo->get_doc_at(i);
        AcExtDataParser attr(doc);
        uint64_t fwnum = attr.get_fwnum();
        uint64_t digit_attr = attr.get_digit_attr();
        //只统计原创结果
        if (GET_FORWARD_FLAG(digit_attr))
        {
            continue;
        }
        int diff_time = curtime - doc->rank;
        if (fwnum >= THRE_6 && diff_time < TIMESPAN_6)
        {
            hot6++;
        }
        if (fwnum >= THRE_24 && diff_time < TIMESPAN_24)
        {
            hot24++;
        }
        if (fwnum >= THRE_48 && diff_time < TIMESPAN_48)
        {
            hot48++;
        }
        if (fwnum >= THRE_WEEK && diff_time < TIMESPAN_WEEK)
        {
            hot_week++;
        }
    }
    SLOG_DEBUG("resort_for_time: hot6:%d, hot24:%d, hot48:%d, hot_week:%d", hot6, hot24, hot48,hot_week);
    int add_time = curtime;
    int add_num = 0;
    int fw_thre = THRE_WEEK;
    if (hot6 > 5)
    {
        add_time = curtime - TIMESPAN_6;
        add_num = hot6 / 2;
        fw_thre = TIMESPAN_6;
    }
    else if (hot24 > 5)
    {
        add_time = curtime - TIMESPAN_24;
        add_num = hot24 / 2;
        fw_thre = THRE_24;
    }
    else if (hot48 > 5)
    {
        add_time = curtime - TIMESPAN_48;
        add_num = hot48 / 2;
        fw_thre = THRE_48;
    }
    else if (hot_week > 5)
    {
        add_time = curtime - TIMESPAN_WEEK;
        add_num = hot_week / 2;
        fw_thre = THRE_WEEK;
    }
    if (add_num > MAX_PROMOTE_NUM)
    {
        add_num = MAX_PROMOTE_NUM;
    }
    //不提权多条同一用户的结果
    //不提权多条重复的结果
    uint64_t promote_uid[MAX_PROMOTE_NUM];
    int promote_dup[MAX_PROMOTE_NUM];
    int promote_num = 0;
    for (int i = 0; i < bcinfo->get_doc_num(); i++)
    {
        SrmDocInfo* doc = bcinfo->get_doc_at(i);
        AcExtDataParser attr(doc);
        uint64_t digit_attr = attr.get_digit_attr();
        uint64_t fwnum = attr.get_fwnum();
        uint64_t cmtnum = attr.get_cmtnum();
        uint64_t qi = attr.get_qi();
        bool is_bad = is_bad_doc(info, doc, classifier, qi, fwnum,cmtnum, dict);
        if (doc->rank > add_time 
                && promote_num < add_num
                && attr.get_fwnum() >= fw_thre
                && !GET_FORWARD_FLAG(digit_attr)
                && !is_bad)
        {
            bool found_dup = false;
            for (int j = 0; j < promote_num; j++)
            {
                //注意：big uid 没有去重
                if (GetDomain(doc) > 1 && promote_uid[j] == GetDomain(doc))
                {
                    found_dup = true;
                    break;
                }
                if (promote_dup[j] == attr.get_dup())
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
            promote_uid[promote_num] = GetDomain(doc);
            promote_dup[promote_num] = attr.get_dup();
            SLOG_DEBUG("resort_for_time: promoted [uid:%llu, rank:%u, fwnum:%u]", GetDomain(doc), doc->rank, attr.get_fwnum());
            promote_num++;
        }
    }
    bcinfo->sort(0, bcinfo->get_doc_num());
    return 0;
}

int ac_inner_uids(QsInfo *info) {
	User2ClassDict *dict = Resource::getResource<User2ClassDict>(
			"user_class.dict");
	if (dict == NULL) {
        // warn FATAL, if not succ
		SLOG_FATAL("can not find data/user_class.dict");

		return -1;
	}

	int qtype = -1;
	int timel = 0;

	gettimelevel(info, &timel, &qtype);

	enum {
		UID_NEWS = 1, UID_FINANCE = 2, UID_TECH = 3, UID_OTHER = 4
	};

	int i;
	int lasti = -1;
	int optimes = 0;
	int num = info->bcinfo.get_doc_num();
	for (i = 0; i < num && i < 300; i++) {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);

		if (doc == NULL)
			continue;

		if (optimes >= 3)
			break;

		int old_value = doc->value;
		int utype = dict->find(GetDomain(doc));

		int lessi, morei;
		lessi = morei = i;

		if (i < 20) {
			lessi -= 2;
			morei -= 6;
		} else if (i < 50) {
			lessi -= 5;
			morei -= 20;
		} else if (i < 100) {
			lessi -= 10;
			morei -= 40;
		} else if (i < 200) {
			lessi -= 20;
			morei -= 60;
		} else {
			lessi -= 30;
			morei -= 100;
		}

		lessi = lessi > 0 ? lessi : 0;
		morei = morei > 0 ? morei : 0;

		if (lasti >= 0) {
			lessi = lessi > lasti ? lessi : lasti + 1;
			morei = morei > lasti ? morei : lasti + 1;
		}

		if ((timel >= 3 && utype == UID_NEWS) || ((qtype & 0x800) && utype
				== UID_FINANCE) || (((qtype & 0x400) || (qtype & 0x08))
				&& utype == UID_TECH)) {
			SrmDocInfo *doci = info->bcinfo.get_doc_at(morei);

			if (doci != NULL) {
                if (doc->docid == info->parameters.debugid) {
                    AC_WRITE_LOG(info, " [ac_inner_uids:%llu, orig:%u]", doc->docid, doc->value);
                }
				doc->value = doci->value + 1;
                if (doc->docid == info->parameters.debugid) {
                    AC_WRITE_LOG(info, " [ac_inner_uids:%llu, after:%u]", doc->docid, doc->value);
                }

				optimes++;

				SLOG_DEBUG(
						"INNER_UIDS:morei [%d %d %d] [%d %d %d %d] [%llu %u %u %u]",
						qtype, utype, timel, i, lasti, lessi, morei,
						GetDomain(doc), old_value, doci->value, doc->value);

				lasti = morei;
			}
		} else if (utype != -1) {
			SrmDocInfo *doci = info->bcinfo.get_doc_at(lessi);

			if (doci != NULL) {
                if (doc->docid == info->parameters.debugid) {
                    AC_WRITE_LOG(info, " [ac_inner_uids:%llu, orig:%u]", doc->docid, doc->value);
                }
				doc->value = doci->value + 1;
                if (doc->docid == info->parameters.debugid) {
                    AC_WRITE_LOG(info, " [ac_inner_uids:%llu, after:%u]", doc->docid, doc->value);
                }

				//optimes++;

				SLOG_DEBUG(
						"INNER_UIDS:lessi [%d %d %d] [%d %d %d %d] [%llu %u %u %u]",
						qtype, utype, timel, i, lasti, lessi, morei,
						GetDomain(doc), old_value, doci->value, doc->value);

				//lasti = lessi;
			}
		}
	}

	info->bcinfo.sort(0, i);

	return 0;
}

int ac_classifier(QsInfo *info){
	if(NULL==info){
		return -1;
	}
	QsInfoBC *bcinfo = &info->bcinfo;
	if(NULL==bcinfo){
		return -2;
	}

	int classifier=0;
	int i,numall =info->bcinfo.get_doc_num();
	if( numall > (SORT_COUNT+1000 )) 
		numall = SORT_COUNT+1000;
	int curtime = time(NULL)- 86400 ;
	int historytime = curtime - 86400 * 4;
	int todaynum=0, qi32=0, qi48=0,whiteqi48=0, historynum=0;
	int weeknum=0, weekqi32=0, weekqi48=0,weekwhiteqi48=0;
        for (i = 0; i < numall; i++) {
         	SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if(doc==NULL)
			continue;
        AcExtDataParser attr(doc);
		if(doc->rank>historytime){
			uint64_t qi = attr.get_qi();
			if(doc->rank>curtime){
				todaynum++;
				if(( qi & 0x30 )==0x20)
					qi32++;
				if(( qi & 0x30 )==0x30)	{
					qi48++;
					if( (qi&0x80)==0 )
						whiteqi48++ ;
				}
			}
			weeknum++;
			if(( qi & 0x30 )==0x20)
				weekqi32++;
			if(( qi & 0x30 )==0x30)	{
				weekqi48++;
				if( (qi&0x80)==0 )
					weekwhiteqi48++ ;
			}
		}
		else
			historynum++;
	}

	if( numall < 200 ) 
		classifier = 0;
	else if( whiteqi48 >= 20)
		classifier = 1 + whiteqi48/20;
	else if( whiteqi48 >=10 && whiteqi48<20 && qi48>=20 )
		classifier = 1;
	else
		classifier = 0;

	char log[1024];
	snprintf(log, 1024, "[RESULTNUM] todaynum:%d qi32:%d qi48:%d whiteqi48:%d week:%d-%d-%d-%d historynum:%d classifier:%d", 
		todaynum,qi32,qi48,whiteqi48, 
		weeknum, weekqi32,weekqi48,weekwhiteqi48,historynum,classifier);
	AC_WRITE_LOG(info, "\t%s", log);
	
	return classifier;
}

int update_validfans(QsInfo *info)
{
	auto_reference<DASearchDict> uids_validfans_week("uids_validfans_week");
	auto_reference<DASearchDict> uids_validfans_hour("uids_validfans_hour");
	if (!(*uids_validfans_week) && !(*uids_validfans_hour)) 
	{
		SLOG_FATAL("both week and hour validfans dicts are unavaliable");
		return -1;
	}
	/* 更新有效粉丝数 */
	for (int i = 0; i < info->bcinfo.get_doc_num(); i++) 
	{
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (doc == NULL)
			continue;
		uint64_t uid = GetDomain(doc); 
		if (uid && *uids_validfans_week) 
		{
			int validfans = uids_validfans_week->Search(uid);
			if (validfans != -1) 
			{
				if (doc->docid == info->parameters.debugid) 
				{
					AC_WRITE_LOG(info, " [validfans %d in uids_validfans_week,docid:%llu]",validfans, doc->docid);
				}
				doc->validfans = validfans;
			}
			else 
			{
				if (uid && *uids_validfans_hour) 
				{
					validfans = uids_validfans_hour->Search(uid);
					if (validfans != -1) 
					{ 
						if (doc->docid == info->parameters.debugid) 
						{
							AC_WRITE_LOG(info, " [validfans %d in uids_validfans_hour,docid:%llu]", validfans, doc->docid);
						}
						doc->validfans = validfans;
					}
				}
			}
		}
	}
	return 0;
}

int dafilter_extract(QsInfo *info, Ac_hot_statistic &ac_hot_data) {
	Json::Reader reader;
	Json::Value  root;
	if (info->json_intention == NULL || info->json_intention->text == NULL || !reader.parse(info->json_intention->text, root)) {
		AC_WRITE_LOG(info, " [json_intention NULL]");
		if (info->json_intention && info->json_intention->text) {
			AC_WRITE_LOG(info, " [json.parse error:%s]", info->json_intention->text);
		}
		return -1;
	}
	if (root.isMember("strategy_info") && root["strategy_info"].isMember("ac_media_filter")) {
		std::string val = root["strategy_info"]["ac_media_filter"].asString();
		int ok = expr_create(info, (char*)val.c_str(), val.size(), ac_hot_data.exp);
		if(ok == 0){
			ac_hot_data.expr_init_ok = false;
		} else {
			ac_hot_data.expr_init_ok = true;
		}
		AC_WRITE_LOG(info, " [ac_media_filter:%s,%d]", val.c_str(), ac_hot_data.expr_init_ok);
	}
	return 0;
}

bool pass_dafilter(QsInfo *info, Ac_hot_statistic &ac_hot_data, SrmDocInfo* doc) {
	if (ac_hot_data.expr_init_ok) {
		int val = expr_calc(info, ac_hot_data.exp, doc);
		if (!val) { 
			//AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE," [filter exp failed:"PRI64", %d]",doc->docid, val);
			return false;
		} else {
			//AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE," [filter exp OK:"PRI64", %d]", doc->docid, val);
		}
	}
	return true;
}

bool ac_film_sort(QsInfo *info,int ranktype, Ac_hot_statistic &ac_hot_data) 
{
	bool is_film_sort = false;
    if (ranktype == RANK_HOT && info->strategy_info.hotdata_as_media == 1 && (info->parameters.getUs() == 1 || info->parameters.getCategory() == 4))	
	{
		is_film_sort = true;
		auto_reference<DASearchDict>  film_uids("film_uids");
		if(!(*film_uids))
		{
			SLOG_FATAL("film_uids dict is unavaliable!");
		}
		time_t cur_time = time(NULL);
		QsInfoBC *bcinfo = &info->bcinfo;
		for (int i = 0; i < bcinfo->get_doc_num(); i++)
		{
			SrmDocInfo* doc = bcinfo->get_doc_at(i);
			if (doc == NULL)
				continue;
			AcExtDataParser attr(doc);
			if (!pass_dafilter(info, ac_hot_data, doc)) {
				if (doc->docid == info->parameters.debugid) {
					AC_WRITE_LOG(info, "[expr fail: %ld, v: %d,type: %d, filter: %ld]", doc->docid, attr.get_verified(), attr.get_verified_type(), attr.get_digit_attr());
				}
				if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
					AC_WRITE_LOG(info, "[expr fail: %ld, v: %d,type: %d, filter: %ld]", doc->docid, attr.get_verified(), attr.get_verified_type(), attr.get_digit_attr());
				}
				bcinfo->del_doc(doc);
				continue;
			} else {
				if (doc->docid == info->parameters.debugid) {
					AC_WRITE_LOG(info, "[pass expr: %ld, v: %d,type: %d, filter: %ld]", doc->docid, attr.get_verified(), attr.get_verified_type(), attr.get_digit_attr());
				}
				if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
					AC_WRITE_LOG(info, "[pass expr: %ld, v: %d,type: %d, filter: %ld]", doc->docid, attr.get_verified(), attr.get_verified_type(), attr.get_digit_attr());
				}
			}
			int64_t fwnum = attr.get_fwnum();
			//int64_t cmtnum = attr.get_cmtnum();
			int64_t validfwnm = attr.get_validfwnm();
			fwnum = fwnum * validfwnm / 1000;
			int64_t like_num = attr.get_likenum();
			int white_black_like_switch = int_from_conf(WHITE_BLACK_LIKE_SWITCH, 0);
			int64_t whitelike_num = attr.get_whitelikenum();
			if (white_black_like_switch == 1)
				whitelike_num = like_num * whitelike_num / 1000;
			if (info->parameters.getUs() == 1)
			{
				if (fwnum < int_from_conf(FILM_VALIDFWNUM_THR,10) && whitelike_num < int_from_conf(FILM_VALIDLIKENUM_THR,50))
				{
					if (doc->docid == info->parameters.debugid) {
						AC_WRITE_LOG(info, " [%lu,%lu,film validfwlike not pass:%lu]", fwnum, whitelike_num, doc->docid);
					}
					if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
						AC_WRITE_LOG(info, " [%lu,%lu,film validfwlike not pass:%lu]", fwnum, whitelike_num, doc->docid);
					}
					bcinfo->del_doc(doc);
					continue;
				}
				if (cur_time - doc->rank >= int_from_conf(FILM_TIME_THR, 1) * 24 * 3600)
				{
					if (doc->docid == info->parameters.debugid) {
						AC_WRITE_LOG(info, " [%lu,film time not pass:%lu]", doc->rank, doc->docid);
					}
					if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
						AC_WRITE_LOG(info, " [%lu,film time not pass:%lu]", doc->rank, doc->docid);
					}
					bcinfo->del_doc(doc);
					continue;
				}
				uint64_t uid = GetDomain(doc);
				if (uid && *film_uids && film_uids->Search(uid) != -1) 
				{
					continue;
				}
				else
				{
					if (doc->docid == info->parameters.debugid) {
						AC_WRITE_LOG(info, " [%lu,not in filmuids]", doc->docid);
					}
					if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
						AC_WRITE_LOG(info, " [%lu,not in filmuids]", doc->docid);
					}
					bcinfo->del_doc(doc);
					continue;
				}
			}
		}
		bcinfo->refresh();
		bcinfo->sort(0, bcinfo->get_doc_num(), time_cmp);
		for (int i = 0; i < info->bcinfo.get_doc_num() && i <= 3; i++)
		{
			SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
			if (doc == NULL) continue;
			AC_WRITE_LOG(info, " [filmtop %d id:%lu]", i, doc->docid);
		}
	}
	return is_film_sort;
}


int ac_sort_filter(QsInfo *info) {
	struct timeval begin;
	gettimeofday(&begin, NULL);
	if (NULL == info) {
		return -1;
	}
	
	QsInfoBC *bcinfo = &info->bcinfo;
	assert(info->magic == MAGIC_QSINFO);
	if (NULL == bcinfo) {
		return -2;
	}
	
	int num = bcinfo->get_doc_num();
	if (num <= 0 || info->parameters.nofilter != 0) {
		return 0;
	}
	
	ac_get_debug(info, bcinfo);
	
	int classifier = 0;
	int ranktype = RankType(info);

	int resort_flag = 1;
	int dup_flag = 1;
	
	ac_set_flag(info, &resort_flag, &dup_flag);

	ac_inner_uids(info);

	SLOG_DEBUG("FILTER_START:%d", info->bcinfo.get_doc_num());

	ac_mask_del(info);

	update_validfans(info);	

	Ac_hot_statistic  ac_hot_data;

    AcHotWeiboRank hot_weibo_rank;
	
	/* 通用过滤策略,使用逆波兰形式,字符串由da传输, 调用pass_dafilter来判断即可 */
	dafilter_extract(info, ac_hot_data);
	
	/* 电影媒体库的特殊需求，可以认为是走的精选流，但是策略完全不同，精选的一个特殊分支 */
	if (ac_film_sort(info, ranktype, ac_hot_data)) {
		goto Done;
	}
	SLOG_DEBUG("FILTER_MASK:%d", info->bcinfo.get_doc_num());
	
    //AcHotWeiboRank hot_weibo_rank;
	/* 正常精选逻辑 */
    if (ranktype == RANK_HOT || ranktype == RANK_SOCIAL)
    {
        hot_weibo_rank.do_hot_rank(info, classifier, resort_flag, dup_flag, ac_hot_data);
    }

	if (info->parameters.debug > HOT_SORT_LOG_LEVEL) {
		AC_WRITE_LOG(info, " [after_ac_sort_filter:num:%d] ", info->bcinfo.get_doc_num());
	}
	if (ranktype == RANK_HOT) {
        uint32_t dc_doc_num = int_from_conf(GET_DC_DOC_NUM, 100);
        num = bcinfo->get_doc_num();
        if (num > dc_doc_num) {
            num = dc_doc_num;
        }
        int i = 0;
        for (i = 0; i < num; i++) {
            SrmDocInfo *doc = bcinfo->get_doc_at(i);
            if (doc == NULL ||
                !get_bc_use_dc(doc->bcidx)) {
                continue;
            }
            DcDedupData data;
            data.doc = doc;
            data.content.clear();
            info->get_dc_docs.insert(make_pair(doc->docid, data));
        }
	} else if (ranktype == RANK_TIME) {
		bcinfo->sort(0, bcinfo->get_doc_num(), time_cmp);
	} else {
	}
	
Done:

	int reranking_cost = calc_time_span(&begin) / 1000;
	AC_WRITE_LOG(info, " {ac_sort_filter:time:%d }", reranking_cost);
	return 0;
}

int sort_print_version(void) {

	return 0;
}

#ifndef AC_SORT_UNIT

/*static void __attribute__ ((constructor))*/
void ac_sort_init(void) {
	char *filename = conf_get_str(g_server->conf_current, "bitmap_file",
			"/dev/shm/dup.bit");
	g_sort.bitmap = bitdup_open_oop(filename, g_server->oop);
	assert(g_sort.bitmap);

	char *p = conf_get_str(g_server->conf_current, "uid_black_file",
			"uids_black.txt");
	g_sort.uid_black_type = bitdup_add(g_sort.bitmap, p, MAX_UID_SIZE);

	p = conf_get_str(g_server->conf_current, "source_black_file",
			"sources_black.txt");
	g_sort.source_black_type = bitdup_add(g_sort.bitmap, p, MAX_SOURCE_SIZE);

	p = conf_get_str(g_server->conf_current, "source_white_file",
			"sources_white.txt");
	g_sort.source_white_type = bitdup_add(g_sort.bitmap, p, MAX_SOURCE_SIZE);

	if (g_sort.uid_black_type < 0 || g_sort.source_black_type < 0
			|| g_sort.source_white_type < 0) {
		unlink(filename);
		suicide();
	}

	slog_write(LL_NOTICE, "ac_sort so inited. black list file loaded");
	return;
}

static void __attribute__ ((destructor)) ac_sort_fini(void) {
	slog_write(LL_NOTICE, "ac_sort so destroyed");
	return;
}

static int TransStr(char *src, int lenSrc, char *dest, int lenDest) {
	int i, j;
	int cn_num = 0;
	int en_num = 0;
	for (i = 0, j = 0; i < lenSrc;) {
		if (src[i] & 0x80) {
			if (cn_num < 80) {
				dest[j++] = src[i];
				dest[j++] = src[i + 1];
				cn_num++;
			}

			i += 2;
		} else {
			if (en_num < 40) {
				dest[j++] = src[i];
				dest[j++] = ' ';
				en_num++;
			}

			i++;
		}

		if (cn_num + en_num > 80)
			break;
	}
	dest[j] = '\0';
	return j;
}

static float Modify(float ratio, int samePoint[], int count, int commonStrLen,
		int strLen) {
	if (count <= 0 || commonStrLen <= 0 || commonStrLen > strLen)
		return 0.0;

	float ret = 0.0;
	int totallen = 0;

	/*for (i = count - 1; i > 0; i--) {
	 if (samePoint[i] + 1 == samePoint[i - 1]) {
	 curlen++;
	 } else {
	 if (curlen >= 10 || (commonStrLen >= 10 && curlen >= 5)) {
	 float temp = (float) (curlen) / (float) (strLen);
	 ret += temp * (1 + 0.1 * temp);

	 totallen += curlen;
	 curlen = 1;
	 }
	 }
	 }

	 if (curlen >= 10 || (commonStrLen >= 10 && curlen >= 5)) {
	 float temp = (float) (curlen) / (float) (strLen);
	 ret += temp * (1 + 0.1 * temp);

	 totallen += curlen;
	 }*/

	ret += (float) (commonStrLen - totallen) / (float) strLen;
	if (ret > 1.0)
		ret = 1.0;

	return ret;
}

static int CutLeaves(int a1, int a2, int b1, int b2, int *sCur) {
	if (*sCur > Min(a2, b2) * ABS_DUP_RATIO) {
		return Min(a2, b2);
	}

	if ((a2 - a1) * 3 < (b2 - b1)) {
		return (a2 - a1) * (*sCur) / min(a2, b2) + 2;
	} else if ((b2 - b1) * 3 < (a2 - a1)) {
		return (b2 - b1) * (*sCur) / min(a2, b2) + 2;
	}

	if (a2 - a1 < 20 || b2 - b1 < 20)
		return min(a2 - a1, b2 - b1) * (*sCur) / min(a2, b2) + 2;

	if (max(a2 / (a2 - a1), b2 / (b2 - b1)) > 5)
		return min(a2 - a1, b2 - b1) * (*sCur) / min(a2, b2) + 2;

	return -1;
}

static int Caculate(char *strA, int a1, int a2, char *strB, int b1, int b2,
		int *interValue, char *testValue, int *sCur, int *sIndex,
		int sSamePoint[], int *call_count, CALC_OPT opt) {
	if (a1 > a2 || b1 > b2)
		return 0;

	if (*call_count > 2000)
		return *sCur;

	int ret;
	ret = CutLeaves(a1, a2, b1, b2, sCur);
	if (ret != -1)
		return ret;

	*call_count += 1;

	int offset00 = (a1) * (b2 + 2) + b1;
	int offset10 = (a1 + 2) * (b2 + 2) + b1;
	int offset01 = (a1) * (b2 + 2) + b1 + 2;
	int offset11 = (a1 + 2) * (b2 + 2) + b1 + 2;

	int *index00 = interValue + offset00;
	int *index10 = interValue + offset10;
	int *index01 = interValue + offset01;
	int *index11 = interValue + offset11;

	char *test_index10 = testValue + offset10 / 8;
	char *test_index01 = testValue + offset01 / 8;
	char *test_index11 = testValue + offset11 / 8;

	if (strA[a1] == strB[b1] && strA[a1 + 1] == strB[b1 + 1]) {
		if (!(*test_index11 & (0x1 << (offset11 % 8)))) {
			*index11 = Caculate(strA, a1 + 2, a2, strB, b1 + 2, b2, interValue,
					testValue, sCur, sIndex, sSamePoint, call_count, CALC_AB);
			*test_index11 |= (0x1 << (offset11 % 8));
		}

		*index00 = *index11 + 2;
		if (*index00 >= *sCur) {
			*sCur = *index00;
			sSamePoint[*sIndex] = a1 / 2 + 1;
			*sIndex += 1;
		}
	} else {
		static int spawn = 40;

		if (opt == CALC_B && (b2 - b1) + spawn < (a2 - a1)) {
			*index01 = (b2 - b1) * (*sCur) / min(a2, b2) + 2;
			*test_index01 |= (0x1 << (offset01 % 8));
		} else {
			if (!(*test_index01 & (0x1 << (offset01 % 8)))) {
				*index01
						= Caculate(strA, a1, a2, strB, b1 + 2, b2, interValue,
								testValue, sCur, sIndex, sSamePoint,
								call_count, CALC_B);
				*test_index01 |= (0x1 << (offset01 % 8));
			}
		}

		if (opt == CALC_A && (a2 - a1) + spawn < (b2 - b1)) {
			*index10 = (a2 - a1) * (*sCur) / min(a2, b2) + 2;
			*test_index10 |= (0x1 << (offset10 % 8));
		} else {
			if (!(*test_index10 & (0x1 << (offset10 % 8)))) {
				*index10
						= Caculate(strA, a1 + 2, a2, strB, b1, b2, interValue,
								testValue, sCur, sIndex, sSamePoint,
								call_count, CALC_A);
				*test_index10 |= (0x1 << (offset10 % 8));
			}
		}

		if (opt == CALC_AB && (a2 - a1 > spawn || b2 - b1 > spawn)) {
			*index11 = (min(a2 - a1, b2 - b1)) * (*sCur) / min(a2, b2) + 2;
			*test_index11 |= (0x1 << (offset11 % 8));
		} else {
			if (!(*test_index11 & (0x1 << (offset11 % 8)))) {
				*index11 = Caculate(strA, a1 + 2, a2, strB, b1 + 2, b2,
						interValue, testValue, sCur, sIndex, sSamePoint,
						call_count, CALC_AB);
				*test_index11 |= (0x1 << (offset11 % 8));
			}
		}

		*index00 = Max(Max(*index01, *index10), *index11);
	}

	return *index00;
}

int SequenceMatcher(QsInfo *info, char *A, int lenA, char *B, int lenB,
		float *ratio, int *call_count) {
	*ratio = 0.0;

	if (A == NULL || B == NULL || lenA == 0 || lenB == 0) {
		return -1;
	}

	if (lenA > 1.2 * lenB) {
		*ratio = 0.0;

		if (lenB > 30 && lenA > 1.5 * lenB && lenA < 3 * lenB) {
			char *findstr = strstr(A, B);
			if (findstr != NULL) {
				*ratio = 1.0;
			}
		}

		return 0;
	} else if (lenB > 1.2 * lenA) {
		*ratio = 0.0;

		if (lenA > 30 && lenB > 1.5 * lenA && lenB < 3 * lenA) {
			char *findstr = strstr(B, A);
			if (findstr != NULL) {
				*ratio = 1.0;
			}
		}

		return 0;
	}

	int size = (2 * lenA + 1) * (2 * lenB + 1);
	if (size > MAX_TEXT_LEN * MAX_TEXT_LEN) {
		return -2;
	}

	char *buffer = (char *) malloc(2 * (lenA + lenB) + 1024);
	if (buffer == NULL) {
		return -3;
	}

	char *strA = buffer;
	char *strB = buffer + 2 * lenA + 256;

	lenA = TransStr(A, lenA, strA, 2 * lenA + 1);
	lenB = TransStr(B, lenB, strB, 2 * lenB + 1);

	int sCur;
	int sIndex;

	*ratio = 0.0;
	sCur = sIndex = 0;

	int sSamePoint[MAX_TEXT_LEN];
	//memset(sSamePoint, -1, MAX_TEXT_LEN);

	int interValue[lenA + 16][lenB + 16];
	char *testValue = (char *) calloc(((lenA + 16) * (lenB + 16)) / 8 + 3, 1);
	if (testValue == NULL) {
		if (buffer != NULL)
			free(buffer);

		return -1;
	}

	int common;
	if (lenA <= lenB) {
		common = Caculate(strA, 0, lenA - 1, strB, 0, lenB - 1,
				(int *) interValue, testValue, &sCur, &sIndex, sSamePoint,
				call_count, CALC_NONE);
	} else {
		common = Caculate(strB, 0, lenB - 1, strA, 0, lenA - 1,
				(int *) interValue, testValue, &sCur, &sIndex, sSamePoint,
				call_count, CALC_NONE);
	}

	*ratio = Modify(*ratio, sSamePoint, sIndex, common / 2, (lenA + lenB) / 4);

	if (testValue != NULL)
		free(testValue);

	if (buffer != NULL)
		free(buffer);

	return 0;
}

static int ac_dup_multiforward(char *query, int qlen, char *content, int len) {
	return 0;

	char *first = NULL;
	char *second = NULL;
	char *rest = NULL;

	first = strstr(content, "//@");
	if (first == NULL)
		return 0;

	second = strstr(first + 3, "//@");
	if (second != NULL)
		rest = strstr(second + 3, "//@");

	if (first == content || first - content < qlen || rest != NULL) {
		char *ishit = strstr(content, query);

		if (second == NULL || (second != NULL && ((ishit != NULL && ishit
				< second) || second - content > len * 0.2)))
			return 0;
		else
			return -1;
	}

	return 0;
}

static int ac_dup_multihuati(char *query, int qlen, char *query_da, char *tag,
		int tlen, char *content, int clen, float *hitratio) {
	if (tlen <= 0 || qlen <= 0 || query_da == NULL || query == NULL || tag
			== NULL || content == NULL || hitratio == NULL)
		return 1;

	static const int max_seg_len = 128;
	static const int max_seg_num = 32;
	static const int max_cont_len = 1024;

	char query_seg[max_seg_num][max_seg_len];
	char tag_str[max_seg_num][max_seg_len];
	char cont_str[max_cont_len];

	int i, j;
	int qseg_num = 0;
	int tag_num = 0;
	int tag_num_hit = 0;

	//tag划分
	i = 0;
	j = i;
	while (i < tlen) {
		if (!isspace(tag[i]) && isspace(tag[i + 1])) {
			if (i - j >= max_seg_len || tag_num >= max_seg_num - 1) {
				*hitratio = 0.0;
				return -1;
			}

			strncpy(tag_str[tag_num], tag + j, i - j + 1);
			tag_str[tag_num][i - j + 1] = 0;
			if (strcasestr(tag_str[tag_num], query) != NULL) {
				tag_num_hit++;
			}

			tag_num++;
		}

		if (isspace(tag[i]) && !isspace(tag[i + 1]))
			j = i + 1;

		i++;
	}

	if (j < tlen) {
		if (tlen - j >= max_seg_len || tag_num >= max_seg_num - 1) {
			*hitratio = 0.0;
			return -1;
		}

		strncpy(tag_str[tag_num], tag + j, tlen - j + 1);
		tag_str[tag_num][tlen - j + 1] = 0;
		if (strcasestr(tag_str[tag_num], query) != NULL) {
			tag_num_hit++;
		}

		tag_num++;
	}

	if (tag_num >= 3)
		return -2;

	if (tag_num_hit <= 0) {
		//query未命中tag，放行
		return 2;
	}

	return 0;

	//切词串划分
	char query_gbk[max_cont_len];

	std::string result(query_da);
	strncpy(query_gbk, (char *) cv_utf82gbk(result).c_str(), max_cont_len);

	char *begin = query_gbk;
	char *next = strchr(begin, '/');
	while (next != NULL && *begin != 0) {
		if (next - begin >= max_seg_len || qseg_num >= max_seg_num - 1)
			return 3;

		strncpy(query_seg[qseg_num], begin, next - begin);
		query_seg[qseg_num][next - begin] = 0;
		qseg_num++;

		begin = next + 1;
		next = strchr(begin, '/');
	}

	if (*begin != 0) {
		strncpy(query_seg[qseg_num], begin, max_seg_len);
		qseg_num++;
	}

	//content去除tag
	int istag = 0;
	for (i = 0, j = 0; i < clen && j < max_cont_len; i++) {
		if (istag == 0 && content[i] == '#' && strchr(content + i + 1, '#')
				!= NULL) {
			istag = 1;
			continue;
		}

		if (istag == 1 && content[i] == '#') {
			istag = 0;
			continue;
		}

		if (istag == 0) {
			cont_str[j++] = content[i];
		}
	}

	cont_str[j++] = 0;

	//query切词在正文中命中综合超过query长度的一半以上0
	float hitchnnum = 0;
	float hitennum = 0;
	float allchnnum = 0;
	float allennum = 0;
	float isChn = 0;

	for (i = 0; i < qseg_num; i++) {
		if (query_seg[i][0] & 0x80) {
			isChn = 1;
		} else
			isChn = 0;

		int templen = strlen(query_seg[i]);
		if (isChn) {
			templen /= 2;
			allchnnum += 1.0;
		} else
			allennum += 1.0;

		if (isChn) {
			for (j = 0; j < templen;) {
				char tempstr[3];

				tempstr[0] = query_seg[i][j * 2];
				tempstr[1] = query_seg[i][j * 2 + 1];
				tempstr[2] = 0;

				if (strcasestr(cont_str, tempstr) != NULL)
					hitchnnum += 1.0 / templen;

				j++;
			}
		} else if (strcasestr(cont_str, query_seg[i]) != NULL) {
			hitennum += 1.0;
		}
	}

	*hitratio = (float) (hitchnnum + hitennum) / (allchnnum + allennum);

	int taglenall = 0;
	int contlenall = 0;

	for (i = 0; i < tag_num; i++)
		taglenall += strlen(tag_str[i]);

	contlenall = strlen(cont_str);

	if (contlenall > taglenall * 0.5 && contlenall < 100 && tag_num <= 2)
		return 4;

	float hit_ratio_all = 0.2;
	float hit_ratio_cn = 0.3;
	float hit_ratio_en = 0.3;

	if (tag_num > 3) {
		hit_ratio_all *= 2;
		hit_ratio_cn *= 2;
		hit_ratio_en *= 1.5;
	}

	if (*hitratio < hit_ratio_all && (hitchnnum < allchnnum * hit_ratio_cn
			|| hitennum < allennum * hit_ratio_en)) {
		if (strstr(cont_str, "话题") != NULL || strstr(cont_str, "详情") != NULL
				|| strstr(cont_str, "我点评了") != NULL)
			return 5;

		for (i = 0; i < tag_num; i++) {
			if (strcasestr(cont_str, tag_str[i]) != NULL)
				return 6;
		}

		return -3;
	}

	return 0;
}

static int ac_dup_multiat(char *query, int qlen, char *utext, int len) {

	return 0;
}

static int ac_get_abs_field(SrmDocInfo *doc, int field, int type, char *buffer,
		int *len) {
	if (doc == NULL || buffer == NULL || len == NULL)
		return -1;

	int i;
	int fnum = doc->getDI_fieldNum();

	*len = 0;
	buffer[0] = 0;
	for (i = 0; i < fnum; i++) {
		int fid = doc->getDI_fieldIdAt(i);
		int ftype = doc->getDI_fieldTypeAt(i);

		if (fid == field && ftype == type && type == TYPE_CHAR) {
			const char *data = doc->getDI_fieldValueAsStrAt(i).c_str();

			if (strlen(data) > 0) {
				strncpy(buffer,
						cv_utf82gbk(doc->getDI_fieldValueAsStrAt(i)).c_str(),
						MAX_TEXT_LEN - 1);
				*len = strlen(buffer);
			}

			break;
		}
	}

	if (field == F_CONTENT && *len > 0) {
		//预处理，去除短链等
		char *slink = strstr(buffer, "<sina:link");
		if (slink != NULL)
			*len = slink - buffer;
	}

	//SLOG_DEBUG("ABS_DUP:%d %d %s ac_get_abs_field", field, *len, buffer);

	return 0;
}

static int ac_print_info(QsInfo *info, char *title, int num, uint64_t docid) {
	int i;
	int num1 = info->bcinfo.get_doc_num();
	num1 = num > num1 ? num1 : num;

	for (i = 0; i < num1; i++) {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);

		if (doc == NULL)
			continue;

		if (docid > 0) {
			if (doc->docid == docid)
				SLOG_DEBUG("PRINT_INFO:%s %d %lu %llu %u", title, i, doc->docid,
						GetDomain(doc), doc->value);
		} else
			SLOG_DEBUG("PRINT_INFO:%s %d %lu %llu %u", title, i, doc->docid,
					GetDomain(doc), doc->value);
	}
}

int get_doc_detail(SrmDocInfo *doc, char * debug_info, size_t buff_size) {
    int str_len = strlen(doc->ac_debug);
    if (str_len >= buff_size) {
        str_len = buff_size - 1;
    }
    memcpy(debug_info, doc->ac_debug, str_len);
	debug_info[str_len] = '\0';
    return 0;
}

#endif

