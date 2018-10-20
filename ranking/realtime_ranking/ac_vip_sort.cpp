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
#include <list>
#include <assert.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dacore/auto_reference.h>
#include "search_dict.h"
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
#include "ac_hot_sort_model.h"
#include "ac_g_conf.h"
#include "intention_def.h"
#include "ac_intervene_dict.h"
#include "csort_vip_base.h"
#include "ac_sandbox.h"
#include "ac_hot_ext_data_parser.h"


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

enum {
	CHECK_FAMOUS_USER_VALID_NONE = 0,
	CHECK_FAMOUS_USER_VALID_PUSH_LINK = 1,
	CHECK_FAMOUS_USER_VALID_DROP = 2,
};

typedef struct {
	void *bitmap;
	int uid_black_type;
	int source_black_type;
	int source_white_type;
} AcSort;

extern ACServer *g_server;
AcSort g_sort;
CsortVipBase g_csort_vip_base;
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
#define VIP_USERS_CNT 10

static int ac_sort_rerank(QsInfo *info, int dup_flag, int resort_flag);
void ac_resort(QsInfo *info, int ranktype);
static int ac_dup_multiforward(char *query, int qlen, char *content, int len);
int ac_unset_default(QsInfo *info);

uint64_t ac_sort_uid(SrmDocInfo *doc) {
	return GetDomain(doc);
}

#define DOC_VALUE_ZERO (1)

static uint32_t ac_reset_docvalue(uint32_t value) {
	return value;
}

#define TMP_LOG_LEN 1024
#define DUP_STR_LEN 128

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
        AcExtDataParser attr(doc);
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

void ac_mask_del(QsInfo *info) {
	QsInfoBC *bcinfo = &info->bcinfo;

	node_t *htable = (node_t*) (info->buffer + info->buf_pos);
	HASH_INIT(*htable, MAX_DUP_SIZE);

	char log[TMP_LOG_LEN];
	int log_len = snprintf(log, TMP_LOG_LEN, "[ac_sort:");

	int doc_ori_total, doc_total;

	if (bcinfo->flag && bcinfo->get_doc_num() > 1200) {//
		doc_ori_total = doc_total = bcinfo->get_doc_num() - LEFT_COUNT;
		log_len += snprintf(log + log_len, TMP_LOG_LEN, " hold");
	} else {
		doc_ori_total = doc_total = bcinfo->get_doc_num();
		log_len += snprintf(log + log_len, TMP_LOG_LEN, " unhold");
	}

	int uid_black_count = 0;
	int source_black_count = 0;
	char *uid_black = bitdup_get(g_sort.bitmap, g_sort.uid_black_type);
	char *source_black = bitdup_get(g_sort.bitmap, g_sort.source_black_type);

	int i;
	int deleted_num = 0;
	bool check_query_intervention = true;
	char *query = (char *)info->parameters.getKey().c_str();
	if (query == NULL) {
		check_query_intervention = false;
	}
	AcInterventionDel *dict = Resource::Reference<AcInterventionDel>("query_del.dict");
	if (dict == NULL) {
		check_query_intervention = false;
		if (info->parameters.debug) {
			AC_WRITE_LOG(info, "\n[AcInterventionDel::dict == NULL]\n");
		}
	} else {
		if (info->parameters.debug) {
			AC_WRITE_LOG(info, "\n[AcInterventionDel::dict.size:%d]\n", dict->count());
			const std::map<std::string, std::set<uint64_t> >& data = dict->Data();
			std::map<std::string, std::set<uint64_t> >::const_iterator it = data.begin();
			for (; it != data.end(); ++it) {
				AC_WRITE_LOG(info, "\n[query:%s]\n", it->first.c_str());
				std::set<uint64_t>::iterator it_set = it->second.begin();
				for (; it_set != it->second.end(); ++it_set) {
					AC_WRITE_LOG(info, "\n[docid:%llu]\n", *it_set);
				}
			}
		}
	}

	// for mids mask
	ac_black_mids_mask(info, NULL, false);

    // for query.uid black
    ac_sandbox(info, NULL, false);
	ACLongSet* uid_dict = Resource::Reference<ACLongSet>(
                conf_get_str(info->conf, "uids_black_manual",
                "uids_black_manual.txt"));
	auto_reference<DASearchDictSet> black_uids("uids_black");
	for (i = 0; i < doc_total; i++) {
		SrmDocInfo *doc = bcinfo->get_doc_at(i);

		if (doc == NULL)
			continue;
		if (check_query_intervention && dict && dict->find(query, doc->docid)) {
			SLOG_DEBUG("docid %u of query %s in black interventionDel", (uint32_t) doc->docid, query);
			if (info->parameters.debug) {
				AC_WRITE_LOG(info, "\n[AcInterventionDel,docid:%llu,of query %s]", doc->docid, query);
			}
			bcinfo->del_doc(doc);
			deleted_num++;
			continue;
		}

		if (bcCaller.isBlack(info->params.squery, doc->bsidx, doc->docid)) {
			SLOG_DEBUG("docid %u %u of query %s in black doc_query",
					(uint32_t) doc->docid, doc->bsidx, info->params.squery);

			if (info->parameters.debug) {
				AC_WRITE_LOG(info, "\n[del,docid:%llu,%u of query %s]", doc->docid, doc->bsidx, info->params.squery);
			}
			bcinfo->del_doc(doc);
			deleted_num++;
			continue;
		}
		//if (resort_flag && doc->extra == NULL) {
		uint64_t uid = GetDomain(doc);
		if (uid && BITDUP_EXIST(uid_black, uid,/* MAX_UID_SIZE*/0xffffffff)) {
			uid_black_count++;
			if (info->parameters.debug) {
				AC_WRITE_LOG(info, "\n[del,docid:%llu,uid:%llu]", doc->docid, uid);
			}
			bcinfo->del_doc(doc);
			deleted_num++;
			continue;
		}
        if (uid && uid_dict && uid_dict->find(uid)) {
			uid_black_count++;
            if (info->parameters.debug) {
                AC_WRITE_LOG(info, "\n%d,del %lu", __LINE__, doc->docid);
            }
			bcinfo->del_doc(doc);
			deleted_num++;
			continue;
        }
        AcExtDataParser attr(doc);
        int source = attr.get_digit_source();
		if (source && BITDUP_EXIST(source_black, source, MAX_SOURCE_SIZE)) {
			source_black_count++;
			if (info->parameters.debug) {
				AC_WRITE_LOG(info, "\n[del,docid:%llu,source:%u]", doc->docid, source);
			}
			bcinfo->del_doc(doc);
			deleted_num++;
			continue;
		}
        if (uid && *black_uids) {
            int ret = black_uids->Search(uid);
            if (ret == -1) {
                continue;
            }
            DOC_WRITE_ACDEBUG(doc, "delete:in black_uids:%d", ret);
            if (doc->docid == info->parameters.debugid) {
                AC_WRITE_LOG(info, " \n[del,in black_uids,docid:%llu,%d]", doc->docid, ret);
            }
            bcinfo->del_doc(doc);
            deleted_num++;
        }
		//}
		/*if (deleted_num) {
		 } else {
		 if ((ctime + difftime) < doc->value && doc->bcidx >= 4) {
		 doc->value = i + 1;

		 SLOG_WARNING("[seqid:%u]the doc value is error:%d",
		 info->seqid, doc->value);
		 }
		 }*/
	}
	if (dict != NULL) {
		dict->UnReference();
	}
	if (uid_dict != NULL) {
		uid_dict->UnReference();
	}
	doc_total -= deleted_num;
	SLOG_DEBUG("doc_total:%d, the num of empty results:%d", doc_total,
			deleted_num);
	if (uid_black_count) {
		if (log_len < TMP_LOG_LEN)
			log_len += snprintf(log + log_len, TMP_LOG_LEN, " uid:%d", uid_black_count);
	}
	if (source_black_count) {
		if (log_len < TMP_LOG_LEN)
			log_len += snprintf(log + log_len, TMP_LOG_LEN, " source:%d", source_black_count);
	}
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

static int ac_unset_insert_one(QsInfo *info, const std::vector<uint64_t>& docid_vec,
		const std::map<uint64_t, uint32_t>& docid_num_map, const std::vector<uint64_t>& docid_online_vec,
		uint32_t value, uint32_t category, uint32_t vip_type_famous,
		std::vector<uint64_t>& fix_docid_vec, std::set<uint64_t>& already_fix_docid) {
	SrmDocInfo *newdoc = NULL;
	QsInfoBC *bcinfo = &info->bcinfo;
	if (docid_vec.size() == VIP_POS_NUM) {
		// set intervention docid, insert to fix_docid_vec
		for (int i = 0; i < docid_vec.size() && i < fix_docid_vec.size(); ++i) {
			uint64_t famous_docid = docid_vec[i];
			if (famous_docid == 0) {
				continue;
			}
			std::map<uint64_t, uint32_t>::const_iterator it_doc = docid_num_map.find(famous_docid);
			if (it_doc != docid_num_map.end()) {
				SrmDocInfo *docid_doc = info->bcinfo.get_doc_at(it_doc->second);
				if (docid_doc == NULL) {
					continue;
				}
				docid_doc->value = value - i;
				if (info->parameters.debug) {
					AC_WRITE_LOG(info, "\n[AcInterventionAdd:update famous,id:%llu,v:%u]", docid_doc->docid, docid_doc->value);
				}
				fix_docid_vec[i] = docid_doc->docid;
				already_fix_docid.insert(docid_doc->docid);
			} else {
				newdoc = NULL;
				newdoc = info->thread->popDoc();
				if (newdoc == NULL) {
					continue;
				}
				newdoc->docid = famous_docid;
				newdoc->value = value - i;
				newdoc->setCategory(category);
				newdoc->vip_type = vip_type_famous;
				bcinfo->insert_doc(newdoc, 0);
				if (info->parameters.debug) {
					AC_WRITE_LOG(info, "\n[AcInterventionAdd:famous,id:%llu,v:%u]", newdoc->docid, newdoc->value);
				}
				fix_docid_vec[i] = famous_docid;
				already_fix_docid.insert(famous_docid);
			}
		}
		// set bs result docid one by one, 
		for (int i = 0; i < fix_docid_vec.size(); ++i) {
			uint64_t famous_docid = fix_docid_vec[i];
			if (famous_docid != 0) {
				continue;
			}
			uint32_t index = 0;
			for (int j = 0; j < docid_online_vec.size(); ++j) {
				uint64_t tmp_docid = docid_online_vec[j];
				std::set<uint64_t>::iterator it_find = already_fix_docid.find(tmp_docid);
				if (it_find == already_fix_docid.end()) {
					famous_docid = tmp_docid;
					index = j;
					break;
				}
			}
			if (famous_docid == 0) {
				continue;
			}
			SrmDocInfo *docid_doc = info->bcinfo.get_doc_at(index);
			if (docid_doc == NULL) {
				continue;
			}
			docid_doc->value = value - i;
			already_fix_docid.insert(famous_docid);
		}
	}
}

int ac_unset_default(QsInfo *info) {
	if (info->parameters.debug) {
		AC_WRITE_LOG(info, "\n[ac_unset_default::num:%d]\n", info->bcinfo.get_doc_num());
	}
	int intention = 0;
	int timel = 0;
	gettimelevel(info, &timel, &intention);
	QsInfoBC *bcinfo = &info->bcinfo;
	char *query = (char *)info->parameters.getKey().c_str();
	if (query == NULL) {
		if (info->parameters.debug) {
			AC_WRITE_LOG(info, "\n[AcInterventionAdd query==NULL]\n");
		}
		return 0;
	}
	AcInterventionAdd *dict = Resource::Reference<AcInterventionAdd>("query_add.dict");
	if (dict == NULL) {
		if (info->parameters.debug) {
			AC_WRITE_LOG(info, "\n[AcInterventionAdd dict==NULL]\n");
		}
		return 0;
	}
	const std::vector<uint64_t>& famous_docid_vec = dict->find(query, DOC_FAMOUS_USER);
	const std::vector<uint64_t>& media_docid_vec = dict->find(query, DOC_MEDIA_USER);
	int num = bcinfo->get_doc_num();
	std::map<uint64_t, uint32_t> docid_num_map;
	for (int i = 0; i < num; i++) {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (doc == NULL) {
			continue;
		}
		{
			docid_num_map[doc->docid] = i;
			if (info->parameters.debug) {
				AC_WRITE_LOG(info, "\n[AcInterventionAdd:%llu %d]", doc->docid, i);
			}
		}
	}

	int famous_pos = 0, media_pos = 0;
	int now = time(NULL);
	std::vector<uint64_t> famous_fix_docid_vec, media_fix_docid_vec;
	famous_fix_docid_vec.resize(VIP_POS_NUM, 0);
	media_fix_docid_vec.resize(VIP_POS_NUM, 0);
	std::vector<uint64_t> famous_docid_online_vec, media_docid_online_vec;
	famous_docid_online_vec.resize(VIP_POS_NUM, 0);
	media_docid_online_vec.resize(VIP_POS_NUM, 0);
	std::set<uint64_t> already_fix_docid;
	// get bs result docid list, insert to famous_docid_online_vec and media_docid_online_vec
	for (int i = 0; i < num; ++i) {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (doc == NULL) {
			continue;
		}
		if ((doc->vip_type == VIP_TYPE_FAMOUS) &&
				(famous_pos < famous_docid_online_vec.size()) &&
				(famous_pos < VIP_POS_NUM)) {
			famous_docid_online_vec[famous_pos] = doc->docid;
			++famous_pos;
			continue;
		}
		if ((doc->vip_type == VIP_TYPE_MEDIA) &&
				(media_pos < media_docid_online_vec.size()) &&
				(media_pos < VIP_POS_NUM)) {
			media_docid_online_vec[media_pos] = doc->docid;
			++media_pos;
		}
	}
	uint32_t value = 0;
	if (timel >= 3) {
		value = now + VALUE_EXTRA/8 + 3;
	} else {
		value = VALUE_EXTRA/8*9 + 3;
	}
	ac_unset_insert_one(info, famous_docid_vec, docid_num_map, famous_docid_online_vec, value, DOC_FAMOUS_USER, VIP_TYPE_FAMOUS, famous_fix_docid_vec, already_fix_docid);
	value = now;
	ac_unset_insert_one(info, media_docid_vec, docid_num_map, media_docid_online_vec, value, DOC_MEDIA_USER, VIP_TYPE_MEDIA, media_fix_docid_vec, already_fix_docid);
	if (dict != NULL) {
		dict->UnReference();
	}
	bcinfo->refresh();
	bcinfo->sort(0, SORT_COUNT);
	return 0;
}

int ac_abs_dup(QsInfo *info) {
	QsInfoBC *bcinfo = &info->bcinfo;
	assert(info->magic == MAGIC_QSINFO);
	if (bcinfo->get_doc_num() <= 0 || info->parameters.nofilter != 0) {
		ac_unset_default(info);
		return 0;
	}
	if (!(strcmp(info->params.sid, "t_search") == 0 || strcmp(info->params.sid,
			"t_wap") == 0 || strcmp(info->params.sid, "t_wap_c") == 0)) {
		ac_unset_default(info);
		return 0;
	}
	int resort_flag = 1;
	int dup_flag = 1;
	ac_set_flag(info, &resort_flag, &dup_flag);
	if (dup_flag == 0 || info->parameters.debug == 2) {
		ac_unset_default(info);
		return 0;
	}
	ac_unset_default(info);
	return 0;
}

int IsNeedFilter(QsInfo *info, int curdoc, std::set<std::string> *dup_set,
		int dup_flag, int resort_flag, int *isdupset, char* dup_str) {
	int need_filter = 0;

	if (resort_flag) {
		Result *results = (Result*) (info->buffer + info->buf_pos);
		if (results + curdoc != NULL && results[curdoc].delflag && need_filter == 0) {
			need_filter = 1;
		}
	}
	SrmDocInfo *doc = info->bcinfo.get_doc_at(curdoc);
	if (doc == NULL) {
		return need_filter;
	}

	if (dup_flag) {
        AcExtDataParser attr(doc);
		if(doc->docattr & (0x1ll << 60)) {
			need_filter = 2;
		}
		if (need_filter == 0 && attr.get_dup_cont() != 0 && curdoc < 10 * 20) {
			uint64_t dup_cont = attr.get_dup_cont();
			uint64_t domain = GetDomain(doc);
			if(domain==0) domain = 0xffffffff;
			int j, k;
			for (j = 0, k = 0; j < curdoc && k < 20; j++) {
				SrmDocInfo *doc_j = info->bcinfo.get_doc_at(j);
                if (doc_j == NULL) {
                    continue;
                }
				if (!info->bcinfo.find_doc(doc_j)) {
					continue;
				}
				if (doc_j == NULL || doc_j->extra == NULL)
					continue;

				if (doc_j->value < SORT_COUNT + 1000)
					continue;

				k++;
				if(GetDomain(doc_j) == domain ) {
					// need_filter = 3;
					break;
				}
                AcExtDataParser attr_j(doc_j);
				uint64_t attr_xor = dup_cont ^ attr_j.get_dup_cont();

				int diff = 0;
				while (attr_xor > 0) {
					attr_xor &= (attr_xor - 1);
					diff++;
				}

				if (diff <= 3) {
					need_filter = 4;
					break;
				}
			}
		}

		if (need_filter == 0 && *dup_str) {
			if (dup_set->find(dup_str) != dup_set->end()) {
				need_filter = 5;
			} else {
				dup_set->insert(dup_str);
				*isdupset = 1;
			}
		}
	}

	return need_filter;
}

static int CheckFamousUserValid(int timel, SrmDocInfo *doc, int three_day_time, int one_month_time, int is_link, int famous_user_cnt, const std::set<uint64_t>& famous_domain_set) {
	if (doc == NULL) {
		return CHECK_FAMOUS_USER_VALID_NONE;
	}
	if ((famous_user_cnt >= VIP_USERS_CNT) ||
		(famous_domain_set.find(GetDomain(doc)) != famous_domain_set.end())) {
		return CHECK_FAMOUS_USER_VALID_DROP;
	}
	return CHECK_FAMOUS_USER_VALID_NONE;
	if (timel >= 3) {
		if (doc->rank < three_day_time) {
			return CHECK_FAMOUS_USER_VALID_DROP;
		}
	} else {
		if (doc->rank < one_month_time) {
			return CHECK_FAMOUS_USER_VALID_DROP;
		}
/*
		if (is_link) {
			return CHECK_FAMOUS_USER_VALID_PUSH_LINK;
		}
*/
	}
	if ((famous_user_cnt >= VIP_USERS_CNT) ||
		(famous_domain_set.find(GetDomain(doc)) != famous_domain_set.end())) {
		return CHECK_FAMOUS_USER_VALID_DROP;
	}
	return CHECK_FAMOUS_USER_VALID_NONE;
}

static int ac_sort_rerank(QsInfo *info, int dup_flag, int resort_flag) {
	QsInfoBC *bcinfo = &info->bcinfo;
	int num = bcinfo->get_doc_num();
	int checknum = SORT_COUNT + 1000;
	num = num > checknum ? checknum : num;

	int i = 0;
	int category = info->parameters.getCategory();
	int media_user_cnt = 0, famous_user_cnt = 0, media_user_weibo_cnt = 0, famous_user_weibo_cnt = 0;
	int intention = 0;
	int timel = 0;
	gettimelevel(info, &timel, &intention);
	if (timel >= 3) {
		bcinfo->sort(0, bcinfo->get_doc_num(), time_cmp);
	}
	uint32_t vip_type = GetVipType(intention, timel);
	std::set<std::string> dup_set;
	std::map<uint64_t, uint64_t> media_domain_map;
	std::set<uint64_t> famous_domain_set;
	std::set<uint64_t> all_media_domain_set;
	std::set<uint64_t> all_famous_domain_set;
	std::set<uint64_t> drop_media_domain_set;
	std::set<uint64_t> drop_famous_domain_set;
	std::map<uint64_t, std::list<uint32_t> > drop_media_domain_map;
	std::list<uint64_t> drop_famous_domain_list;
	std::set<uint64_t> link_drop_famous_domain_set;
	std::list<uint64_t> link_drop_famous_domain_list;
	int media_all_link = 0, famous_all_link = 0;
	int best_media_user_i = 0, best_famous_user_i = 0;
	int one_day_time = time(NULL)- 86400 ;
	int three_day_time = time(NULL)- 86400*3;
	int one_month_time = time(NULL)- 86400*30 ;
	AC_WRITE_LOG(info, " [query,ct:%d,num:%d,debug:%d,timel:%d,inten:%d,type:%d] ", category, num, info->parameters.debug, timel, intention, vip_type);
	for (i = 0; i < num; i++) {
		if (category != DOC_TIME &&
			category != DOC_FAMOUS_USER &&
			category != DOC_MEDIA_USER) {
			if (info->parameters.debug) {
				AC_WRITE_LOG(info, "\n[break category:%d]", category);
			}
			break;
		}
		SrmDocInfo *doc = bcinfo->get_doc_at(i);
		if (doc == NULL)
			continue;
		int droptype = 0;
		int isdupset = 0;
		int famous_type = 0;
		char dup_str[DUP_STR_LEN];
		memset(dup_str, 0, DUP_STR_LEN);
        AcExtDataParser attr(doc);
        uint64_t digit_attr = attr.get_digit_attr();
		int is_link = 0;
		if (((digit_attr >> 2) & 0x1)) {
			is_link = 1;
		}
		if (attr.get_dup()) {
			int dup_str_len = snprintf(dup_str, DUP_STR_LEN, "%llu:%llu", attr.get_dup(), GetDomain(doc));
		}
		int need_filter = IsNeedFilter(info, i, &dup_set, dup_flag, resort_flag, &isdupset, dup_str);
		// doc->value = doc->rank;
		uint64_t uid = GetDomain(doc);
		if(need_filter) {
			doc->value = num - i + 1;
			doc->setCategory(DOC_TIME);
			if (info->parameters.debug) {
				AC_WRITE_LOG(info, "\n[del:i:%d,f:%d,att:%llu,id:%llu]", i, need_filter, doc->docattr, doc->docid);
			}
			droptype = 2;
		} else if (category == DOC_MEDIA_USER && uid && g_csort_vip_base.CheckMediaUser(uid, intention, timel, vip_type) && !is_link && g_csort_vip_base.CheckAttrValid(doc->docattr, DOC_MEDIA_USER)) {
			++media_user_cnt;
			doc->setCategory(DOC_MEDIA_USER);
			// media sort by time
			doc->value = doc->rank;
			if (info->parameters.debug) {
				snprintf(doc->ac_debug + doc->ac_curlen, sizeof(doc->ac_debug)-doc->ac_curlen,
						"[ct:%d,in:%d]", category, intention);
				doc->ac_curlen = strlen(doc->ac_debug);
				AC_WRITE_LOG(info, "\n[ok:i:%d,blue%d,id:%llu,v:%u]", i, media_user_cnt, doc->docid, doc->value);
			}
		} else if (category == DOC_FAMOUS_USER && uid && (famous_type = g_csort_vip_base.CheckFamousUser(uid, intention, timel, vip_type))) {
			++famous_user_cnt;
			if (timel >= 3) {
				doc->value = doc->rank;
			}
			doc->setCategory(DOC_FAMOUS_USER);
			//if (famous_type == 1) {
				// doc->value += VALUE_EXTRA/2;
			//}
			if (info->parameters.debug) {
				snprintf(doc->ac_debug + doc->ac_curlen, sizeof(doc->ac_debug)-doc->ac_curlen,
						"[ct:%d,in:%d]", category, intention);
				doc->ac_curlen = strlen(doc->ac_debug);
				AC_WRITE_LOG(info, "\n[ok:i:%dvip:%d,id:%llu,v:%u,f_t:%d]", i, famous_user_cnt, doc->docid, doc->value, famous_type);
			}
		} else if (category == DOC_TIME) {
			if (uid && g_csort_vip_base.CheckMediaUser(uid, intention, timel, vip_type) && !is_link && g_csort_vip_base.CheckAttrValid(doc->docattr, DOC_MEDIA_USER)) {
/*
				if ((timel >= 3 && doc->rank < one_day_time)) {
					//(media_user_cnt >= VIP_USERS_CNT) ||
					doc->value = num - i + 1;
					doc->setCategory(DOC_TIME);
					++media_user_weibo_cnt;
					if (drop_media_domain_set.find(uid) == drop_media_domain_set.end()) {
						drop_media_domain_set.insert(uid);
					}
					std::map<uint64_t, std::list<uint32_t> >::iterator iter_map = drop_media_domain_map.find(doc->rank);
					if (iter_map == drop_media_domain_map.end()) {
						std::list<uint32_t> index;
						index.push_back(i);
						drop_media_domain_map[doc->rank] = index;
					} else {
						iter_map->second.push_back(i);
					}
					all_media_domain_set.insert(uid);
					if (info->parameters.debug) {
						AC_WRITE_LOG(info, "\n[del:media,i:%d,id:%llu,timel:%d,link:%d]", i, doc->docid, (timel >= 3 && doc->rank < one_day_time), is_link);
					}
					if(isdupset){
						if (*dup_str) {
							dup_set.erase(dup_str);
						}
					}
					continue;
				}
*/
				
				std::map<uint64_t, uint64_t>::iterator it_media_map = media_domain_map.find(uid);
				if (it_media_map != media_domain_map.end()) {
					SrmDocInfo *media_doc = bcinfo->get_doc_at(it_media_map->second);
					if (media_doc != NULL) {
						if (media_doc->rank < doc->rank) {
							media_domain_map[uid] = i;
							doc->setCategory(DOC_MEDIA_USER);
							doc->vip_type = VIP_TYPE_MEDIA;
							media_doc->setCategory(DOC_TIME);
							media_doc->vip_type = VIP_TYPE_NONE;
						} else {
							doc->setCategory(DOC_TIME);
							doc->vip_type = VIP_TYPE_NONE;
						}
					} else {
						media_domain_map[uid] = i;
						doc->setCategory(DOC_MEDIA_USER);
						doc->vip_type = VIP_TYPE_MEDIA;
					}
				} else {
					media_domain_map[uid] = i;
					doc->setCategory(DOC_MEDIA_USER);
					doc->vip_type = VIP_TYPE_MEDIA;
				}
				all_media_domain_set.insert(uid);
				doc->value = doc->rank;
				++media_user_cnt;
				++media_user_weibo_cnt;
				if (info->parameters.debug) {
					AC_WRITE_LOG(info, "\n[ok:i:%d,blue:%d:%d,id:%llu,v:%u,stat:%lld]", i, media_user_cnt, media_user_weibo_cnt, doc->docid, doc->value, doc->docattr);
				}
				continue;
			}
			if (uid && (famous_type = g_csort_vip_base.CheckFamousUser(uid, intention, timel, vip_type))) {
				int check_famous_user_valid = CheckFamousUserValid(timel, doc, three_day_time, one_month_time, is_link, famous_user_cnt, famous_domain_set);
				if (check_famous_user_valid) {
					doc->value = num - i + 1;
					doc->setCategory(DOC_TIME);
					++famous_user_weibo_cnt;
					if (drop_famous_domain_set.find(uid) == drop_famous_domain_set.end()) {
						drop_famous_domain_list.push_back(i);
						drop_famous_domain_set.insert(uid);
					}
					if ((CHECK_FAMOUS_USER_VALID_PUSH_LINK == check_famous_user_valid) &&
						(link_drop_famous_domain_set.find(uid) == link_drop_famous_domain_set.end())) {
						link_drop_famous_domain_list.push_back(i);
						link_drop_famous_domain_set.insert(uid);
					}
					all_famous_domain_set.insert(uid);
					if (info->parameters.debug) {
						AC_WRITE_LOG(info, "\n[del:famous,i:%d,id:%llu,timel:%d,%d,link:%d]", i, doc->docid, (timel >= 3 && doc->rank < three_day_time), (timel < 3 && doc->rank < one_month_time), is_link);
					}
					if(isdupset){
						if (*dup_str) {
							dup_set.erase(dup_str);
						}
					}
					continue;
				}
				all_famous_domain_set.insert(uid);
				if (timel >= 3) {
					doc->value = doc->rank + VALUE_EXTRA/8;
				} else {
					doc->value += VALUE_EXTRA;
				}
				++famous_user_cnt;
				++famous_user_weibo_cnt;
				famous_domain_set.insert(uid);
				doc->setCategory(DOC_FAMOUS_USER);
				doc->vip_type = VIP_TYPE_FAMOUS;
				if (info->parameters.debug) {
					AC_WRITE_LOG(info, "\n[ok:i:%d,vip:%d:%d,id:%llu,v:%u,f_t:%d,stat:%lld]", i, famous_user_cnt, famous_user_weibo_cnt, doc->docid, doc->value, famous_type, doc->docattr);
				}
				continue;
			}
			doc->value = num - i + 1;
			doc->setCategory(DOC_TIME);
			if (info->parameters.debug) {
				AC_WRITE_LOG(info, "\n[del:no_uid,i:%d,uid:%llu,id:%llu,v:%u,stat:%llu]", i, uid, doc->docid, doc->value, doc->docattr);
			}
			droptype = 3;
		} else {
			if (info->parameters.debug) {
				AC_WRITE_LOG(info, "\n[del:else,i:%d,uid:%llu,id:%llu,v:%u]", i, uid, doc->docid, doc->value);
			}
			doc->value = num - i + 1;
			doc->setCategory(DOC_TIME);
			droptype = 4;
		}
		if((droptype)&&(isdupset)){
			if (*dup_str) {
				dup_set.erase(dup_str);
			}
		}
	}

	AC_WRITE_LOG(info, " [media:%d,%d,%d,%d, famous:%d,%d,%d,%d,%d,%d] ", media_domain_map.size(), all_media_domain_set.size(), drop_media_domain_set.size(), drop_media_domain_map.size(), famous_domain_set.size(), all_famous_domain_set.size(), drop_famous_domain_set.size(), drop_famous_domain_list.size(), link_drop_famous_domain_set.size(), link_drop_famous_domain_list.size());
	if (category == DOC_TIME) {
		if (media_domain_map.size() > 0) {
			std::map<uint64_t, std::list<uint32_t> >::reverse_iterator iter_media = drop_media_domain_map.rbegin();
			for (int i = media_domain_map.size();i < VIP_USERS_CNT && (iter_media != drop_media_domain_map.rend()); ++iter_media) {
				std::list<uint32_t>::iterator it_media_list = iter_media->second.begin();
				for (;i < VIP_USERS_CNT && it_media_list != iter_media->second.end(); ++it_media_list) {
					uint32_t index = *it_media_list;
					SrmDocInfo *doc = bcinfo->get_doc_at(index);
					if (doc == NULL) {
						continue;
					}
					uint64_t uid = GetDomain(doc);
					std::map<uint64_t, uint64_t>::iterator it_media_map = media_domain_map.find(uid);
					if (it_media_map != media_domain_map.end()) {
						SrmDocInfo *media_doc = bcinfo->get_doc_at(it_media_map->second);
						if (media_doc != NULL) {
							if (media_doc->rank < doc->rank) {
								media_domain_map[uid] = index;
								doc->value = doc->rank;
								doc->setCategory(DOC_MEDIA_USER);
								doc->vip_type = VIP_TYPE_MEDIA;
								++i;
								if (info->parameters.debug) {
									AC_WRITE_LOG(info, "\n[re_add_ok,media,i:%d,docid:%llu,uid:%llu,", index, doc->docid, GetDomain(doc));
								}
								media_doc->setCategory(DOC_TIME);
								media_doc->vip_type = VIP_TYPE_NONE;
							} else {
								doc->setCategory(DOC_TIME);
								doc->vip_type = VIP_TYPE_NONE;
							}
						} else {
							media_domain_map[uid] = index;
							doc->value = doc->rank;
							doc->setCategory(DOC_MEDIA_USER);
							doc->vip_type = VIP_TYPE_MEDIA;
							++i;
							if (info->parameters.debug) {
								AC_WRITE_LOG(info, "\n[re_add_ok,media,i:%d,docid:%llu,uid:%llu,", index, doc->docid, GetDomain(doc));
							}
						}
					} else {
						media_domain_map[uid] = index;
						doc->value = doc->rank;
						doc->setCategory(DOC_MEDIA_USER);
						doc->vip_type = VIP_TYPE_MEDIA;
						++i;
						if (info->parameters.debug) {
							AC_WRITE_LOG(info, "\n[re_add_ok,media,i:%d,docid:%llu,uid:%llu,", index, doc->docid, GetDomain(doc));
						}
					}
				}
			}
		}
		if (famous_domain_set.size() == 0) {
			std::list<uint64_t>::iterator iter_famous = link_drop_famous_domain_list.begin();
			for (int i = famous_domain_set.size();i < VIP_USERS_CNT && (iter_famous != link_drop_famous_domain_list.end()); ++iter_famous) {
				SrmDocInfo *doc = bcinfo->get_doc_at(*iter_famous);
				if (doc == NULL) {
					continue;
				}
				uint64_t uid = GetDomain(doc);
				if (famous_domain_set.find(uid) == famous_domain_set.end()) {
					famous_domain_set.insert(uid);
					if (timel >= 3) {
						doc->value = doc->rank + VALUE_EXTRA/8;
					} else {
						doc->value += VALUE_EXTRA;
					}
					doc->setCategory(DOC_FAMOUS_USER);
					doc->vip_type = VIP_TYPE_FAMOUS;
					++i;
					if (info->parameters.debug) {
						AC_WRITE_LOG(info, "\n[re_add_link_ok,vip,i:%d,docid:%llu,uid:%llu,", *iter_famous, doc->docid, GetDomain(doc));
					}
				}
			}
		}
		if (famous_domain_set.size() > 0) {
			std::list<uint64_t>::iterator iter_famous = drop_famous_domain_list.begin();
			for (int i = famous_domain_set.size();i < VIP_USERS_CNT && (iter_famous != drop_famous_domain_list.end()); ++iter_famous) {
				SrmDocInfo *doc = bcinfo->get_doc_at(*iter_famous);
				if (doc == NULL) {
					continue;
				}
				uint64_t uid = GetDomain(doc);
				if (famous_domain_set.find(uid) == famous_domain_set.end()) {
					famous_domain_set.insert(uid);
					if (timel >= 3) {
						doc->value = doc->rank + VALUE_EXTRA/8;
					} else {
						doc->value += VALUE_EXTRA;
					}
					doc->setCategory(DOC_FAMOUS_USER);
					doc->vip_type = VIP_TYPE_FAMOUS;
					++i;
					if (info->parameters.debug) {
						AC_WRITE_LOG(info, "\n[re_add_ok,vip,i:%d,docid:%llu,uid:%llu,", *iter_famous, doc->docid, GetDomain(doc));
					}
				}
			}
		}
	}
	AC_WRITE_LOG(info, " [media:%d,%d,%d,%d, famous:%d,%d,%d,%d,%d,%d] ", media_domain_map.size(), all_media_domain_set.size(), drop_media_domain_set.size(), drop_media_domain_map.size(), famous_domain_set.size(), all_famous_domain_set.size(), drop_famous_domain_set.size(), drop_famous_domain_list.size(), link_drop_famous_domain_set.size(), link_drop_famous_domain_list.size());
	if (info->parameters.debug == 2) {
		int num = info->bcinfo.get_doc_num();
		for (i = 0; i < num; i++) {
			SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
			if (doc == NULL)
				continue;
			if (doc->getCategory() != DOC_TIME) {
				//if (info->parameters.debug) {
				//	AC_WRITE_LOG(info, "\n[del,i:%d,docid:%llu,uid:%u,", i, doc->docid, GetDomain(doc));
				//}
				info->bcinfo.del_doc(doc);
			}
		}
	} else if (info->parameters.debug == 1) {
		int num = info->bcinfo.get_doc_num();
		for (i = 0; i < num; i++) {
			SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
			if (doc == NULL)
				continue;
			if (doc->getCategory() == DOC_TIME) {
				//if (info->parameters.debug) {
				//	AC_WRITE_LOG(info, "\n[del,i:%d,docid:%llu,uid:%u,", i, doc->docid, GetDomain(doc));
				//}
				info->bcinfo.del_doc(doc);
			}
		}
	} else {
		int num = info->bcinfo.get_doc_num();
		for (i = 0; i < num; i++) {
			SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
			if (doc == NULL)
				continue;
			if (doc->getCategory() == DOC_TIME) {
				info->bcinfo.del_doc(doc);
			}
		}
	}
	info->media_user_weibo_cnt = media_user_weibo_cnt;
	info->famous_user_weibo_cnt = famous_user_weibo_cnt;
	info->media_user_cnt = all_media_domain_set.size();
	info->famous_user_cnt = all_famous_domain_set.size();
	info->bcinfo.refresh();
	info->bcinfo.sort();
	if (category == DOC_FAMOUS_USER || category == DOC_MEDIA_USER) {
			int num = info->bcinfo.get_doc_num();
			int already_fix_num = 0;
			for (int i = 0; i < num && already_fix_num < VIP_POS_NUM; i++) {
				SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
				if (doc == NULL) {
					continue;
				}
				++already_fix_num;
				if (category == DOC_FAMOUS_USER) {
					doc->vip_type = VIP_TYPE_FAMOUS;
				} else if (category == DOC_MEDIA_USER) {
					doc->vip_type = VIP_TYPE_MEDIA;
				}
			}
	}
	return 0;
}

int ac_sort_filter(QsInfo *info) {
	if(NULL==info){
		return -1;
	}
	
	QsInfoBC *bcinfo = &info->bcinfo;
	assert(info->magic == MAGIC_QSINFO);
	if(NULL==bcinfo){
		return -2;
	}
	
	int num = bcinfo->get_doc_num();
	if (num <= 0 || info->parameters.nofilter != 0) {
		return 0;
	}
	
	ac_get_debug(info, bcinfo);
	
	int resort_flag = 1;
	int dup_flag = 1;
	
	ac_set_flag(info, &resort_flag, &dup_flag);

	SLOG_DEBUG("FILTER_START:%d", info->bcinfo.get_doc_num());

	ac_mask_del(info);

	SLOG_DEBUG("FILTER_MASK:%d", info->bcinfo.get_doc_num());
	
	ac_sort_rerank(info, dup_flag, resort_flag);

	return 0;
}

int sort_print_version(void) {

	return 0;
}

#ifndef AC_SORT_UNIT

/*static void __attribute__ ((constructor))*/
void ac_sort_init(void) {
	char *p;
	char *filename = conf_get_str(g_server->conf_current, "bitmap_file",
			"/dev/shm/dup.bit");

	g_sort.bitmap = bitdup_open_oop(filename, g_server->oop);
	assert(g_sort.bitmap);

	p = conf_get_str(g_server->conf_current, "uid_black_file",
					"uids_black.txt");
	g_sort.uid_black_type = bitdup_add(g_sort.bitmap, p, MAX_UID_SIZE);

	p = conf_get_str(g_server->conf_current, "source_black_file",
			"sources_black.txt");
	g_sort.source_black_type = bitdup_add(g_sort.bitmap, p, MAX_SOURCE_SIZE);

	p = conf_get_str(g_server->conf_current, "source_white_file",
			"sources_white.txt");
	g_sort.source_white_type = bitdup_add(g_sort.bitmap, p, MAX_SOURCE_SIZE);


	if (g_sort.uid_black_type < 0 || g_sort.source_black_type < 0 ||
		g_sort.source_white_type < 0) {
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

int get_doc_detail(SrmDocInfo *doc, char * debug_info,size_t buff_size) {
    return 0;
}


#endif
