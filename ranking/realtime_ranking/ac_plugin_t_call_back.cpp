/***************************************************************************
 * 
 * Copyright (c) 2013 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file ac_plugin_t_call_back.h
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/02/04
 * @version 1.0
 * @brief 
 *  
 **/

#include "sframe_type_server.h"
#include "ac_caller_man.h"
#include "ac_head.h"
#include "ac_filter.h"
#include "mark_red.h"
#include "ac_plugins_common_include.h"
#include "parsejson.h"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include "ac_type.h"
#include "ac_hot_sort_model.h"
#include "intention_def.h"
#include "ac_intervene_dict.h"
#include "dacore/resource.h"
#include "ac_global_conf.h"

using namespace boost::property_tree;


#define PRINT_CLUSTER_LOG_LELVE 0
#define MAX_BUF_SIZE 1024

#define AC_NEED_MARK(info,field_id) (info->dainfo. words_snap && \
	(info->params.flag & QSPARAMS_FLAG_MARKRED) && \
	info->conf->field_info.fields[field_id].mark)

#define BS_FIELD_NUM 12
#define FIELD_GETS(docinfo, arr)	{		\
	arr[0] = FIELD_GET_DOCID(docinfo);		\
	arr[1] = FIELD_GET_BCIDX(docinfo);		\
	arr[2] = FIELD_GET_GROUP(docinfo);		\
	arr[3] = GetDomain(docinfo);		\
	arr[4] = FIELD_GET_ATTRLOW32(docinfo);	\
	arr[5] = FIELD_GET_ATTRHIGH32(docinfo);	\
	arr[6] = FIELD_GET_ATTR64(docinfo);	\
	arr[7] = FIELD_GET_ATTR96(docinfo);	\
	arr[8] = FIELD_GET_RANK(docinfo);		\
	arr[9] = FIELD_GET_DOCATTR(docinfo);	\
	arr[10] = FIELD_GET_SCORE(docinfo);		\
	arr[11] = FIELD_GET_RELATION(docinfo);		\
}

extern int get_doc_detail(SrmDocInfo *doc, char * debug_info,size_t buff_size);

extern ACServer *g_server;

extern void *g_pool;

int _de(const unsigned char *p, unsigned short *codep);
int _en(unsigned short code, unsigned char *p);

static int cli_get_req(QsInfo *info, ACRequest *header, int request_len){
	QsInfoParams *params;
	int sid_len, query_len, ext_len;
	uint32_t ext_pos = 0;


	if(ACREQUEST_SIZE(header) != (uint32_t)request_len){
		SLOG_WARNING("[seqid:%d] the length of header is not correct", info->seqid);
		return -1;
	}

	params = &info->params;
	params->flag		= header->flag;
	params->nofilter	= header->nofilter;
	params->start		= header->start;
	params->num			= header->num;
	params->kf			= header->kf;
	params->ip			= header->ip;
	params->uid			= header->uid;
	params->ext_fmt		= header->ext_fmt;
	params->ext_size	= header->ext_size;
	sid_len				= header->sid_len;
	query_len			= header->key_len;
	ext_len				= header->ext_size;

	//sid
	params->sid = (char*)ac_malloc(info, sid_len + 1);
	memmove(params->sid, ACREQUEST_GET_SID(header), sid_len);
	params->sid[sid_len] = 0;

	//query
	params->squery = (char*)ac_malloc(info, query_len + 1);
	memmove(params->squery, ACREQUEST_GET_QUERY(header), query_len);
	params->squery[query_len] = 0;

	//ext
	if(ext_len){
		params->ext = (char*)ac_malloc(info, ext_len);
		memmove(params->ext, ACREQUEST_GET_EXT(header), header->ext_size);
	}

	if(params->flag & QSPARAMS_FLAG_BCSPEC){
		if(header->ext_size >= ext_pos + 4){
			params->bc_idx = *(uint32_t*)(params->ext + ext_pos);
			ext_pos += 4;
			SLOG_TRACE("[seqid:%u] assign bc_idx to %d", info->seqid, *(uint32_t*)(params->ext + ext_pos));
		}else{
			params->flag &= ~QSPARAMS_FLAG_BCSPEC;
		}
	}

	if(params->flag & QSPARAMS_FLAG_BSSPEC){
		if(header->ext_size >= ext_pos + 4){
			params->bs_idx = *(uint32_t*)(params->ext + ext_pos);
			ext_pos += 4;
			SLOG_TRACE("[seqid:%u] assign bs_idx to %d", info->seqid, *(uint32_t*)(params->ext + ext_pos));
		}else{
			params->flag &= ~QSPARAMS_FLAG_BSSPEC;
		}
	}
	return 0;
}

static int call_pass(void *args, char *k, char *v){
	QsInfo *info = (QsInfo*)args;
	info->buf_pos += sprintf(info->buffer + info->buf_pos, "&%s=%s", k, v);
	return 0;
}

static int ac_output_json_error(QsInfo *info, char *output, int maxlen){
	JsonText_t *json_tmp;
	int len;
	char tmp[50];
	json_tmp = CreateJsonText(10000);
	snprintf(tmp, sizeof(tmp), "%d", info->status);
	AddPairValue(json_tmp, "status", tmp, V_VALUE_STR);
	AddPairValue(json_tmp, "errinfo", info->errmsg, V_STR);
	len = json_tmp->curLen;
	memmove(output, json_tmp->text, len);
	FreeJsonText(json_tmp);
	return len;
}

int TCallBack::call_request(void *data, int cmd, char *request, int request_len, int *step, uint32_t *addr) {
	QsInfo *info = (QsInfo*)data;
	QsInfoParams *params;
	char *query;
	int max_res_num;

	int filter = GlobalConfSingleton::GetInstance().getAcConf().global.filter_keyword;

	info->buffer = server_buffer(g_server->server, &info->buf_size);
	info->buf_pos = 0;

	gettimeofday(&info->result.start_time, NULL);

	if (cmd == AC_CMD_JSON) {
		SLOG_TRACE("[seqid:%u] type of request is json, len:%d", info->seqid, request_len);
        int print_request_log = conf_get_int(g_server->conf_current, "print_request_log", 0);
        if (print_request_log) {
            int request_str_len = conf_get_int(g_server->conf_current, "request_str_len", 10240);
            int request_str_cp_len = request_len;
            if (request_str_cp_len > request_str_len - 1) {
                request_str_cp_len = request_str_len - 1;
            }
            char request_str[request_str_len];
            memcpy(request_str, request, request_str_len);
            request_str[request_str_cp_len] = 0;
            SLOG_TRACE("[seqid:%u] request:[%s]", info->seqid, request_str);
        }
		// test
		AC_WRITE_ERRLOG(info, "%s", request);

		int ret = info->readParams(request, request_len);
		if (ret) {
			info->status = -1;
			sprintf(info->errmsg, "param error!");
			AC_WRITE_LOG(info, "\tparam_error:%s", info->params.url);
			SLOG_WARNING("[%u] param error", info->seqid);
			goto LB_ERROR;
		}
        if (info->parameters.debug &&
            conf_get_int(g_server->conf_current, "stop_use_debug", 0)) {
            info->parameters.debug = 0;
        }
		const char * sid = NULL;
		int max_request_len_each_sid = GlobalConfSingleton::GetInstance().getAcConf().global.max_request_len_each_sid;
        SLOG_DEBUG("[seqid:%u] max_request_len_each_id is %d", info->seqid, max_request_len_each_sid);
		sid = info->parameters.sid.c_str();
		if (sid == NULL) {
			info->status = -1;
			sprintf(info->errmsg, "sid is empty");
			SLOG_WARNING("[seqid:%u] sid is empty", info->seqid);
			goto LB_ERROR;
		}

		hconf_pass(info->parameters.getKvlist(), call_pass, info);
		query = info->buffer + 1;
		info->params.url = ac_get_query(info, query, strlen(query));
		//DLLCALL(so, query_input, ret, info);
        
        ret = query_input(info);

		if (ret) {
			info->status = -1;
			sprintf(info->errmsg, "param error!");
			AC_WRITE_LOG(info, "\tparam_error:%s", info->params.url);
			SLOG_WARNING("[%u] param error", info->seqid);
			goto LB_ERROR;
		}
	} else if (cmd == AC_CMD_STRUCT) {
		SLOG_TRACE("[seqid:%u] the type of request is struct", info->seqid);
		if (cli_get_req(info, (ACRequest*) request, request_len)) {
			AC_WRITE_LOG(info, "\tstruct_error");
			slog_write(LL_WARNING, "%u\tstruct error", info->seqid);
			goto LB_ERROR;
		}
	} else {
		SLOG_WARNING("[%u] tcmd error:%d", info->seqid, cmd);
		goto LB_ERROR;
	}

	params = &info->params;
	if(params->start < 0 || params->start >= MAX_DOCID_NUM){
		params->start = 0;
	}
	if(params->num < 0){
		params->num = 10;
	}
	max_res_num = GlobalConfSingleton::GetInstance().getAcConf().global.max_res_num;
	if(params->num > max_res_num){
		params->num = max_res_num;
	}
	if(params->start + params->num > MAX_DOCID_NUM){
		params->num = MAX_DOCID_NUM - params->start;
	}

	//DLLCALL(so, query_work_pre, query, info, info->params.squery);
	query = query_work_pre(info, info->params.squery);
	info->params.query = ac_get_query(info, query, strlen(query));
	
	if(filter && !(info->params.nofilter & 0x1)){
		int i, dict = 0;
		int filter_count = GlobalConfSingleton::GetInstance().getAcConf().global.filter_count;
		for(i = 0; i < filter_count; i ++){
			//DLLCALL(so, query_filter_pre, query, info, i, &dict);
			query = query_filter_pre(info, i ,&dict);
			if(query == NULL || *query == 0){
				continue;
			}
			//err_out("query_filter_pre:%s.", query);
			if(dict == 0){
				dict = info->conf->filter_full;
			}
			//info->result.filter_flag = filter_keyword(info->conf->filter, query, dict);
			//DLLCALL(so_filter, ac_filter_keyword, info->result.filter_flag, query);
			info->result.filter_flag = ac_filter_keyword(query);
			if(info->result.filter_flag == 1){
				//DLLCALL(so, query_filter_after, query, info);
				query = query_filter_after(info);
				if(query){
					info->params.query = ac_get_query(info, query, strlen(query));
				}
			}else if(info->result.filter_flag == 2){
				break;
			}
		}
	}

    char q[MAX_LOG_LEN/3];
    info->parameters.toString(q, sizeof(q));
	AC_WRITE_LOG(info, "%s %lu %d-%d '%s' flag:%d", params->sid, params->uid,
		params->start, params->num, q, params->flag);
    SLOG_TRACE("[seqid:%u] @@analysis@@ query is %s %lu %d-%d '%s'",
               info->seqid, params->sid, params->uid,
		params->start, params->num, params->query);
	if(info->result.filter_flag == 2){
		AC_WRITE_LOG(info, "\tfiltered");
		return -1;
	}

    if (get_whole_cache(info) == AC_SUCC) {
        info->result.type_incache = PAGE_CACHE;
        (*step) = AC_STEP_DC;
        AC_WRITE_LOG(info, "{REQUEST step:%s}", PSTEP(*step));
        return AC_SUCC;
    }
    
	if (
        ( (strcmp(info->params.sid, "t_search") == 0)
          || (strcmp(info->params.sid, "t_wap_ios")==0)
          || (strcmp(info->params.sid, "t_wap_android")==0)
          || (strcmp(info->params.sid, "t_wap_c")==0) )
        && get_page_cache(info) == 0){
            info->result.type_incache = PAGE_CACHE;
            SLOG_TRACE("[seqid:%u] %s page cache hitted", info->seqid, info->params.squery);
			/**< because this assign used in done_all, after done_all, the step added 1 in frame, so
			 AC_STEP_DC less than real STEP_DC,but after call_request(), the step has no added 1 */
            (*step) = AC_STEP_DC;
            AC_WRITE_LOG(info, "{request: step:%s}", PSTEP(*step));
            return AC_SUCC;
	}else if (get_disk_cache(info) == AC_SUCC){
		// disk cache hitted, only search mem
		info->result.type_incache = DISK_CACHE;
		//DLLCALL(so_dispatch, ac_dispatch_mem, ret, info);
		int ret = ac_dispatch_mem(info);
		if (ret == 0){
			ac_bc_search(info, step);
		}else {
			(*step) = AC_STEP_DC;
		}
		SLOG_TRACE("[seqid:%u] %s disk cache hitted",info->seqid, info->params.squery);
    }else{
		ac_bc_search(info, step);
	}
    AC_WRITE_LOG(info, "{request: step:%s}", PSTEP(*step));
	return 0;

LB_ERROR:
    AC_WRITE_LOG(info, "{request: ERROR step:%d}", *step);
	return -1;
}

__STATIC__ int ac_output_json_cluster(QsInfo *info, JsonText_t *json_text, char *buffer, int maxlen){
     if(info->bcinfo.bcCluster_count == 0) {
          SLOG_DEBUG("[seqid:%d] there is no cluster", info->seqid);
          return 0;
     }

     JsonText_t * json_result;
     JsonText_t * first_level_result;
     json_result = CreateJsonText(10000);
     first_level_result = CreateJsonText(10000);

     int lastvalue = -1;
     int i;
     for (i = 0 ; i < info->bcinfo.bcCluster_count; i++) {
          if (info->bcinfo.pBcClusters[i] == NULL) {
               SLOG_WARNING("[seqid:%d] the %d then cluster is NULL", info->seqid, i);
               return -1;
          }

          if (lastvalue >= 0 && (int)info->bcinfo.pBcClusters[i]->val != lastvalue) {
               AddArrayValue(json_result, first_level_result->text, V_PAIR_ARRAY);
               FreeJsonText(first_level_result);
               first_level_result = NULL;
               first_level_result = CreateJsonText(10000);

          }
          lastvalue = info->bcinfo.pBcClusters[i]->val;
          char * p = buffer;
          int len = sprintf(buffer, "%d", (int32_t)info->bcinfo.pBcClusters[i]->fid);
          p = p + len + 1;
          sprintf(p, "%d", (int32_t)info->bcinfo.pBcClusters[i]->count);
          AddPairValue(first_level_result, buffer, p, V_VALUE_STR);

     }
     AddArrayValue(json_result, first_level_result->text, V_PAIR_ARRAY);
     FreeJsonText(first_level_result);
     //AddPairValue(json_text, "cluster", json_result->text, V_VALUE_ARRAY);
     FreeJsonText(json_result);
     return 0;
}

static int set_bc_doc_json(QsInfo *info, SrmDocInfo *doc, JsonText_t *json_tmp, char* buffer){
	uint8_t *ids = info->conf->field_ids;
	uint64_t res[BS_FIELD_NUM];
	char tmp[256];
	int i;
	MField *mfield;	FIELD_GETS(doc, res);
	for(i = 0; i < BS_FIELD_NUM; i ++){
		mfield = info->conf->field_info.fields + ids[i];
		if(mfield->status == 0){
			continue;
		}
        if(conf_get_int(g_server->conf_current, "only_show_mid", 0) &&
           (conf_get_int(g_server->conf_current, "print_ac_log", 1) == 0 ||
            info->parameters.print_more_aclog == 0)) {
            continue;
        }
		sprintf(tmp, "%lu", res[i]);
		AddPairValue(json_tmp, mfield->label, tmp, V_STR);
	}

    AcExtDataParser attr(doc);
    uint64_t fwnum = attr.get_fwnum();
    uint64_t dup = attr.get_dup();
    //uint64_t cont_sign = get_cont_sign();

	if (doc->debug || doc->ac_debug[0] != '\0'){
			sprintf(buffer, "bc:[%s] ac:[%s]", doc->debug ? doc->debug : "", doc->ac_debug);
		AddPairValue(json_tmp, "debug", buffer, V_STR);
	}

    if(conf_get_int(g_server->conf_current, "only_show_mid", 0) &&
       (conf_get_int(g_server->conf_current, "print_ac_log", 1) == 0 ||
        info->parameters.print_more_aclog == 0)) {
        return 0;
    }
    sprintf(tmp, "%lu", fwnum);
    AddPairValue(json_tmp, "doc_fwnum", tmp, V_STR);
    sprintf(tmp, "%lu", dup);
    AddPairValue(json_tmp, "doc_dup", tmp, V_STR);
    
    AddPairValue(json_tmp, "doc_category", (char*)doc->getCategoryAsStr().c_str(), V_STR);
 
	return 0;
}

static int srm_add_hothit(QsInfo *info, char midstr[], SrmDocInfo *doc_info)
{
    DIDocHead *doc_head = doc_info->doc_head;
    char *doc_content = (char *)((DIFieldHead *)(doc_head + 1) + doc_head->fieldnum);
    dm_dict_t *dm = (dm_dict_t *)ddm_get(g_server->ddm, "hot_dict");
    dm_pack_t *ppack = NULL;
    dm_lemma_t *lemma = NULL;
    JsonText_t *json_tags = NULL;
    uint32_t i;
    int j;

    char *begin, *end;
    int len;
    int add_num;
    int add_flag;
    char tags[2][MAX_TAGSTR];

    if (info->ppack == NULL)
        info->ppack = dm_pack_create(10000);
    ppack = info->ppack;

    if (dm == NULL || ppack == NULL || dm_search(dm, ppack, doc_content, doc_head->doclen, DM_OUT_FMM, DM_CHARSET_UTF8) != 0)
        return -1;

    if (ppack->ppseg_cnt > 0){
        if (info->tags[0].num == 0){
            info->tags[0].str[0] = '\0';

            if (info->params.url != NULL){
                len = strlen("key=");
                begin = strstr(info->params.url, "key=");
                if (begin == NULL){
                    len = strlen("key=");
                    begin = strstr(info->params.url, "query=");
                    if (begin == NULL){
                        return 0;
                    }
                }
                begin += len;
                end = strchr(begin, '&');
                if (end != NULL){
                    *end = '\0';
                }
                strncpy(info->tags[0].str, begin, MAX_TAGSTR - 1);
                if (end != NULL){
                    *end = '&';
                }
            }
            info->tags[0].num = 2;
        }

        for (i = 0, add_num = 0; i < ppack->ppseg_cnt && add_num < 2; i++){
            add_flag = 0;
            lemma = ppack->ppseg[i];
            for (j = 0; j < add_num; j++){
                if (strcmp(tags[j], lemma->pstr) == 0){
                    break;
                }
            }
            if (j < add_num){
                continue;
            }

            for (j = 0; j < MAX_HOTTAG; j++){
                if (info->tags[j].num == 0){
                    strncpy(info->tags[j].str, lemma->pstr, MAX_TAGSTR - 1);
                    info->tags[j].num++;
                    strncpy(tags[add_num++], lemma->pstr, MAX_TAGSTR - 1);
                    add_flag = 1;
                    break;
                }

                if (strcmp(info->tags[j].str, lemma->pstr) == 0){
                    if (info->tags[j].num < 2){
                        info->tags[j].num++;
                        strncpy(tags[add_num++], lemma->pstr, MAX_TAGSTR - 1);
                        add_flag = 1;
                    }
                    break;
                }
            }

            if (add_flag == 1){
                if (json_tags == NULL){
                    json_tags = CreateJsonText(10000);
                }
                AddArrayValue(json_tags, lemma->pstr, V_STR);
            }
        }
    }

    if (json_tags != NULL){
        if (info->json_hothit == NULL){
            info->json_hothit = CreateJsonText(10000);
        }
        if (info->json_hothit) {  
            AddPairValue(info->json_hothit, midstr, json_tags->text, V_PAIR_ARRAY);
        } else {
            SLOG_WARNING("[seqid:%u] CreateJsonText error", info->seqid);
        }
        
        
        FreeJsonText(json_tags);
    }

    return 0;
}

static int doc_add_cluster_info(QsInfo *info, SrmDocInfo *doc, JsonText_t *json_result, std::string *output, std::set<uint64_t> &uid_set, int &cluster_num) {
    JsonText_t *json_cluster = CreateJsonText(10000);
    if (!json_cluster) {
        SLOG_FATAL("can not alloc json_result!");
        suicide();
    }
    JsonText_t *json_mids = CreateJsonText(10000);
    if (!json_mids) {
        SLOG_FATAL("can not alloc json_result!");
        suicide();
    }
    for (int i = 0; i < CLUSTER_INFO_CACHE_MID_COUNT; ++i) {
        JsonText_t *json_cluster_tmp = CreateJsonText(10000);
        if (!json_cluster_tmp) {
            SLOG_FATAL("can not alloc json_result!");
            suicide();
        }
		if (doc->cluster_info.mid[i] > 0)
		{
			cluster_num += 1;
			if (doc->cluster_info.uid[i] > 0)
			{
				uid_set.insert(doc->cluster_info.uid[i]);
			}
		}
        char tmp[64];
        snprintf(tmp, sizeof(tmp), "%lu", doc->cluster_info.mid[i]);
        AddPairValue(json_cluster_tmp, "docid", tmp, V_STR);
        /*
        if(info->parameters.debug > PRINT_CLUSTER_LOG_LELVE) {
            AC_WRITE_LOG(info, " [cluster,output:%lld]", doc->cluster_info.mid[i]);
        }
        if(info->parameters.debug > PRINT_CLUSTER_LOG_LELVE) {
            snprintf(tmp, sizeof(tmp), "%llu", doc->cluster_info.cont_sign[i]);
            AddPairValue(json_cluster_tmp, "cont_sign", tmp, V_STR);
        }
        */
        AddArrayValue(json_mids, json_cluster_tmp->text, V_PAIR_ARRAY);
        FreeJsonText(json_cluster_tmp);
    }
    char tmp[64];
    snprintf(tmp, sizeof(tmp), "%u", doc->cluster_info.time);
    AddPairValue(json_cluster, "time", tmp, V_STR);
    output->append("c:");
    output->append(tmp);
    output->append(",");
    AddPairValue(json_cluster, "mids", json_mids->text, V_VALUE_ARRAY);
    AddPairValue(json_result, "cluster_info", json_cluster->text, V_VALUE_ARRAY);
    FreeJsonText(json_cluster);
    FreeJsonText(json_mids);
	return 0;
}

static int doc_add_socialhead(QsInfo *info, SrmDocInfo *doc, JsonText_t *json_result, std::string *output, std::set<uint64_t> & uid_set, const int cluster_num, bool is_concerned) {
	// social以前旧字段的保留social_action_info
	if (info->parameters.getXsort() == std::string("social") && !is_concerned && (doc->social_head_output || doc->famous_concern_info.person_info[0].time))
	{
		JsonText_t *json_social_head = CreateJsonText(10000);
		if (!json_social_head) {
			SLOG_FATAL("can not alloc json_result!");
			suicide();
		}
		JsonText_t *json_action = CreateJsonText(10000);
		if (!json_action) {
			SLOG_FATAL("can not alloc json_result!");
			suicide();
		}
		if (SOCIAL_ACTION_HEAD_PERSON == doc->social_head_output) {
			// now only send index 0
			for (int i = 0; i < 1; ++i) {
				if (doc->social_head.person[i].time) {
					JsonText_t *json_action_tmp = CreateJsonText(10000);
					if (!json_action_tmp) {
						SLOG_FATAL("can not alloc json_result!");
						suicide();
					}
					char tmp[64];
					snprintf(tmp, sizeof(tmp), "%lu", doc->social_head.person[i].time);
					AddPairValue(json_action_tmp, "time", tmp, V_STR);
					output->append("t:");
					output->append(tmp);
					output->append(",");
					snprintf(tmp, sizeof(tmp), "%lu", doc->social_head.person[i].action_state);
					AddPairValue(json_action_tmp, "type", tmp, V_STR);
					snprintf(tmp, sizeof(tmp), "%llu", doc->social_head.person[i].uid);
					AddPairValue(json_action_tmp, "uid", tmp, V_STR);
					AddArrayValue(json_action, json_action_tmp->text, V_PAIR_ARRAY);
					FreeJsonText(json_action_tmp);
				}
			}
		} else if (SOCIAL_ACTION_HEAD_COLONIAL == doc->social_head_output) {
			JsonText_t *json_action_tmp = CreateJsonText(10000);
			if (!json_action_tmp) {
				SLOG_FATAL("can not alloc json_result!");
				suicide();
			}
			char tmp[64];
			snprintf(tmp, sizeof(tmp), "%lu", doc->social_head.colonialinfo.colonial_time);
			AddPairValue(json_action_tmp, "time", tmp, V_STR);
			output->append("t:");
			output->append(tmp);
			output->append(",");
			snprintf(tmp, sizeof(tmp), "%lu", doc->social_head.colonialinfo.colonial_type);
			AddPairValue(json_action_tmp, "type", tmp, V_STR);
			snprintf(tmp, sizeof(tmp), "%lu", doc->social_head.colonialinfo.colonial_num);
			AddPairValue(json_action_tmp, "count", tmp, V_STR);
			AddArrayValue(json_action, json_action_tmp->text, V_PAIR_ARRAY);
			FreeJsonText(json_action_tmp);
		} else if (SOCIAL_ACTION_HEAD_FOLLOWS == doc->social_head_output) {
			JsonText_t *json_action_tmp = CreateJsonText(10000);
			if (!json_action_tmp) {
				SLOG_FATAL("can not alloc json_result!");
				suicide();
			}
			char tmp[64];
			snprintf(tmp, sizeof(tmp), "%lu", doc->social_head.person[0].time);
			AddPairValue(json_action_tmp, "time", tmp, V_STR);
			output->append("t:");
			output->append(tmp);
			output->append(",");
			snprintf(tmp, sizeof(tmp), "%lu", doc->social_head.person[0].action_state);
			AddPairValue(json_action_tmp, "type", tmp, V_STR);
			snprintf(tmp, sizeof(tmp), "%lu", doc->social_head.person[0].uid);
			AddPairValue(json_action_tmp, "uid", tmp, V_STR);
			AddArrayValue(json_action, json_action_tmp->text, V_PAIR_ARRAY);
			FreeJsonText(json_action_tmp);
		}
		char tmp[64];
		snprintf(tmp, sizeof(tmp), "%lu", doc->social_head_output);
		AddPairValue(json_social_head, "cat", tmp, V_STR);
		AddPairValue(json_social_head, "action", json_action->text, V_VALUE_ARRAY);
		AddPairValue(json_result, "social_action_info", json_social_head->text, V_STR);
		FreeJsonText(json_social_head);
		FreeJsonText(json_action);
	}
	// 新的字段social_time_info
	if ((info->parameters.getXsort() == std::string("social") || info->parameters.getXsort() == std::string("hot")) && !is_concerned && (doc->social_head_output || doc->famous_concern_info.person_info[0].time))
	{
		JsonText_t *json_social_head = CreateJsonText(10000);
		if (!json_social_head) {
			SLOG_FATAL("can not alloc json_result!");
			suicide();
		}
		JsonText_t *json_time = CreateJsonText(10000);
		if (!json_time) {
			SLOG_FATAL("can not alloc json_result!");
			suicide();
		}
		if (doc->social_head_output) {
			JsonText_t *json_action = CreateJsonText(10000);
			if (!json_action) {
				SLOG_FATAL("can not alloc json_result!");
				suicide();
			}
			if (SOCIAL_ACTION_HEAD_PERSON == doc->social_head_output) {
				// now only send index 0
				for (int i = 0; i < 1; ++i) {
					if (doc->social_head.person[i].time) {
						JsonText_t *json_action_tmp = CreateJsonText(10000);
						if (!json_action_tmp) {
							SLOG_FATAL("can not alloc json_result!");
							suicide();
						}
						char tmp[64];
						snprintf(tmp, sizeof(tmp), "%u", doc->social_head.person[i].time);
						AddPairValue(json_action_tmp, "time", tmp, V_STR);
						output->append("t:");
						output->append(tmp);
						output->append(",");
						snprintf(tmp, sizeof(tmp), "%lu", doc->social_head.person[i].action_state);
						AddPairValue(json_action_tmp, "type", tmp, V_STR);
						snprintf(tmp, sizeof(tmp), "%lu", doc->social_head.person[i].uid);
						AddPairValue(json_action_tmp, "uid", tmp, V_STR);
						AddArrayValue(json_action, json_action_tmp->text, V_PAIR_ARRAY);
						FreeJsonText(json_action_tmp);
					}
				}
			} else if (SOCIAL_ACTION_HEAD_COLONIAL == doc->social_head_output) {
				JsonText_t *json_action_tmp = CreateJsonText(10000);
				if (!json_action_tmp) {
					SLOG_FATAL("can not alloc json_result!");
					suicide();
				}
				char tmp[64];
				snprintf(tmp, sizeof(tmp), "%u", doc->social_head.colonialinfo.colonial_time);
				AddPairValue(json_action_tmp, "time", tmp, V_STR);
				output->append("t:");
				output->append(tmp);
				output->append(",");
				snprintf(tmp, sizeof(tmp), "%u", doc->social_head.colonialinfo.colonial_type);
				AddPairValue(json_action_tmp, "type", tmp, V_STR);
				snprintf(tmp, sizeof(tmp), "%u", doc->social_head.colonialinfo.colonial_num);
				AddPairValue(json_action_tmp, "count", tmp, V_STR);
				AddArrayValue(json_action, json_action_tmp->text, V_PAIR_ARRAY);
				FreeJsonText(json_action_tmp);
			} else if (SOCIAL_ACTION_HEAD_FOLLOWS == doc->social_head_output) {
				JsonText_t *json_action_tmp = CreateJsonText(10000);
				if (!json_action_tmp) {
					SLOG_FATAL("can not alloc json_result!");
					suicide();
				}
				char tmp[64];
				snprintf(tmp, sizeof(tmp), "%u", doc->social_head.person[0].time);
				AddPairValue(json_action_tmp, "time", tmp, V_STR);
				output->append("t:");
				output->append(tmp);
				output->append(",");
				snprintf(tmp, sizeof(tmp), "%lu", doc->social_head.person[0].action_state);
				AddPairValue(json_action_tmp, "type", tmp, V_STR);
				snprintf(tmp, sizeof(tmp), "%lu", doc->social_head.person[0].uid);
				AddPairValue(json_action_tmp, "uid", tmp, V_STR);
				AddArrayValue(json_action, json_action_tmp->text, V_PAIR_ARRAY);
				FreeJsonText(json_action_tmp);
			}
			char tmp[64];
			snprintf(tmp, sizeof(tmp), "%u", doc->social_head_output);
			AddPairValue(json_time, "cat", tmp, V_STR);
			AddPairValue(json_time, "action", json_action->text, V_VALUE_ARRAY);
			FreeJsonText(json_action);
		}
		JsonText_t *json_uids = CreateJsonText(10000);
		if (!json_uids) {
			SLOG_FATAL("can not alloc json_result!");
			suicide();
		}
		if (doc->famous_concern_info.person_info[0].time) {
			AC_WRITE_LOG(info, " [docid:%lu, uids:", doc->docid);
			for (int i = 0; i < 5; i++)  {
				if (doc->famous_concern_info.person_info[i].time){
					JsonText_t *json_uids_tmp = CreateJsonText(10000);
					if (!json_uids_tmp) {
						SLOG_FATAL("can not alloc json_result!");
						suicide();
					}
					char tmp[64];
					snprintf(tmp, sizeof(tmp), "%u", doc->famous_concern_info.person_info[i].time);
					AddPairValue(json_uids_tmp, "time", tmp, V_STR);
					snprintf(tmp, sizeof(tmp), "%d", doc->famous_concern_info.person_info[i].type);
					AddPairValue(json_uids_tmp, "type", tmp, V_STR);
					snprintf(tmp, sizeof(tmp), "%lu", doc->famous_concern_info.person_info[i].uid);
					AddPairValue(json_uids_tmp, "uid", tmp, V_STR);
					snprintf(tmp, sizeof(tmp), "%d", doc->famous_concern_info.person_info[i].cat);
					AddPairValue(json_uids_tmp, "cat", tmp, V_STR);
					AddArrayValue(json_uids, json_uids_tmp->text, V_PAIR_ARRAY);
					FreeJsonText(json_uids_tmp);
					AC_WRITE_LOG(info, "uid:%lu, time:%u, type:%d, cat:%d;", doc->famous_concern_info.person_info[i].uid, doc->famous_concern_info.person_info[i].time, doc->famous_concern_info.person_info[i].type, doc->famous_concern_info.person_info[i].cat);
				}
			}
			AC_WRITE_LOG(info, "]");
		}
		if (doc->social_head_output)
			AddPairValue(json_social_head, "time", json_time->text, V_VALUE_ARRAY);
		if (doc->famous_concern_info.person_info[0].time)
			AddPairValue(json_social_head,"uids", json_uids->text, V_VALUE_ARRAY);
		AddPairValue(json_result, "social_time_info", json_social_head->text, V_STR);
		FreeJsonText(json_social_head);
		FreeJsonText(json_time);
		FreeJsonText(json_uids);
	}

	// 评论展示字段cluster_comment_info,不管是不是关注人都出
	if (info->parameters.getXsort() == std::string("social") || info->parameters.getXsort() == std::string("hot"))
	{
		if (cluster_num < CLUSTER_INFO_CACHE_MID_COUNT){
			//得到随机的comment_id
			std::vector<uint64_t> comment_id_rand;
			for (int i = 0; i < COMMENT_INFO_CACHE_MID_COUNT; i++) {
				if(doc->social_head.comment[i].comment_id > 0) {
					if (doc->social_head.comment[i].uid > 0) {
						if (uid_set.find(doc->social_head.comment[i].uid) == uid_set.end()){
							comment_id_rand.push_back(doc->social_head.comment[i].comment_id);
						}
					}
				}
			}
			if (comment_id_rand.size() > 0) {
				int comment_show_num = int_from_conf(COMMENT_SHOW_NUM, 2);  
				comment_show_num = (CLUSTER_INFO_CACHE_MID_COUNT - cluster_num) > comment_show_num ? comment_show_num : (CLUSTER_INFO_CACHE_MID_COUNT - cluster_num);
				std::vector<int> index;
				std::vector<int> comment_index;
				for (int i = 0; i < comment_id_rand.size(); i++) {
					comment_index.push_back(i);
				}
				for (int i = 0; i < comment_show_num && i < comment_id_rand.size(); i++) {
					if (comment_index.size() > 0) {
						int cur_index = rand() % comment_index.size();	
						index.push_back(comment_index[cur_index]);
						std::vector<int>::iterator it = comment_index.begin() + cur_index;
						comment_index.erase(it);
					}
				}
				JsonText_t *json_cids = CreateJsonText(10000);
				if (!json_cids) {
					SLOG_FATAL("can not alloc json_result!");
					suicide();
				}
				JsonText_t *json_cluster_comment = CreateJsonText(10000);
				if (!json_cluster_comment) {
					SLOG_FATAL("can not alloc json_result!");
					suicide();
				}
				// for ac log trace
				bool ac_log_flag = (info->parameters.getXsort() == std::string("hot")) ? true : false;
				if (index.size() > 0 && ac_log_flag) {
					AC_WRITE_LOG(info, "[mid:%ld,cid:", doc->docid);
				}
				for (int i = 0; i < index.size(); i++) {
					if (i != index.size() - 1 && ac_log_flag) {
						AC_WRITE_LOG(info, "%ld-", comment_id_rand[index[i]]);
					} else if (ac_log_flag) {
						AC_WRITE_LOG(info, "%ld", comment_id_rand[index[i]]);
					}
					JsonText_t *json_comment_tmp = CreateJsonText(10000);
					if (!json_comment_tmp) {
						SLOG_FATAL("can not alloc json_result!");
						suicide();
					}
					uint64_t show_comment_id = comment_id_rand[index[i]];
					char tmp[64];
					snprintf(tmp, sizeof(tmp), "%lu", show_comment_id);
					AddPairValue(json_comment_tmp, "docid", tmp, V_STR);
					AddArrayValue(json_cids, json_comment_tmp->text, V_PAIR_ARRAY);
					FreeJsonText(json_comment_tmp);
				}
				if (index.size() > 0 && ac_log_flag) {
					AC_WRITE_LOG(info,"]");
				}
				AddPairValue(json_cluster_comment,"cids", json_cids->text, V_VALUE_ARRAY);
				AddPairValue(json_result, "cluster_comment_info", json_cluster_comment->text, V_VALUE_ARRAY);
				FreeJsonText(json_cluster_comment);
				FreeJsonText(json_cids);
			}
		}
	}
	return 0;
}
// uid_set用于和转发聚合uid去重,cluster_num >= 3个时,不出评论, is_concerned是表示是否是关注人模块
static bool srm_add_social_json(QsInfo *info, SrmDocInfo *doc, JsonText_t *json_result, std::string &output, std::set<uint64_t>& uid_set,const int cluster_num, bool is_concerned)
{
	if(info == NULL || doc == NULL)
		return false;
	if ((doc->social_head_output || doc->famous_concern_info.person_info[0].time || doc->social_head.comment[0].comment_id > 0) &&
			info->parameters.getUs() == 1 &&
			(info->parameters.getXsort() == std::string("social") || info->parameters.getXsort() == std::string("hot"))) {
		doc_add_socialhead(info, doc, json_result, &output, uid_set, cluster_num, is_concerned);
		return true;
	}
	return false;
}

static int srm_add_doc_json(QsInfo *info, SrmDocInfo *doc, JsonText_t *json_result, char *buffer, int maxlen,
		bool nodi = false){
    if (doc == NULL) {
        return -1;
    }
	JsonText_t *json_tmp;
	DIDocHead *doc_head;
	DIFieldHead *field_head, *field;
	MField *mfield;
	char *p, *v;
	int i, ret;

    char idstr[MAX_BUF_SIZE], midstr[MAX_BUF_SIZE], relstr[MAX_BUF_SIZE];
    int id_len = 0, mid_len = 0, rel_len = 0;
    UID_T id;
    JsonText_t *json_comment;

	json_tmp = CreateJsonText(10000);

    bool already_set_bc = false;
    if (doc->type == SRMDOC_NORMAL){
		if (!nodi || info->parameters.debug ||
            (info->parameters.getUs() && conf_get_int(g_server->conf_current, "us_set_bc_doc_json", 1))) {
			set_bc_doc_json(info, doc, json_tmp, buffer);
            already_set_bc = true;
		}
    }
	

	if(doc->doc_head){	//ժҪ????
		std::string output;
		std::set<uint64_t> uid_nouse;
		srm_add_social_json(info, doc, json_tmp, output, uid_nouse, 0, false);
		doc_head = doc->doc_head;
		field_head = (DIFieldHead*)(doc_head + 1);
		p = (char*)(field_head + doc_head->fieldnum);
		for(i = 0; i < doc_head->fieldnum; i ++){
			field = field_head + i;
			if(field->type == 0){
				field->type = TYPE_CHAR;
			}
			mfield = info->conf->field_info.fields + field->id;
			if(mfield->status == 0){
				//err_out("fieldid not exists %d", field->id);
				continue;
			}
			if(field->pos >= doc_head->doclen){
				//err_out("field error %d\t%d\t%d\n", field->id, field->pos, field->len);
				continue;
			}
			if(field->type == TYPE_INT && doc->type == SRMDOC_NORMAL){
				sprintf(buffer, "%u", *(uint32_t*)(p + field->pos));
				AddPairValue(json_tmp, mfield->label, buffer, V_STR);
			}else{
				memmove(buffer, p + field->pos, field->len);
				buffer[field->len] = 0;
				if (strcmp(mfield->label, "ID") == 0 ) {
				    trim_char(buffer, strlen(buffer), "\"", 1);
				}
				if (doc->type == SRMDOC_NORMAL){
				    if(AC_NEED_MARK(info, field->id)){
					v = buffer + field->len + 1;
					mark_red(buffer, info->dainfo.words_snap, v);
				    }else v = buffer;

				    AddPairValue(json_tmp, mfield->label, v, V_STR);
				}

				if (strcmp(mfield->label, "UID") == 0){
				    id_len = field->len < MAX_BUF_SIZE - 1 ? field->len : MAX_BUF_SIZE - 1;
				    strncpy(idstr, buffer, id_len);
				    idstr[id_len] = '\0';
				}else if (strcmp(mfield->label, "ID") == 0){
				    mid_len = field->len < MAX_BUF_SIZE - 1 ? field->len : MAX_BUF_SIZE - 1;
				    strncpy(midstr, buffer, mid_len);
				    midstr[mid_len] = '\0';
				}
			}
		}
	}else if(nodi){
        if(conf_get_int(g_server->conf_current, "only_show_mid", 0) &&
           (conf_get_int(g_server->conf_current, "print_ac_log", 1) == 0 ||
            info->parameters.print_more_aclog == 0)) {
            char temp[64];
            snprintf(temp, sizeof(temp), "%lu", FIELD_GET_DOCID(doc));
            AddPairValue(json_tmp, "ID", temp, V_STR);
            snprintf(temp, sizeof(temp), "%lu", FIELD_GET_RELATION(doc));
            AddPairValue(json_tmp, "doc_rel", temp, V_STR);
        } else {
            char temp[64];
            snprintf(temp, sizeof(temp), "%lu", FIELD_GET_DOCID(doc));
            AddPairValue(json_tmp, "ID", temp, V_STR);

            if (info->parameters.debug ||
                (conf_get_int(g_server->conf_current, "print_ac_log", 1) &&
                 info->parameters.print_more_aclog)) {
                if (!already_set_bc) {
                    set_bc_doc_json(info, doc, json_tmp, buffer);
                }
                AcExtDataParser attr(doc);
                snprintf(temp, sizeof(temp), "%lu", attr.get_uint64_1());
                AddPairValue(json_tmp, "uint64_1", temp, V_STR);
                snprintf(temp, sizeof(temp), "%lu", attr.get_uint64_2());
                AddPairValue(json_tmp, "uint64_2", temp, V_STR);
                snprintf(temp, sizeof(temp), "%lu", attr.get_uint64_3());
                AddPairValue(json_tmp, "uint64_3", temp, V_STR);
                snprintf(temp, sizeof(temp), "%lu", attr.get_uint64_4());
                AddPairValue(json_tmp, "uint64_4", temp, V_STR);
                snprintf(temp, sizeof(temp), "%u", doc->validfans);
                AddPairValue(json_tmp, "vfans", temp, V_STR);
            }
        }
	
		//social time info and cluster repost result
		bool social = false, cluster = false;
		std::string output;
		std::set<uint64_t> uid_dup_set;
		int cluster_midnum = 0;
		// 评论uid需要和转发聚合uid去重
		if (doc->cluster_info.time &&
				info->parameters.getXsort() == std::string("social") &&
				info->parameters.getUs() == 1 &&
				info->parameters.cluster_repost == 1)
		{
			doc_add_cluster_info(info, doc, json_tmp, &output, uid_dup_set, cluster_midnum);
			cluster = true;
		}
		if (srm_add_social_json(info, doc, json_tmp, output, uid_dup_set, cluster_midnum, false)) 
		{
			social = true;
		}
		//output more log
        /*
		if (conf_get_int(g_server->conf_current, "output_show_more_log", 1) &&
				info->parameters.getUs() == 1 &&
				info->parameters.getXsort() == std::string("social") &&
				info->params.num <= 20 &&
				info->parameters.nofilter == 0) {
            AcExtDataParser attr(doc);
            uint64_t user_type = attr.get_user_type();
            int degree = ((doc->filter_flag)>>20) & 0xF;
			//AC_WRITE_LOG(info, " [:%lu,T:%d,S:%d,C:%d,%s,D:%d,%lu-%lu-%lu]",
			//		doc->docid, doc->rank, social, cluster, output.c_str(),
             //       degree, attr.get_fwnum(), attr.get_cmtnum(), attr.get_likenum());
			AC_WRITE_LOG(info, " [:%lu,S:%d,C:%d,D:%d,%lu-%lu-%lu-%lu]",
					doc->docid, social, cluster, degree,
                    attr.get_fwnum(), attr.get_cmtnum(), attr.get_likenum(), user_type);
		}
        */

	}else if(doc->type == SRMDOC_NORMAL && (info->params.flag & QSPARAMS_FLAG_DOCID)){
		set_bc_doc_json(info, doc, json_tmp, buffer);
	}

	if (info->parameters.debug) {
		char debug_buf[1024];
		debug_buf[0] = '\0';
		get_doc_detail(doc, debug_buf, sizeof(debug_buf));
		AddPairValue(json_tmp, "doc_detail", debug_buf, V_STR);
	}

	if (json_tmp->curLen || doc->type == SRMDOC_RELATION){
		if (json_tmp->curLen){
			AddArrayValue(json_result, json_tmp->text, V_PAIR_ARRAY);
        }
		ret = 0;

        if (doc->doc_head && (strcmp(info->params.sid, "t_search") == 0 || 
					strcmp(info->params.sid, "t_search_nologin") == 0)){
            if (doc->type == SRMDOC_NORMAL){
                srm_add_hothit(info, midstr, doc);
            }else if (doc->type == SRMDOC_RELATION){
                id = strtol(idstr, NULL, 10);
                for (i = 0; i < info->id_num && i < MAX_EXTRA_DOC_NUM; i++){
                    if (info->ids[i] == id){
                        break;
                    }
                }
                if (i == info->id_num){
                    info->ids[info->id_num++] = id;
                    json_comment = CreateJsonText(10000);

                    AddPairValue(json_comment, "id", idstr, V_PAIR_ARRAY);
                    AddPairValue(json_comment, "mid", midstr, V_PAIR_ARRAY);

                    rel_len = snprintf(relstr, MAX_BUF_SIZE - 1, "%d", doc->relation);
                    relstr[rel_len] = '\0';
                    AddPairValue(json_comment, "relation", relstr, V_PAIR_ARRAY);

                    if (info->json_comments == NULL){
                        info->json_comments = CreateJsonText(10000);
                    }
                    AddArrayValue(info->json_comments, json_comment->text, V_PAIR_ARRAY);

                    FreeJsonText(json_comment);
                }
            }
        }
	}else{
		ret = -1;
	}
	FreeJsonText(json_tmp);
	return ret;
}

static int GetRankType(QsInfo *info) {
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

static int HotAdjust(QsInfo *info, JsonText_t *json_result, bool fake_dc) {
	int ranktype = GetRankType(info);
    if (ranktype != RANK_HOT) {
        return 0;
    }
    if (info->params.start > 0 ||
        info->dcinfo.recv_num > 0 ||
        !fake_dc) {
        /*
		if (info->parameters.debug) {
            AC_WRITE_LOG(info, "\n[return 0, %d:%d:%d:%d]",
                        info->params.start, info->dcinfo.recv_num,
                        info->dcinfo.docnum, !fake_dc);
        }
        */
        return 0;
    }
	char *query = (char *) info->parameters.getKey().c_str();
	if (query == NULL) {
		if (info->parameters.debug) {
            AC_WRITE_LOG(info, "\n[query return 0]");
        }
		return 0;
    }
	Query2ClassDict *dict = Resource::Reference<Query2ClassDict>("query_class.dict");
	if (dict == NULL) {
		if (info->parameters.debug) {
			AC_WRITE_LOG(info, "\n[AcInterventionAdd dict==NULL]");
		}
		return 0;
	}
	if (info->parameters.debug) {
		AC_WRITE_LOG(info, "\n[ac_unset_default::num:%d, start:%d]",
                    info->bcinfo.get_doc_num(), info->params.start);
	}
	if (info->parameters.debug) {
		AC_WRITE_LOG(info, "\n[ac_unset_default::num:%d, start:%d]",
                    info->bcinfo.get_doc_num(), info->params.start);
	}
    int manu_hot_max = conf_get_int(g_server->conf_current, "manu_hot_max", 3);
	uint64_t hot_adjust_docid_list[manu_hot_max + 1];
    int hot_adjust_num = 0;
	for (int i = 0; i < manu_hot_max; i++) {
        uint64_t adjust_docid = dict->find(query, i);
        if (info->parameters.debug) {
            AC_WRITE_LOG(info, "\n[manu_hot get_hot:%lu", adjust_docid);
        }
        hot_adjust_docid_list[i] = adjust_docid;
        if (adjust_docid) {
            ++hot_adjust_num;
        }
    }
	if (dict != NULL) {
		dict->UnReference();
	}
    if (!hot_adjust_num) {
		if (info->parameters.debug) {
            AC_WRITE_LOG(info, "\n[!hot_adjust_num]");
        }
        return 0;
    }
    for (int i = 0; i < manu_hot_max; ++i) {
        if (!hot_adjust_docid_list[i]) {
            continue;
        }
        JsonText_t *json_tmp;
        json_tmp = CreateJsonText(10000);
		char temp[64];
		snprintf(temp, sizeof(temp), "%lu", hot_adjust_docid_list[i]);
		AddPairValue(json_tmp, "ID", temp, V_STR);
		AddPairValue(json_tmp, "doc_id", temp, V_STR);
		AddPairValue(json_tmp, "doc_category", "adjust", V_STR);
        AC_WRITE_LOG(info, "\n[manu_hot add:%lu", hot_adjust_docid_list[i]);
        if (json_tmp->curLen){
            AddArrayValue(json_result, json_tmp->text, V_PAIR_ARRAY);
        }
        FreeJsonText(json_tmp);
    }
	return 0;
}

__STATIC__ int ac_output_json_result(QsInfo *info, JsonText_t *json_text, char *buffer, int maxlen){
	int fake_dc = conf_get_int(g_server->conf_current, "fake_dc", 0);
	JsonText_t *json_result = CreateJsonText(10000);
    if (!json_result) {
        SLOG_FATAL("can not alloc json_result!");
        suicide();
    }
 
	QsInfoDC *dcinfo = &info->dcinfo;
    const char * xsort = info->parameters.getXsort().c_str();
    int docnum = info->bcinfo.get_doc_num() - info->params.start;
    if (docnum >= info->params.num) {
        docnum = info->params.num;
    }

    if (docnum >= info->dcinfo.recv_num) {
        docnum = info->dcinfo.recv_num;
    }

    if ((info->params.flag & QSPARAMS_FLAG_DOCID) == 0 ) {
        if (docnum >= info->dcinfo.recv_num) {
            docnum = info->dcinfo.recv_num;
        }

        if (docnum >=  info->dcinfo.docnum - info->dcinfo.docstart) {
            docnum = info->dcinfo.docnum - info->dcinfo.docstart;
        }
    }

    if (docnum < 0) {
        docnum = 0;
    }

    if (std::string(xsort) == std::string("time")) {
        if (info->params.start + docnum < (int)info->bcinfo.get_doc_num()) {
            info->bcinfo.sort(info->params.start, info->params.start + docnum, time_cmp);
        }
    }

	const std::vector<SrmDocInfo*>& atresult_weibo = info->bcinfo.atresult_weibo;
	if (atresult_weibo.size()) {
		JsonText_t *json_at = CreateJsonText(10000);
		if (!json_at) {
			SLOG_FATAL("can not alloc json_result!");
			suicide();
		}

		std::vector<SrmDocInfo*>::const_iterator atresult_it;
		for (atresult_it = atresult_weibo.begin(); atresult_it != atresult_weibo.end(); atresult_it++) {
	        if (srm_add_doc_json(info, *atresult_it, json_at, buffer, maxlen, true) == 0) {
			}
		}

		if (json_at->curLen != 0) {
			AddPairValue(json_text, "at_weibo", json_at->text, V_PAIR_ARRAY);
		}

		FreeJsonText(json_at);
	}

	int count = 0;
	for (int i = info->params.start; i < (int)info->bcinfo.get_doc_num(); i ++) {
		if (count >= info->params.num || (!fake_dc && count >= info->dcinfo.recv_num)) {
			break;
		}

		if ((info->params.flag & QSPARAMS_FLAG_DOCID) == 0 &&
				((!fake_dc && count >= info->dcinfo.recv_num) || i >= dcinfo->docstart + dcinfo->docnum)){
			break;
		}

		if (srm_add_doc_json(info, info->bcinfo.get_doc_at(i), json_result, buffer, maxlen, fake_dc) == 0){
			count ++;
		}
	}
    HotAdjust(info, json_result, fake_dc);

    for (int i = 0; i < info->extra_doc_num && info->extra_docs[i] != NULL; i++) {
        srm_add_doc_json(info, info->extra_docs[i], json_result, buffer, maxlen, fake_dc);
    }

	if (json_result->curLen == 0){
		AddArrayValue(json_result, "", V_PAIR_ARRAY);
	}
	AddPairValue(json_text, "result", json_result->text, V_VALUE_ARRAY);

    if (info->parameters.debug) {
        JsonText_t *json_filtered = CreateJsonText(10000);
        if (!json_filtered) {
            SLOG_FATAL("can not alloc json_result!");
            suicide();
        }
        const std::vector<SrmDocInfo*>& docDeleds = *info->bcinfo.getDocDeleds();
        std::vector<SrmDocInfo*>::const_iterator deledsIt;
		for (deledsIt = docDeleds.begin(); deledsIt != docDeleds.end(); deledsIt++) {
			if (info->parameters.debug >= DEBUG_LEVEL_PLUG_DELETED_INFO ||
					(*deledsIt)->docid == info->parameters.debugid) {
				srm_add_doc_json(info, *deledsIt, json_filtered, buffer, maxlen, fake_dc);
			}
		}
        
        if (json_filtered->curLen == 0) {
			AddPairValue(json_filtered, "num",  (char*)safe_to_string((int)docDeleds.size()).c_str(), V_VALUE_STR);
        } else {
			AddPairValue(json_text, "result_filted", json_filtered->text, V_VALUE_ARRAY);
		}
		FreeJsonText(json_filtered);
    }
    FreeJsonText(json_result);
        
	return 0;
}

static int ac_output_json(QsInfo *info, char *output, int maxlen){
	JsonText_t *json_ext = CreateJsonText(100000);
	JsonText_t *json_tmp = CreateJsonText(100000);
	assert(json_ext != NULL && json_tmp != NULL);
	ac_output_json_global(info, json_tmp, output, maxlen, true);
    ac_output_json_cluster(info, json_tmp, output, maxlen);
	ac_output_json_result(info, json_tmp, output, maxlen);

    // 热门搜索词
    if (info->json_hothit != NULL){
        AddPairValue(json_ext, "hot_hit", info->json_hothit->text, V_PAIR_ARRAY);
    }

    // 口碑类评论(提前到最前面的两条)
    if ((info->intention & INTENTION_COMMENT) != 0){
        if (info->json_comments != NULL){
            AddPairValue(json_ext, "friend_comments", info->json_comments->text, V_PAIR_ARRAY);
        } else {
            AddPairValue(json_ext, "friend_comments", "\"no comments\"", V_PAIR_ARRAY);
        }
    }

    // 结构化数据筛选
    if (info->json_category != NULL) {
        AddPairValue(json_ext, "category", info->json_category->text, V_PAIR_ARRAY);
    }

    // 结构化数据重构
    // "social":{ 
    //    [{"title":"1", "info":[{"uid":"123","mid":"567","doc_rank":"140000"}]
    // }
    JsonText_t *json_socials = NULL;
    for (int i = 0; i < MAX_CATEGORY_NUM;i++) {
        const std::set<SrmDocInfo*, SrmDocInfoNewestRankComp>& social = info->bcinfo.socials[i];
        if (social.empty()) {
            continue;
        }
        JsonText_t * json_social = CreateJsonText(1000);
        char tmp_str[MAX_BUF_SIZE];
        snprintf(tmp_str, sizeof(tmp_str), "%d", i);
        AddPairValue(json_social, "title", tmp_str, V_STR);
        //"info":[{"uid":"123","mid":"567"}]
        JsonText_t * json_sinfo = NULL; 
        std::set<SrmDocInfo*, SrmDocInfoNewestRankComp>::reverse_iterator it = social.rbegin();
		int j = 0;
        for (; it != social.rend() && j < 4; ++it, ++j) {
            //{"uid":"123","mid":"567","doc_rank":"222"}
            JsonText_t * json_sivalue = CreateJsonText(10000);
            SrmDocInfo * doc = (*it);
            snprintf(tmp_str, sizeof(tmp_str), "%lu", GetDomain(doc));
            AddPairValue(json_sivalue, "uid", tmp_str, V_STR);
            snprintf(tmp_str, sizeof(tmp_str), "%u", doc->rank);
            AddPairValue(json_sivalue, "doc_rank", tmp_str, V_STR);
			snprintf(tmp_str, sizeof(tmp_str), "%lu", doc->docid);
            AddPairValue(json_sivalue, "mid", tmp_str, V_STR);
			std::set<uint64_t> set_nouse;
			std::string output;
			srm_add_social_json(info, doc, json_sivalue, output, set_nouse, 0, true);
            if (json_sinfo == NULL) {
                json_sinfo = CreateJsonText(10000);
            }
            AddArrayValue(json_sinfo, json_sivalue->text, V_PAIR_ARRAY);
            FreeJsonText(json_sivalue);
			json_sivalue = NULL;
        }
        if (json_sinfo != NULL) { 
            AddPairValue(json_social,"info", json_sinfo->text, V_VALUE_STR);
        }
        FreeJsonText(json_sinfo);
		json_sinfo = NULL;
        if (json_socials == NULL) {
            json_socials = CreateJsonText(10000);
        }
		
		AddArrayValue(json_socials, json_social->text, V_PAIR_ARRAY);
        FreeJsonText(json_social);
		json_social = NULL;
    }
    if (json_socials != NULL) {
        AddPairValue(json_ext, "socials", json_socials->text, V_VALUE_STR);
        FreeJsonText(json_socials); json_socials = NULL;
    }
    int vip_ac = conf_get_int(g_server->conf_current, "vip_ac", 0);
    if (vip_ac) {
        // media or famous
        if (info->media_user_cnt || info->famous_user_cnt || info->media_user_weibo_cnt || info->famous_user_weibo_cnt) {
            JsonText_t * json_media_cnt = CreateJsonText(10000);
            char tmp_str[MAX_BUF_SIZE];
            memset(tmp_str, 0, MAX_BUF_SIZE);
            snprintf(tmp_str, sizeof(tmp_str), "%u", info->media_user_cnt);
            AddPairValue(json_media_cnt, "media_user_cnt", tmp_str, V_PAIR_ARRAY);

            memset(tmp_str, 0, MAX_BUF_SIZE);
            snprintf(tmp_str, sizeof(tmp_str), "%u", info->famous_user_cnt);
            AddPairValue(json_media_cnt, "famous_user_cnt", tmp_str, V_PAIR_ARRAY);

            memset(tmp_str, 0, MAX_BUF_SIZE);
            snprintf(tmp_str, sizeof(tmp_str), "%u", info->media_user_weibo_cnt);
            AddPairValue(json_media_cnt, "media_user_weibo_cnt", tmp_str, V_PAIR_ARRAY);

            memset(tmp_str, 0, MAX_BUF_SIZE);
            snprintf(tmp_str, sizeof(tmp_str), "%u", info->famous_user_weibo_cnt);
            AddPairValue(json_media_cnt, "famous_user_weibo_cnt", tmp_str, V_PAIR_ARRAY);

            AddPairValue(json_ext, "media_and_famous", json_media_cnt->text, V_PAIR_ARRAY);
        /*
            AddArrayValue(json_ext, json_media_cnt->text, V_PAIR_ARRAY);
                AddPairValue(json_ext, "hot_hit", info->json_hothit->text, V_PAIR_ARRAY);
            AddPairValue(info->json_hothit, midstr, json_tags->text, V_PAIR_ARRAY);
        */

            FreeJsonText(json_media_cnt);
            json_media_cnt = NULL;
        }
    }
    // 扩展信息赋值
    if (json_ext->curLen > 0){
        AddPairValue(json_tmp, "ext_info", json_ext->text, V_PAIR_ARRAY);
    }
	JsonText_t *json_sp = CreateJsonText(100000);
	AddPairValue(json_sp, "sp", json_tmp->text, V_PAIR_ARRAY);
	FreeJsonText(json_tmp);
	memmove(output, json_sp->text, json_sp->curLen);
	int len = json_sp->curLen;
	FreeJsonText(json_sp);
    FreeJsonText(json_ext);
	return len;
}

int TCallBack::call_response(void *data, char *output, int maxlen) {
	QsInfo *info = (QsInfo*) data;
	QsInfoDC *dcinfo;
	QsInfoBC *bcinfo;
	QsInfoResult *result;
	int ret;

	info->buffer = server_buffer(g_server->server, &info->buf_size);
	info->buf_pos = 0;

	//error
	if (info->result.start_time.tv_sec == 0) {
		return ac_output_json_error(info, output, maxlen);
	}

	bcinfo = (QsInfoBC*) &info->bcinfo;
	dcinfo = (QsInfoDC*) &info->dcinfo;
	result = (QsInfoResult*) &info->result;

	bcinfo->num_adjust -= info->result.filter_count;
	SLOG_DEBUG("bcinfo->num_adjust:%d(info->rsult.filter_count:%d)",
			bcinfo->num_adjust, info->result.filter_count);
	calc_sample(info);

	if (info->params.flag & QSPARAMS_FLAG_DOCID) {
		dcinfo->recv_num =
            (int)info->bcinfo.get_doc_num() > info->params.start ?
            info->bcinfo.get_doc_num() - info->params.start : 0;
	}

	//ret = ac_output(info, output, maxlen);
	ret = ac_output_json(info, output, maxlen);
	/*
	 // write 3s cache
	 set_whole(info, output, ret);
	 }
	 */
	info->result.time_span = calc_time_span(&info->result.start_time);
	AC_WRITE_LOG(info, "\ttotal:%f", info->result.time_span / 1000000.0);
    
	// count timeout

    if (info->timeoutCounter == NULL) {
        return ret;
    }
	// TODO 1.0 move to conf
    if (info->timeoutCounter->isValid()) {
    	if (info->result.time_span / 1000.0  >
    		 GlobalConfSingleton::GetInstance().getAcConf().global.press_threshhold_timeout) {
    		info->timeoutCounter->pushSid(info->params.sid, true);
    	} else {
    		info->timeoutCounter->pushSid(info->params.sid, false);
    	}
    }

	return ret;
}

int TCallBack::call_log(void *data, char *output, int maxlen) {
	QsInfo *info = (QsInfo*)data;
	QsInfoBC *bcinfo = &info->bcinfo;
	QsInfoDC *dcinfo = &info->dcinfo;
	QsDCInfo *dc;
	QsBCInfo *bc;
	int pos = 0, i;

	info->buffer = output;
	info->buf_size = maxlen;
	info->buf_pos = 0;

	//error
	if(info->result.start_time.tv_sec == 0){
		return -1;
	}

	for(i = 0; i < info->bc_num; i ++){
		bc = bcinfo->bc[i];
		while(bc){
			if(bc->status == (int)MAGIC_NUM_32){
				bc->time_span = calc_time_span(&bc->start_time);
				AC_WRITE_LOG(info, "\t[BC%d:%f %s timeout]", bc->id,
						bc->time_span / 1000000.0, get_bcgroup_name(bc->id));
			}
			bc = bc->next;
		}
		dc = dcinfo->dc[i];
		while(dc){
			if(dc->status == 1){
				dc->status = 3;
				dc->time_span = calc_time_span(&dc->start_time);
				AC_WRITE_LOG(info, "\t[DC%d:%f timeout]", dc->id, dc->time_span / 1000000.0);
			}
			dc = dc->next;
		}
	}


	AC_WRITE_LOG(info, "\t%f\t%f", calc_time_span(&info->result.start_time) / 1000000.0, calc_time_span(&info->result.recv_time) / 1000000.0);
    /*
    AC_WRITE_LOG(info, "\tTIMEOUT_RATE:{")
    for (int i = 0 ; i < (int)bcCaller.machines.size();i++) {
        std::vector<MGroup *>& groups = bcCaller.machines;
        float rate = info->getToRateOfBC(groups[i]->idx);
        if (rate >= 0) {
            AC_WRITE_LOG(info, "BC%d:%f ", groups[i]->idx, rate);
        }
    }
    
    AC_WRITE_LOG(info, "}");
    */
    info->log[info->log_len] = '\0';
    int len = snprintf(output, maxlen, "%s", info->log);
	pos = len + 1;
	return pos;

}

int TCallBack::call_errlog(void *data, char *output, int maxlen) {
	QsInfo* info = (QsInfo*) data;
	if (info->result.start_time.tv_sec == 0) {
		return -1;
	}
    info->errlog[info->err_log_len] = '\0';
    int len = snprintf(output, maxlen, "%s", info->errlog);
  
	return len + 1;
}

int TCallBack::call_monitor(int argc, char **argv, char *reply, int maxlen) {
	char *cmd;

	cmd = argv[0];
	if(strcmp(cmd, "mem") == 0){
		int num, total, left;
		acmpool_stats(g_pool, &num, &total, &left);
		snprintf(reply, maxlen, "num:%d\ttotal:%dM\tleft:%dM", num, total, left);
	}else if(strcmp(cmd, "memdump") == 0){
		if(argc < 2){
			snprintf(reply, maxlen, "usage:memdup filename");
		}else{
			acmpool_dump(g_pool, argv[1]);
			snprintf(reply, maxlen, "ok");
		}
	}else{
		snprintf(reply, maxlen, "unrecognized cmd:%s\n", cmd);
	}
	return 0;
}

void* TCallBack::call_data_create(uint32_t seqid, CommonHead *phead, struct timeval recv_time, ThreadWork * thread) {
	memcached_st *  mem = thread->mem;
	ShresholdTimeoutCounter * timeCounter = &thread->timeoutCounter;
    UTF8ChineseTransfer * ctr = &thread->ctr;
    if (!ctr->isValid()) {
        ctr->loadDict();
    }

	QsInfo *info = /*new QsInfo;*/thread->popInfo();

	if (!info) {
        SLOG_FATAL("[seqid:%u] can not alloc info", seqid);
        suicide();
    }
	info->buffer = server_buffer(g_server->server, &info->buf_size);
	info->buf_pos = 0;

	// init the info
	if (!info) {
		SLOG_FATAL("doc doc over leak 5!");
		assert(info);
	}

	info->seqid = seqid;
	info->conf = g_server->conf_current;
	info->result.recv_time = recv_time;
	info->mem = mem;
	//info->cache = cache;
	//info->cache_size = cache_size;
	info->timeoutCounter = timeCounter;
    info->ctr = ctr;

	if (info->timeoutCounter == NULL) {
		SLOG_WARNING("[seqid:%u] can not get timeCounter", info->seqid);
		return NULL;
	}

	memmove(&info->common_head, phead, sizeof(CommonHead));
	AC_WRITE_LOG(info, "[%u]\t[%u]\t", phead->seqid, seqid);
	//HASH_INIT(info->bcinfo.docids, MAX_DOCID_NUM);
	HASH_INIT(info->bcinfo.bcClusters, MAX_BC_CLUSTER_COUNT);

	//block
	info->block_max = 64;
	info->blocks = (void**)malloc(sizeof(void*) * info->block_max);
	assert(info->blocks);

	info->bc_num = 0;
	for (uint32_t i = 0; i < bcCaller.machines.size(); i ++){
		if (info->bc_num < bcCaller.machines[i]->idx + 1){
			info->bc_num = bcCaller.machines[i]->idx + 1;
		}
	}
//	info->bc_num = info->conf->mac_info.num;

	//init the da
	info->dainfo.querys = (QsDAQuerys**) ac_malloc(info, sizeof(QsDAQuerys*) * info->bc_num);
	memset(info->dainfo.querys, 0, sizeof(QsDAQuerys*) * info->bc_num);

	//init the bc
	info->bcinfo.bc = (QsBCInfo**)ac_malloc(info, sizeof(QsBCInfo*) * info->bc_num);
	memset(info->bcinfo.bc, 0, sizeof(QsBCInfo*) * info->bc_num);

	//init the dc
	info->dcinfo.dc = (QsDCInfo**)ac_malloc(info, sizeof(QsDCInfo*) * info->bc_num);
	memset(info->dcinfo.dc, 0, sizeof(QsDCInfo*) * info->bc_num);

	//init the event point
	info->pevents[AC_EVENT_NUM] = info->events;

	//init the social uids
	HASH_INIT(info->socialinfo.social_uids, MAX_UID_NUM);

	info->magic = MAGIC_QSINFO;

    memset(info->tags, 0, MAX_HOTTAG * sizeof (HotTag));

    // no need to malloc mem for below now
    info->ppack = NULL;
    info->json_hothit = NULL;

    info->intention = 0;
    info->json_intention = NULL;

    info->json_comments = NULL;

    memset(info->extra_docs, 0, MAX_EXTRA_DOC_NUM * sizeof (SrmDocInfo*));
    info->extra_doc_num = 0;

    memset(info->ids, 0, MAX_EXTRA_DOC_NUM * sizeof (UID_T));
    info->id_num = 0;


	return info;
}

void TCallBack::call_data_init(void*) {
	return;
}

void TCallBack::call_data_reset(void*) {
	return;
}

void TCallBack::call_data_destroy(ThreadWork* thread, void* data) {
	if (data) {
      QsInfo * pInfo = (QsInfo*) data;
      //delete pInfo;
      
        pInfo->reset();
		thread->pushInfo(pInfo);
      
	}
}

