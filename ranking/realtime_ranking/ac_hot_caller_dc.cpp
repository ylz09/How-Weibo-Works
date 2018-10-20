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

#include "ac_caller_dc.h"
#include "ac_filter.h"
#include "ac_sort.h"
#include "cache_new.h"
#include "ac/src/plugins_weibo/ac_hot_ext_data_parser.h"
#include "lib64/common/include/strmatch.h"

using namespace std;

#define MAX_BUF_SIZE 10240
#define SHOW_DEBUG_LOG_LEVEL 2
#define SHOW_DEBUG_LOG_LEVEL_MAIN 0
#define SHOW_DEBUG_NEWLINE 1
#define CONTENT_MIN_LEN 15

extern "C" {
	extern ACServer *g_server;
}
const char * AcDcCaller::getName() {
	return "dc";
}

int AcDcCaller::pre_one(void *data, int idx, char *output, int maxlen)
{
	QsInfo *info = (QsInfo*) data;
    AC_WRITE_LOG(info, " {get_bc_use_dc(idx:%d):%d}", idx, get_bc_use_dc(idx));
    // set use_dc
    if (get_bc_use_dc(idx) &&
        info->params.start == 0) {
        AC_WRITE_LOG(info, " {get_bc_use_dc(idx:%d)}", idx);
        return AcCallerDcBase::pre_one(data, idx, output, maxlen);
    }
    // set fake_dc
	if (conf_get_int(g_server->conf_current, "fake_dc", 0)) {
		return -1;
	}

	return AcCallerDcBase::pre_one(data, idx, output, maxlen);
}

int AcDcCaller::get_dc_result(QsInfo *info, DIDocHead *head_doc, SrmDocInfo *doc_info){
    AC_WRITE_LOG(info, " {get_dc_result:%llu}", head_doc->docid);
	int len, i, field_id, ret, flag;
	DIFieldHead *field_head, *field;
	char *p;
	char rec[MAX_BUF_SIZE];
	int status_value;
	int count = 0;

    /*
     * don't filter
    flag = GlobalConfSingleton::GetInstance().getAcConf().global.filter_output;
	if(flag && !(info->params.nofilter & 0x1) ){
		field_head = (DIFieldHead*)(head_doc + 1);
		p = (char*)(field_head + head_doc->fieldnum);
		for(i = 0; i < head_doc->fieldnum; i ++){
			field = field_head + i;
			field_id = field->id;
			if(field_id == info->conf->filter_id){
                AC_WRITE_LOG(info, "{DI:==filter_id:%d, %d:%d, field.len:%d}",
                            info->conf->filter_id, TYPE_INT, field->type,
                            field->len);
				count ++;
				if (field->type != TYPE_INT){
					len = field->len < MAX_BUF_SIZE - 1 ? field->len : MAX_BUF_SIZE - 1;
					memcpy(rec, p+field->pos, len);
					rec[len] = '\0';
				}
			}
			if(field_id == info->conf->status_id){
				count ++;
				if (field->type == TYPE_INT){
					memcpy(&status_value, p+field->pos, field->len);
				} else if (field->type == TYPE_CHAR) {
                     char tmp[20];
                     if (field->len < 20) {
                          memmove(tmp, p+field->pos, field->len);
                          tmp[field->len] = 0;
                          status_value = atoi(tmp);
                     }
                }
			}
			if (count == 2){
				break;
			}
		}
	}

    AC_WRITE_LOG(info, "{DI:field_id:%d,%d:%d,status_id:%d:%d,content:%s}",
                head_doc->fieldnum, field_id, info->conf->filter_id,
                info->conf->status_id, status_value, rec);
    int is_filter = GlobalConfSingleton::GetInstance().getAcConf().global.is_filter;
	if (count == 2){
        if (is_filter) {
            ret = ac_filter_output(rec, GetDomain(doc_info), status_value);
            if(ret){	
                slog_write(LL_WARNING, "uid:%llu, docid:%lu", GetDomain(doc_info), doc_info->docid);
                goto LB_ERROR;
            }
        }
	}
    */
    
	len = sizeof(DIDocHead) + sizeof(DIFieldHead) * head_doc->fieldnum + head_doc->doclen;
	doc_info->doc_head = (DIDocHead*)ac_malloc(info, len);
	memmove(doc_info->doc_head, head_doc, len);
	info->dcinfo.recv_num ++;

	return 0;

LB_ERROR:
	return -1;
}

int AcDcCaller::statistics_dc_result(QsInfo *info)
{
	if (info == NULL || info->bc_num <= 0) {
		return -1;
	}
    // set use_dc
    if (info->get_dc_docs.size() > 0) {
		QsInfoDC *dcinfo = (QsInfoDC*) &info->dcinfo;
		for (int i = 0; i < info->bc_num; ++i) {
			if (dcinfo->dc[i] && get_bc_use_dc(dcinfo->dc[i]->id) &&
                dcinfo->dc[i]->recv_num > 0) {
				AC_WRITE_LOG(info, "{DC%d:num:%d}", i, dcinfo->dc[i]->recv_num);
			}
		}
        int hot_need_filter_threshold =
            conf_get_int(g_server->conf_current, "hot_need_filter_threshold", 20);
        // if (info->parameters.getUs() == 1 ||
        if (info->bcinfo.get_doc_num() > hot_need_filter_threshold) {
            dc_dedup(info);
        }
    }

	if (conf_get_int(g_server->conf_current, "fake_dc", 0)) {
		int dc[info->bc_num];
		memset(dc, 0, sizeof(dc));

		QsInfoBC *bcinfo = (QsInfoBC*) &info->bcinfo;
		QsInfoDC *dcinfo = (QsInfoDC*) &info->dcinfo;
		int end = dcinfo->docstart + dcinfo->docnum;
		if (end > bcinfo->get_doc_num()) {
			end = bcinfo->get_doc_num();
		}
		
		for (int i = dcinfo->docstart; i < end; ++i) {
			SrmDocInfo *doc = bcinfo->get_doc_at(i);
			if (doc && doc->bcidx >= 0 && doc->bcidx < info->bc_num) {
				++dc[doc->bcidx];
			}
		}

		for (int i = 0; i < info->bc_num; ++i) {
			if (!get_bc_use_dc(i) &&
                dc[i] > 0) {
				AC_WRITE_LOG(info, "{DC%d:FAKE num:%d}", i, dc[i]);
			}
		}
	}
	return 0;
}

int AcDcCaller::done_all_rank(QsInfo * info, int * step, uint32_t * addr) {
	QsInfoBC *bcinfo = &info->bcinfo;
	QsInfoDC *dcinfo = &info->dcinfo;
	int di_refetch = GlobalConfSingleton::GetInstance().getAcConf().global.di_refetch;
	if(dcinfo->recv_num < info->params.num &&
       dcinfo->refetch < di_refetch &&
       dcinfo->docstart + dcinfo->docnum < (int)bcinfo->get_doc_num()
	){
		dcinfo->docstart += dcinfo->docnum;
		if(dcinfo->docstart + dcinfo->docnum > (int)bcinfo->get_doc_num()){
            dcinfo->docnum = (int)bcinfo->get_doc_num() - dcinfo->docstart;
		}
		*step = AC_STEP_DC;
		dcinfo->refetch ++;
		dcinfo->start_time.tv_sec = 0;
	}else{
		int fnum = dcinfo->docstart + dcinfo->docnum + info->extra_doc_num - info->params.start - dcinfo->recv_num;
		if(fnum){
			info->result.filter_count += fnum;
           
			SLOG_DEBUG("info->result.filter_count:%d,fm:%d(docstart:%d,docnum:%d,extra_doc_num:%d,paramsstart:%d,dcinfo->num:%d)",
					info->result.filter_count, fnum,
					dcinfo->docstart, dcinfo->docnum, info->extra_doc_num,
					info->params.start, dcinfo->recv_num);
		}
		SLOG_DEBUG("{filter:%d}", fnum);
	}

	ac_abs_dup(info);

	statistics_dc_result(info);

    if ( (info->result.type_incache != PAGE_CACHE)
        && ( (strcmp(info->params.sid, "t_search") == 0) 
            || (strcmp(info->params.sid, "t_wap_ios") == 0)
            || (strcmp(info->params.sid, "t_wap_android") == 0)
            || (strcmp(info->params.sid, "t_wap_c") == 0)
           )
        ) {
        set_whole_cache(info);
		set_page_cache(info);
	}

    set_category_cache(info);

	return 0;
}

int AcDcCaller::done_one(void *data, int idx, char *result, int len) {
    AcCallerDcBase::done_one(data, idx, result, len);

    // 
	QsInfo *info = (QsInfo*) data;
	QsInfoDC *dcinfo = (QsInfoDC*) &info->dcinfo;
	QsInfoBC *bcinfo = (QsInfoBC*) &info->bcinfo;
	CommonHead *phead;
	CMDHead *head;
	DCRespHeader *response;
	DIDocHead *head_doc;
	char *end;
	int doc_len;
	SrmDocInfo *docinfo;

	QSBUF_INIT(info);

	end = result + len;
	phead = (CommonHead*) result;
	head = (CMDHead*) (phead + 1);
	response = (DCRespHeader*) (head + 1);
	head_doc = (DIDocHead*) (response + 1);
    AC_WRITE_LOG(info, " {DC%d_done_one_real}", idx);
    __gnu_cxx::hash_map<uint64_t, DcDedupData >::iterator it;
    while ((char*) head_doc < end) {
		doc_len = sizeof(DIDocHead) + sizeof(DIFieldHead) * head_doc->fieldnum
				+ head_doc->doclen;
        if (info->parameters.debug > SHOW_DEBUG_LOG_LEVEL) {
            AC_WRITE_LOG(info, " [DI:%x:%llu doc_len:%d]", head_doc, head_doc->docid, doc_len);
        }
        if (head_doc->docid == info->parameters.debugid) {
            AC_WRITE_LOG(info, " [DI:%x:%llu doc_len:%d]", head_doc, head_doc->docid, doc_len);
        }
        it = info->get_dc_docs.find(head_doc->docid);
        if (it != info->get_dc_docs.end()) {
            if (info->parameters.debug > SHOW_DEBUG_LOG_LEVEL) {
                AC_WRITE_LOG(info, " [DI:%llu find]", head_doc->docid);
            }
            if (head_doc->docid == info->parameters.debugid) {
                AC_WRITE_LOG(info, " [DI:%llu find]", head_doc->docid);
            }
            docinfo = it->second.doc;
            std::string content;
            if (get_char_field_value(info, head_doc, info->conf->filter_id, TYPE_CHAR, &content)) {
                if (info->params.flag & QSPARAMS_FLAG_ACCACHE) {
                    uint64_t h = DOC_HASH(head_doc->docid, head_doc->source);
                    cache_set(g_server->cache, CACHE_TYPE_DOCID, &h,
                            sizeof(uint64_t), head_doc, doc_len, CACHE_TIME);
                }
                it->second.content = content;
                if (info->parameters.debug > SHOW_DEBUG_LOG_LEVEL) {
                    AC_WRITE_LOG(info, " [content:%llu %s]", head_doc->docid, content.c_str());
                }
                if (head_doc->docid == info->parameters.debugid) {
                    AC_WRITE_LOG(info, " [content:%llu %s]", head_doc->docid, content.c_str());
                }
            } else {
                if (info->parameters.debug > SHOW_DEBUG_LOG_LEVEL) {
                    AC_WRITE_LOG(info, " [DI:%llu not find content]", head_doc->docid);
                }
                if (head_doc->docid == info->parameters.debugid) {
                    AC_WRITE_LOG(info, " [DI:%llu not find content]", head_doc->docid);
                }
            }
		} else {
            if (info->parameters.debug > SHOW_DEBUG_LOG_LEVEL) {
                AC_WRITE_LOG(info, " [DI:%llu not find]", head_doc->docid);
            }
            if (head_doc->docid == info->parameters.debugid) {
                AC_WRITE_LOG(info, " [DI:%llu not find]", head_doc->docid);
            }
        }
		head_doc = (DIDocHead*) ((char*) head_doc + doc_len);
	}
    return 0;
}

bool AcDcCaller::get_char_field_value(QsInfo *info, DIDocHead *head_doc, int field_id,
            int type, std::string* content) {
    if (type != TYPE_CHAR) {
        return NULL;
    }
	int len, i;
	DIFieldHead *field_head, *field;
	char *p;
	char rec[MAX_BUF_SIZE];

    field_head = (DIFieldHead*)(head_doc + 1);
    p = (char*)(field_head + head_doc->fieldnum);
    for(i = 0; i < head_doc->fieldnum; i ++){
        field = field_head + i;
        if(field->id == field_id &&
           field->type == type){
            if (info->parameters.debug > SHOW_DEBUG_LOG_LEVEL) {
                AC_WRITE_LOG(info, " {DI:==field_id:%d, %d:%d, field.len:%d}",
                            field_id, TYPE_CHAR, field->type,
                            field->len);
            }
            len = field->len < MAX_BUF_SIZE - 1 ? field->len : MAX_BUF_SIZE - 1;
            memcpy(rec, p+field->pos, len);
            rec[len] = '\0';
            content->assign(rec);
            return true;
        }
    }
    return false;
}

int AcDcCaller::dc_dedup(QsInfo *info) {
	struct timeval begin;
	gettimeofday(&begin, NULL);
	QsInfoBC *bcinfo = (QsInfoBC*) &info->bcinfo;
    uint32_t dc_doc_num = conf_get_float(info->conf, "get_dc_doc_num", 100);
    uint32_t num = bcinfo->get_doc_num();
    if (num > dc_doc_num) {
        num = dc_doc_num;
    }
    int i = 0;
    std::map<uint64_t, std::string> content_map;
    __gnu_cxx::hash_map<uint64_t, DcDedupData >::iterator it;
	uint32_t stop_num = conf_get_int(g_server->conf_current, "dc_dup_us1_stop_num", 3);
    /*
    if (info->parameters.getUs() == 2) {
        stop_num = conf_get_int(g_server->conf_current, "dc_dup_us2_stop_num", 10);
    }
    */
    bool us_1 = false;  // info->parameters.getUs() == 1;
    uint32_t now_num = 0;
	int dup_max = conf_get_int(g_server->conf_current, "dc_dup_max_per", 80);
	int us_1_max_dc_dedup_num = conf_get_int(g_server->conf_current, "us_1_max_dc_dedup_num", 3);
    std::map<uint64_t, DupMap> dup_map;
	double dup_del_per = conf_get_int(g_server->conf_current, "dup_del_per", 70);
    for (i = 0; i < num; i++) {
        SrmDocInfo *doc = bcinfo->get_doc_at(i);
        if (doc == NULL ||
            !get_bc_use_dc(doc->bcidx)) {
            continue;
        }
        it = info->get_dc_docs.find(doc->docid);
        if (it == info->get_dc_docs.end() ||
            it->second.content.size() < CONTENT_MIN_LEN) {
            continue;
        }
        AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                    " [dc_dedup:size:%d,%llu %s]",
                    content_map.size(), doc->docid, it->second.content.c_str());
        bool del_flag = 0;
        DcDedupData dc_dup_data;
        dc_dup_data.doc = doc;
        dc_dup_data.content = it->second.content;
        if (dup_map.size() == 0) {
            DupMap dup_data;
            dup_data.max_score = doc->value;
            dup_data.max_score_mid = doc->docid;
            dup_data.del_score = doc->value * dup_del_per / 100;
            dup_data.rand_docs = 1;
            dup_data.dedup_content.push_back(it->second.content);
            dup_data.docs.insert(make_pair(i, dc_dup_data));
            dup_map.insert(make_pair(0, dup_data));
        } else {
            std::map<uint64_t, DupMap>::iterator it_map = dup_map.begin(); 
            for (; it_map != dup_map.end(); ++it_map) {
                for (int index = 0; index < it_map->second.dedup_content.size(); ++index) {
                    std::string old_content = it_map->second.dedup_content[index];
                    int max = longest_common_substr(old_content.c_str(), old_content.size(),
                                it->second.content.c_str(), it->second.content.size(), 0);
                    double size = old_content.size() > it->second.content.size() ?
                                it->second.content.size() : old_content.size();
                    double per = 100.0 * max / size;
                    if (per > dup_max) {
                        AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                                    " [:%llu, per:%f, max:%d, :%d,%s, :%d,%s]",
                                    doc->docid, per, max, old_content.size(), old_content.c_str(),
                                    it->second.content.size(), it->second.content.c_str());
                        if (doc->docid == info->parameters.debugid) {
                            DOC_WRITE_ACDEBUG(doc, "dc_dup:per:%f, max:%d, :%d,%s, :%d,%s",
                                        per, max, old_content.size(), old_content.c_str(),
                                        it->second.content.size(), it->second.content.c_str());
                        }
                        if (doc->value >= it_map->second.del_score) {
                            it_map->second.rand_docs += 1;
                        }
                        uint32_t size = it_map->second.docs.size();
                        it_map->second.docs.insert(make_pair(size, dc_dup_data));
                        // 
                        // it_map->second.dedup_content.push_back(it->second.content);
                        // 
                        del_flag = 1;
                        break;
                    }
                }
                if (del_flag) {
                    break;
                }
                // check dup
            }
            if (!del_flag) {
                DupMap dup_data;
                dup_data.max_score = doc->value;
                dup_data.max_score_mid = doc->docid;
                dup_data.del_score = doc->value * dup_del_per / 100;
                dup_data.rand_docs = 1;
                dup_data.dedup_content.push_back(it->second.content);
                dup_data.docs.insert(make_pair(i, dc_dup_data));
                dup_map.insert(make_pair(dup_map.size(), dup_data));
            }
        }
        if (dup_map.size() >= stop_num &&
            i > us_1_max_dc_dedup_num) {
            break;
        }
    }
    std::map<uint64_t, DupMap>::iterator it_delete = dup_map.begin();
    srand((unsigned int)(time((time_t*)NULL)));
    AC_WRITE_LOG(info, " [us:%d, dup_map.size:%d]", us_1, dup_map.size());
    AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                        "\n");
    int dup_group = 0;
	int white_protect = conf_get_int(g_server->conf_current,"white_protect", 1);
    for (; it_delete != dup_map.end(); ++it_delete) {
        ++dup_group;
        AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                            "\n\n");
        bool keep = true;
        uint32_t rand_docs = it_delete->second.rand_docs;
        if (rand_docs > it_delete->second.docs.size()) {
            rand_docs = it_delete->second.docs.size();
        }
        uint32_t keep_doc_count = rand() % rand_docs;
        AC_WRITE_LOG(info, " [g:%d, keep:%d, rand:%d, size:%d]",
                    dup_group, keep_doc_count, rand_docs, it_delete->second.docs.size());
        uint32_t now_doc_num = 0, pass_doc_num = 0;
        std::map<uint32_t, DcDedupData >::iterator it_map_del = it_delete->second.docs.begin();
        bool keep_ok = false;
        for (; it_map_del != it_delete->second.docs.end(); ++it_map_del) {
            ++pass_doc_num;
			AcExtDataParser attr(it_map_del->second.doc); 
			if (PassByWhite(attr) && white_protect){
                AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                            " [docid:%lu, dc pass for white uid]",
                             it_map_del->second.doc->docid);
				continue;
			}
            if (it_map_del->second.doc->docid == it_delete->second.max_score_mid) {
                keep_ok = true;
                AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                            " [g:%d, keep max:pass:%d, now:%d,%llu:%llu]\n[%s]",
                            dup_group, pass_doc_num, now_doc_num, it_map_del->second.doc->docid,
                            it_map_del->second.doc->value, it_map_del->second.content.c_str());
                continue;
            }
            if (!keep_ok && pass_doc_num == it_delete->second.docs.size()) {
                keep_ok = true;
                AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                            " [g:%d, keep end:pass:%d, now:%d,%llu:%llu]\n[%s]",
                            dup_group, pass_doc_num, now_doc_num, it_map_del->second.doc->docid,
                            it_map_del->second.doc->value, it_map_del->second.content.c_str());
                continue;
            }
            AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                        " [g:%d, not max, dc_dup:%llu]", dup_group, it_map_del->second.doc->docid);
            if (it_map_del->second.doc->docid == info->parameters.debugid) {
                DOC_WRITE_ACDEBUG(it_map_del->second.doc, "not max, dc_dup. ");
            }
            bcinfo->del_doc(it_map_del->second.doc);
            continue;
            // for random keep
            if (us_1) {
                // us = 1, keep random doc
                // for random
                AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                            " [pass:%d, now:%d,%llu:%llu]",
                            pass_doc_num, now_doc_num, it_map_del->second.doc->docid,
                            it_map_del->second.doc->value);
                if (it_map_del->second.doc->value < it_delete->second.del_score) {
                    AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                                " [dc_dup:%llu]", it_map_del->second.doc->docid);
                    if (it_map_del->second.doc->docid == info->parameters.debugid) {
                        DOC_WRITE_ACDEBUG(it_map_del->second.doc, "dc_dup. ");
                    }
                    bcinfo->del_doc(it_map_del->second.doc);
                    continue;
                }
                if (keep_ok) {
                    AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                                " [keep_ok, dc_dup:%llu]", it_map_del->second.doc->docid);
                    if (it_map_del->second.doc->docid == info->parameters.debugid) {
                        DOC_WRITE_ACDEBUG(it_map_del->second.doc, "keep_ok,dc_dup. ");
                    }
                    bcinfo->del_doc(it_map_del->second.doc);
                    continue;
                }
                if (now_doc_num != keep_doc_count &&
                    pass_doc_num != it_delete->second.docs.size()) {
                    AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                                " [dc_dup:%llu]", it_map_del->second.doc->docid);
                    if (it_map_del->second.doc->docid == info->parameters.debugid) {
                        DOC_WRITE_ACDEBUG(it_map_del->second.doc, "dc_dup. ");
                    }
                    bcinfo->del_doc(it_map_del->second.doc);
                } else {
                    keep_ok = true;
                    AC_WRITE_DEBUG_LOG(info, SHOW_DEBUG_LOG_LEVEL_MAIN, SHOW_DEBUG_NEWLINE,
                                " [keep:pass:%d, now:%d,%llu:%llu]\n[%s]",
                                pass_doc_num, now_doc_num, it_map_del->second.doc->docid,
                               it_map_del->second.doc->value, it_map_del->second.content.c_str());
                    AC_WRITE_LOG(info, " [dc_rand_keep:%llu]", it_map_del->second.doc->docid);
                    if (it_map_del->second.doc->docid == info->parameters.debugid) {
                        DOC_WRITE_ACDEBUG(it_map_del->second.doc, "keep.");
                    }
                }
                ++now_doc_num;
            } else {
                // us != 1, keep max score
                if (keep) {
                    keep = false;
                    if (info->parameters.debug > SHOW_DEBUG_LOG_LEVEL_MAIN) {
                        AC_WRITE_LOG(info, " [keep_doc:%llu]", it_map_del->second.doc->docid);
                    }
                    continue;
                }
                if (info->parameters.debug > SHOW_DEBUG_LOG_LEVEL_MAIN) {
                    AC_WRITE_LOG(info, " [dc_dup:%llu]", it_map_del->second.doc->docid);
                }
                if (it_map_del->second.doc->docid == info->parameters.debugid) {
                    DOC_WRITE_ACDEBUG(it_map_del->second.doc, "dc_dup. ");
                }
                bcinfo->del_doc(it_map_del->second.doc);
            }
        }
    }
	AC_WRITE_LOG(info, " [stop:%d,now:%d]", stop_num, now_num);
	int reranking_cost = calc_time_span(&begin) / 1000;
	AC_WRITE_LOG(info, " {dc_dup:time:%d }", reranking_cost);
    bcinfo->refresh();
}
