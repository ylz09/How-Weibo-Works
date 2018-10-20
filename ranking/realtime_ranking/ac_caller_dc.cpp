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
#include "ac/src/plugins_weibo/ac_ext_data_parser.h"

#define MAX_BUF_SIZE 10240

extern "C" {
	extern ACServer *g_server;
}
const char * AcDcCaller::getName() {
	return "dc";
}

int AcDcCaller::pre_one(void *data, int idx, char *output, int maxlen)
{
	if (conf_get_int(g_server->conf_current, "fake_dc", 0)) {
		return -1;
	}

	return AcCallerDcBase::pre_one(data, idx, output, maxlen);
}

int AcDcCaller::get_dc_result(QsInfo *info, DIDocHead *head_doc, SrmDocInfo *doc_info){
	int len, i, field_id, ret, flag;
	DIFieldHead *field_head, *field;
	char *p;
	char rec[MAX_BUF_SIZE];
	int status_value;
	int count = 0;

    flag = GlobalConfSingleton::GetInstance().getAcConf().global.filter_output;
	if(flag && !(info->params.nofilter & 0x1) ){
		field_head = (DIFieldHead*)(head_doc + 1);
		p = (char*)(field_head + head_doc->fieldnum);
		for(i = 0; i < head_doc->fieldnum; i ++){
			field = field_head + i;
			field_id = field->id;
			if(field_id == info->conf->filter_id){
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
			if (dc[i] > 0) {
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

