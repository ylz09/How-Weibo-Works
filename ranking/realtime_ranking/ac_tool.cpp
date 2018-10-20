#include "ac_tool.h"
#include "ac_caller_man.h"
#include "jsoncpp/jsoncpp.h"



extern ACServer *g_server;

void* ac_malloc(QsInfo *info, size_t size){
	void *p;
	if(info->block_num == info->block_max){
		info->block_max <<= 1;
		info->blocks = (void**)realloc(info->blocks, info->block_max * sizeof(void*));
		assert(info->blocks);
	}
	p = malloc(size);
	if (p == NULL){
        SLOG_FATAL("can not alloc p");
        suicide();
    }
    
	info->blocks[info->block_num ++] = p;
	return p;
}

int ac_md5(QsInfo *info, char *query, int query_len, uint8_t bytes[16]){
	md5_context ctx;
	md5_start(&ctx);
	md5_update(&ctx, query, query_len);
	md5_finish(&ctx, bytes);
	return 0;
}

char* ac_get_query(QsInfo *info, const char *query, int query_len){
	QsQuery **querys = info->querys;
	uint64_t h = hash64((void *)query, query_len);
	int idx = h % MAX_QUERY_NUM;
	QsQuery *node = querys[idx], *last_node = NULL;
	while(node){
		if(node->id == h){
			return node->query;
		}
		last_node = node;
		node = node->next;
	}
	node = (QsQuery*)ac_malloc(info, query_len + 1 + offsetof(QsQuery, query));
	memset(node, 0, sizeof(QsQuery));
	node->id = h;
	memmove(node->query, query, query_len);
	node->query[query_len] = 0;
	if(last_node){
		last_node->next = node;
	}else{
		querys[idx] = node;
	}
	return node->query;
}

static ACEvent* ac_free_event(QsInfo *info, ACEventEnum etype){
	ACEvent *event = info->pevents[AC_EVENT_NUM];
	ACEvent *last_event;
	int event_id = event - info->events;
	if(event == NULL){
		return NULL;
	}
	if(event->next){
		info->pevents[AC_EVENT_NUM] = event->next;
	}else if(event_id + 1 == MAX_EVENT_NUM){
		info->pevents[AC_EVENT_NUM] = NULL;
	}else{
		info->pevents[AC_EVENT_NUM] ++;
	}
	memset(event, 0, sizeof(ACEvent));
	last_event = info->pevents[etype];
	if(last_event == NULL){	//锟斤拷锟诫到锟铰硷拷锟斤拷锟斤拷锟斤拷
		info->pevents[etype] = event;
	}else{
		while(last_event->next){
			last_event = last_event->next;
		}
		last_event->next = event;
	}
	return event;
}

int ac_add_event(QsInfo *info, ACEventEnum etype, func_event call_event, void *args, int status){
	ACEvent *event = ac_free_event(info, etype);
	if(event == NULL){
		return -1;
	}
	event->status = status;
	event->etype = etype;
	event->call_event = call_event;
	event->args = args;
	return event - info->events;
}

int ac_del_event(QsInfo *info, int event_id){
	ACEvent *event, *last_event;
	ACEventEnum etype;

	if(event_id >= MAX_EVENT_NUM){
		return -1;
	}
	event = info->events + event_id;
	if(event->call_event == NULL){
		return -1;
	}
	etype = event->etype;

	//锟斤拷锟铰硷拷锟斤拷锟斤拷锟斤拷删锟斤拷
	if(info->pevents[etype] == event){
		info->pevents[etype] = event->next;
	}else{
		last_event = info->pevents[etype];
		while(last_event->next != event){
			last_event = last_event->next;
		}
		last_event->next = event->next;
	}
	
	event->next = info->pevents[AC_EVENT_NUM];
	info->pevents[AC_EVENT_NUM] = event;
	return 0;
}

int ac_start_event(QsInfo *info, ACEventEnum etype, int *step){
	int ret = 0;
	ACEvent *event, *last_event, *tmp_event;
	event = info->pevents[etype];
	last_event = NULL;
	while(event){
		if(event->call_event){
			ret = event->call_event(info, event->args, step);
		}
		if(event->status == 0){
			if(last_event == NULL){
				info->pevents[AC_EVENT_DA] = event->next;
			}else{
				last_event = event->next;
			}
			tmp_event = event->next;
			event->next = info->pevents[AC_EVENT_NUM];
			info->pevents[AC_EVENT_NUM] = event;
			event = tmp_event;
		}else{
			event = event->next;
		}
	}
	return ret;
}

int ac_bc_search(QsInfo *info, int *step){
	uint32_t bc_idx = 0;
	int i, ret;
	QsDAQuerys *da_query;
	QsInfoDA *dainfo = &info->dainfo;

    if (info->parameters.getXsort() != ""
        && sid_match(info->parameters.getXsort().c_str(), &(info->conf->month_xsort)) == 0) {
        info->params.flag |= QSPARAMS_FLAG_BCSPEC;
        info->params.bc_idx = ac_conf_all_first_idx();
    }

	if ((info->params.flag & QSPARAMS_FLAG_BCSPEC)){
		int degrade = GlobalConfSingleton::GetInstance().getAcConf().global.history_degrade;
		bool skip_history = (degrade == 2);
		if (sid_match(info->params.sid, &(info->conf->month_sid)) == 0){
			skip_history = true;
		}

		if (degrade == 1 && sid_match(info->params.sid, &(info->conf->value_sid)) != 0){
			skip_history = true;
		}

		if (skip_history) {
            info->params.bc_idx &= ~(ac_conf_all_second_idx());
	    }
	    
	    bc_idx = info->params.bc_idx;
	    ret = ac_dispatch_sample(info, bcCaller.machines, &bc_idx);
	    if((info->bcinfo.addr_res & bc_idx) == bc_idx){
			return -1;
	    }
	}else{
		ret = ac_dispatch(info, bcCaller.machines, &bc_idx);
	    if(ret){
            return -1;
	    }
	}
	info->bcinfo.level ++;
	SLOG_TRACE("[seqid:%u] bc level is %d", info->seqid, info->bcinfo.level);
	info->bcinfo.cur_req = bc_idx;
    AC_WRITE_LOG(info, "\t{ac_bc_search:cur_req:%d}", bc_idx);
	info->bcinfo.addr_req |= bc_idx;
	for(i = 0; i < info->bc_num; i ++){
		if(bc_idx & (1 << i)){
			//DLLCALL(so_query, query_bc, da_query, info, i);
			da_query = query_bc(info, i);
			da_query->next = dainfo->querys[i];
			dainfo->querys[i] = da_query;
		}
	}
	if(dainfo->cur_data == NULL){
		(*step) ++;
		myda_done(info, NULL, step);
	}else{
		ac_add_event(info, AC_EVENT_DA, myda_done, NULL, 0);
	}
	return 0;
}

void* ac_parse_json(char *request, int len){
	void * conf = hconf_create();
    if (!conf) {
        SLOG_FATAL("can not alloc hconf_create");
        suicide();
    }
    /* 
	JsonTree_t *tree = CreateJsonTree();
	if (ParseJsonTop(request, len, tree, 1) < 0){
        FreeJsonTree(tree);
        SLOG_FATAL("parse json error %s", request);
        return NULL;
    }

	for(int i = 0; i < tree->curPairNum; i ++){
		PairNode_t *node = tree->pairs + i;
		if(node && node->pStr && *node->pStr){
			hconf_insert(conf, node->keyStr, node->pStr);
		}
	}
	FreeJsonTree(tree);
	*/
	Json::Reader reader;
	Json::Value  root;
	if (!reader.parse(request, root)) {
        SLOG_FATAL("parse json error %s", request);
        return NULL;
	}
	if (root.type() == Json::objectValue) {
		const Json::Value::Members & members = root.getMemberNames();
		for (Json::Value::Members::const_iterator it = members.begin(); it != members.end(); ++it) {
			std::string val;
			switch (root[*it].type())
			{
				case Json::stringValue:
					val = root[*it].asString();
					break;
				case Json::nullValue:
					val = "";
					break;
				case Json::intValue:
					val = Json::valueToString(root[*it].asInt());
					break; 
				case Json::uintValue:
					val = Json::valueToString(root[*it].asUInt64());
					break;
				case Json::realValue:
					val = Json::valueToString(root[*it].asDouble());
					break;
				case Json::booleanValue:
					val = root[*it].asBool() ? "1" : "0";
					break;
				case Json::arrayValue:
					break;
				case Json::objectValue:
					break;
				default:
					break;
			}
			if ((*it).size() > 0 && val.size() > 0) {
				hconf_insert(conf, const_cast<char*> ((*it).c_str()), const_cast<char*>(val.c_str()));
			}
		}
	}
	return conf;
}

int sid_match(const char* sid, SID_ARRAY* p_sid){
	int i;
	for (i = 0; i < p_sid->count; i ++){
		if(strcmp(sid, p_sid->sid_array[i].name) == 0){
			return 0;
		}
	}
	return -1;
}

