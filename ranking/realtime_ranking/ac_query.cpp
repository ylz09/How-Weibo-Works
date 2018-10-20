#include "ac_include.h"
#include "ac_query_lib.h"
#include "ac_miniblog.h"
#include "ac_so.h"
#include "slog.h"
#include "da_cmd.h"
#include "ac_utils.h"
#include "ac_tool.h"
#include "string_utils.h"
#include "call_data.h"

#include "ac_caller_man.h"

#include "ac_content_control.h"
#include "dacore/resource.h"
#include <dacore/auto_reference.h>
#include "intention_def.h"

using namespace std;

extern ACServer *g_server;
extern int AbtestSplit(std::set<int>& is, const char * input);
extern const int MAX_GRAY_NUM;

extern int del_char(char* origin, char* deleted, char* inserted);
extern void filter_x_query(char* query, char* filter);
extern void change_mark(QueryPartHead* head, int mark_value);

void insert_before(char* key, char* insert, char* query, int* ret)
{			
	char* befkey = key;
	char* aftkey = NULL;

	
	while (befkey[0])
	{
		aftkey = strchr(befkey, ' ');
		if (aftkey == befkey)
		{
			befkey ++;
		}
		else if (aftkey)
		{
			aftkey[0] = '\0';
			*ret += sprintf(query + *ret, " %s%s", insert, befkey);
			befkey = aftkey + 1;
		}
		else
		{
			*ret += sprintf(query + *ret, " %s%s", insert, befkey);
			break;
		}
	}
	
}



void del_escape_char(char * key) {
  del_char(key, "#", " ");
  del_char(key, "@", " ");
  del_char(key, "*", " ");
  del_char(key, "//", " ");
}

bool add_verifiedtype(char* xwhere, int& xw_size, int& ret, int verifiedtype) {
    if (xwhere == NULL) return false;
            if(verifiedtype >= 0 && unsigned(verifiedtype) == VERIFIEDTYPE_NULL){
                SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verified[0:1]==0");
            }    
            else if (verifiedtype & 0xFF) {
                int tmp = verifiedtype & 0xFF;
                SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verified[0:1]==1&&(");
		bool first = true;

		for ( int i = 0; i <= 7; ++i) {
		    if( tmp>>i & 1) {
			if(first){
			    SSNPRINTF(ret, xwhere + ret, xw_size - ret, "verifiedtype[0:3]==%d",i);
			    first = false;
		        }
		        else {
			    SSNPRINTF(ret, xwhere + ret, xw_size - ret, "||verifiedtype[0:3]==%d",i);
		        }
		    }
		}
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, ")");
            }    
            else if (verifiedtype & 0xF00){
                if (verifiedtype & VERIFIEDTYPE_NONV)
                {    
		    if(verifiedtype & VERIFIEDTYPE_BLUEV){
                        SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verified[0:1]==0||(verified[0:1]==1&&verifiedtype[0:3]>=1)");
		    }
		    else  if(verifiedtype & VERIFIEDTYPE_YELV)
                    {    
                        SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verified[0:1]==0||(verified[0:1]==1&&verifiedtype[0:3]==0)");
                    }    
		    else {
			SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verified[0:1]==0");
		    }
		} else{ 
                        SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verified[0:1]==1");
                        if(verifiedtype & VERIFIEDTYPE_BLUEV and verifiedtype & VERIFIEDTYPE_YELV)
                        {    
                            SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verifiedtype[0:3]>=0");
                        }    
			else if(verifiedtype & VERIFIEDTYPE_YELV)
                	{    
                            SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verifiedtype[0:3]==0");
                	}    
			else if(verifiedtype & VERIFIEDTYPE_BLUEV)
			{
                            SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verifiedtype[0:3]>=1");
			}
            	} 
	    }
}


int query_input(QsInfo *info){
    std::cout << "query_input" << std::endl;
	std::string key, xsort, zone, appid, words, \
		status, sid, trend, xcount, xbit, ids, level, lang;
	int starttime, endtime, recentdays, haspic, haslink, hasvideo, hasmusic, hasret, hasori, hasat, hastext, hasvote,
		hasv, start, num, isred, nofilter, atme, dup, ana, mbclass, qi, form, prov,
		city, page, pagesize, istag, xtruncate,isbctruncate, hasarticle;

	int verified, verifiedtype;

	uint64_t cuid, uid;
	char *buffer = info->buffer + info->buf_pos;
	char *query = buffer + 1;
    int qr_size = info->buf_size - info->buf_pos -1024;
	char *xwhere = buffer + (info->buf_size - info->buf_pos - 1024);
    int xw_size = 1024;
	int bsidx, bcidx;
	int ret;
	key = info->parameters.query;
	if(key.length() > 0){
		SLOG_WARNING("this path is deprecated");
		int debug;

		sid = info->parameters.sid;
		cuid = info->parameters.cuid;
		start = info->parameters.start;
		num = info->parameters.num;
		nofilter = info->parameters.nofilter;
		debug = info->parameters.debug;
		bcidx = info->parameters.bcidx;
		bsidx = info->parameters.bsidx;
		isbctruncate = info->parameters.isbctruncate;
		if(bcidx == 0){
			bcidx = -1;
		}
		if(bsidx == 0){
			bsidx = -1;
		}
		info->params.squery = ac_get_query(info, key.c_str(), strlen(key.c_str()));
		info->params.sid = ac_get_query(info, sid.c_str(), strlen(sid.c_str()));
		info->params.uid = cuid;
		info->params.start = start;
		info->params.num = num;
		info->params.isbctruncate = isbctruncate;
		if (nofilter != 0 && nofilter != 1 && nofilter != 2 && nofilter != 3){
			nofilter = 0;
		}
		info->params.nofilter = nofilter;

		if(bcidx != -1){
			info->params.flag |= QSPARAMS_FLAG_BCSPEC;
			info->params.bc_idx = bcidx;
		}
		if(bsidx != -1){
			info->params.flag |= QSPARAMS_FLAG_BSSPEC;
			info->params.bs_idx = bsidx;
		}

		return 0;
	}

	key = info->parameters.getKey();
	xsort = info->parameters.getXsort();
	zone = info->parameters.zone;
	starttime = info->parameters.starttime;
	recentdays = info->parameters.recentdays;
	endtime = info->parameters.endtime;
	haspic = info->parameters.haspic;
	haslink = info->parameters.haslink;
	hasvideo = info->parameters.hasvideo;
	hasarticle = info->parameters.hasarticle;
	hasmusic = info->parameters.hasmusic;
	hasret = info->parameters.hasret;
	hasori = info->parameters.getHasori();
	hasat = info->parameters.hasat;
	hastext = info->parameters.hastext;
	hasvote = info->parameters.hasvote;
	hasv = info->parameters.hasv;
	appid = info->parameters.appid;
	words = info->parameters.words;
	level = info->parameters.level;
	uid = info->parameters.uid;
	status = info->parameters.status;
	start = info->parameters.start;
	num = info->parameters.num;
	sid = info->parameters.sid;
	isred = info->parameters.isred;
	cuid = info->parameters.cuid;
	nofilter = info->parameters.nofilter;
	atme = info->parameters.atme;
	ids = info->parameters.ids;
	dup = info->parameters.dup;
	ana = info->parameters.ana;
	trend = info->parameters.trend;
	mbclass = info->parameters.mbclass;
	qi = info->parameters.qi;
	form = info->parameters.form;
	xcount = info->parameters.xcount;
	xbit = info->parameters.xbit;
	prov = info->parameters.prov;
	city = info->parameters.city;
	page = info->parameters.getPage();
	pagesize = info->parameters.pagesize;
	lang = info->parameters.lang;
	istag = info->parameters.istag;

	bcidx = info->parameters.bcidx;
	bsidx = info->parameters.bsidx;
	
	verifiedtype = info->parameters.verifiedtype;

    	int fwnum = info->parameters.fwnum;

	//PARAM_GET_INT(info->kvlist, "xtruncate", xtruncate, -1);
	isbctruncate = info->parameters.isbctruncate;

	if(sid.length()<= 0){
		return -1;
	}

	ret = 0;
	*xwhere = 0;
    	if (fwnum > 0) {
        	SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&fwnum>=%d", fwnum);
    	}

	//handle verifiedtype from frontend
    	//AcContentControlData* pdict = Resource::Reference<AcContentControlData>("query_vtype.dict");
	const auto_reference<AcContentControlData>& pdict("userdimension.txt");
	vector<int> vtype;
	if(*pdict and pdict->GetControlValue(key,vtype)!= -1) {
	    //slog_write(LL_DEBUG,"add verifiedtype success!");
	    int typeValue= -1;
	    typeValue = vtype[0];
	    int cur_time = time((time_t*)NULL);

	    if(typeValue != -1) {    
		if(cur_time >= vtype[1] && cur_time <= vtype[2])
		{
		    verifiedtype = -1; 
		    bool res = add_verifiedtype(xwhere, xw_size, ret, typeValue);
		    if(!res) slog_write(LL_DEBUG,"add verifiedtype faild!");
		}
	    } 
	}

	// handle verifiedtype from url of libac
        if(verifiedtype >= 0 && verifiedtype < 10) {
            SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verified[0:1]==1");
            switch(verifiedtype){
                case ALL_BLUE_V:// all blue v
                    SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verifiedtype[0:3]>=1"); break;
                case ALL_V:// all v
                    SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verifiedtype[0:3]>=0"); break;
                default:// 0: yellow v; 1-7 specified blue v
                    SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verifiedtype[0:3]==%d", verifiedtype); 

            }    
        }    
        else if (verifiedtype == NOT_V) { // not v
            SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&verified[0:1]==0");
        }  

	if(mbclass == 0){
        SSNPRINTF(ret, xwhere + ret, xw_size - ret, "%s", "&&mbclass==0");
	}else if(mbclass > 0){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&mbclass[%d:1]==1", mbclass);
	}
	if(starttime > 0){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&digit_time>=%d", starttime);
	}
	if(endtime > 0){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&digit_time<=%d", endtime);
	}

    if (starttime <= 0 && endtime <=0 && recentdays > 0) {
        SLOG_TRACE("[seqid:%u] call recent %d days", info->seqid, recentdays);
        int now = (int)time(NULL);
        SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&digit_time>=%d",   now  - SECONDS_OF_DAY * recentdays);
    }
	if(appid.length() > 0){
		int id = 0;
		safe_to_int(id, appid);
		if(id >= 0){	
			SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&digit_source==%d", id);
		}else{
		    SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&digit_source!=%d", -id);
		}
	} 
	if(hasv == 0 || hasv == 1){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, hasv ? "&&level==2" : "&&level!=2");
	}else if (level.length() > 0){
		int le = 0;
		safe_to_int(le, level);
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "%s", le > 0 ? "&&level==%d" : "&&level!=-%d", le);
	}
	if(qi == 0 || qi == 1 || qi == 2){
	    SSNPRINTF(ret, xwhere + ret, xw_size - ret, "%s", qi == 0 ? "&&qi[0:6]<=20" : (qi == 1 ? "&&qi[0:6]>20&&qi[0:6]<=40" : "qi[0:6]>40"));
	}
	if(hasori == 1 && hasret == 1){
		hasori = hasret = -1;
	}else if(hasori == 1){
		hasret = 0;
	}
	if(haslink == 1 && (hasvideo == 1 || hasmusic == 1 || hasvote == 1)){
		haslink = -1;
	}
	int i = 0, j = 0;
	if(hastext == 1) i ++;
	if(haspic == 1) i ++;
	if(hasvideo == 1) i ++;
	if(hasmusic == 1) i ++;
	if(hasvote == 1) i ++;
	if(hasarticle == 1) i ++;
	j = i;
	if(j == 1){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "%s", "&&");
	}else if(j > 1){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "%s", "&&(");
	}
	if(hastext == 1){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "(digit_attr[0:1]==0&&digit_attr[4:3]==0)%s", --i > 0 ? "||" : "");
	}
	if(haspic == 1){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "digit_attr[0:1]==%d%s", haspic, --i > 0 ? "||" : "");
	}
	if(hasvideo == 1){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "digit_attr[4:1]==%d%s", hasvideo, --i > 0 ? "||" : "");
	}
	if(hasmusic == 1){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "digit_attr[5:1]==%d%s", hasmusic, --i > 0 ? "||" : "");
	}
	if(hasvote == 1){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "digit_attr[6:1]==%d%s", hasvote, --i > 0 ? "||" : "");
	}
	if(hasarticle == 1){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "digit_attr[7:1]==%d%s", hasarticle, --i > 0 ? "||" : "");
	}
	if (j > 1){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "%s", ")");
	}
	
	if(hastext == 0){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "%s", "&&(digit_attr[0:1]==1||digit_attr[4:3]!=0)");
	}
	if(haspic == 0){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&digit_attr[0:1]==%d", haspic);
	}
	if(haslink >= 0){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&digit_attr[1:1]==%d", haslink);
	}
	if(hasret >= 0){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&digit_attr[2:1]==%d", hasret);
	}
	if(hasat >= 0){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&digit_attr[3:1]==%d", hasat);
	}
	if(hasvideo == 0){
	    SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&digit_attr[4:1]==%d", hasvideo);
	}
	if(hasmusic == 0){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&digit_attr[5:1]==%d", hasmusic);
	}
	if(hasvote == 0){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&digit_attr[6:1]==%d", hasvote);
	}
	if(hasarticle == 0){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&digit_attr[7:1]==%d", hasarticle);
	}


	if(status.length() > 0){
		char  status_copy[status.length() + 1];
        if (!status_copy) {
            SLOG_FATAL("[seqid:%u] can not alloc status_copy", status_copy);
            suicide();
        }
		std::strcpy(status_copy, status.c_str());
		char* p = status_copy;
		char* seg = NULL;
		while (1)
		{
			seg = strchr(p, '~');
			if (seg)
			{
				seg[0] = '\0';
				SSNPRINTF(ret, xwhere + ret, xw_size - ret, "%sstatus==%s||", p == status ? "&&(" : "", p);
				p = seg + 1;
			}
			else
			{
				break;
			}
		}
		if(p == status_copy) {
			SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&status==%s", p);
		} else {
			SSNPRINTF(ret, xwhere + ret, xw_size - ret, "status==%s)", p);
		}
	}

	if(lang.length() > 0){
		SSNPRINTF(ret, xwhere + ret, xw_size - ret, "&&language==%s", lang.c_str());
	}
	

	//query
	ret = 0;
	if(key.length() > 0){
		char key_copy[key.length() + 1];
        if (!key_copy) {
            SLOG_FATAL("[seqid:%u] can not alloc key_copy", info->seqid);
            suicide();
        }
		std::strcpy(key_copy, key.c_str());
        info->params.key = ac_get_query(info, key_copy, strlen(key_copy));
		del_char(key_copy, "uid:", " ");
		del_char(key_copy, "tag:", " ");
		del_char(key_copy, "zone:", " ");
		
		filter_x_query(key_copy, "x_");
		
		if(istag == 1){
		        del_escape_char(key_copy);
		        insert_before(key_copy, "tag:", query, &ret);
		}else if(istag == 2){
		        del_escape_char(key_copy);
			insert_before(key_copy, "x_docphrase:.16.5.", query, &ret);
		}else{
			SSNPRINTF(ret, query + ret, qr_size - ret, " %s", key.c_str());
		}
	}
	if(uid > 0){
		SSNPRINTF(ret, query + ret, qr_size - ret, " uid:%lu", uid);
	}
	if(zone.length() > 0){
		SSNPRINTF(ret, query + ret, qr_size - ret, " zone:%s", zone.c_str());
	}
	else if(prov > 0 && prov < MAX_PROVINCE_SLOT_NUM){
		char* provname = NULL;
		char* cityname = NULL;
		MProvince* p = g_server->conf_current->area_info.slots[prov];

		if(p){
			provname = p->name;
		
			if (city > 0 && city < MAX_CITY_SLOT_NUM && p->slots[city]){
				cityname = p->slots[city]->name;
			}
		}
		
		if (provname && cityname){
			SSNPRINTF(ret, query + ret, qr_size - ret, " zone:%s:%s", provname, cityname);
		}else if (provname){
		    SSNPRINTF(ret, query + ret, qr_size - ret, " zone:%s", provname);
		}
	}
	if(*xwhere){
		SSNPRINTF(ret, query + ret, qr_size - ret, " x_where:%s", xwhere + 2);
	}
	if(ids.length() > 0){
		SSNPRINTF(ret, query + ret, qr_size - ret, " x_ids:%s", ids.c_str());
	}
	if(atme > 0 && cuid > 0){	
		SSNPRINTF(ret, query + ret, qr_size - ret, " x_doctype:5.AT%u", cuid);
	}
	else if (words.length() > 0)
	{
		SSNPRINTF(ret, query + ret, qr_size - ret, " words:%s", words.c_str());
	}
	if(dup > 0){
		//ret += sprintf(query + ret, " x_cluster:dup");
	}
	if(trend.length() > 0){
		SSNPRINTF(ret, query + ret, qr_size - ret, " x_trend:%s", trend.c_str());
	}
	if(xcount.length() > 0){
	    SSNPRINTF(ret, query + ret, qr_size - ret, " x_count:%s.8", xcount.c_str());
	}
	if(xbit.length()> 0){
		SSNPRINTF(ret, query + ret, qr_size - ret, " x_count_bit:%s", xbit.c_str());
	}

	//if(xsort.length()> 0){
	//	ret += sprintf(query + ret, " x_sort:%s", xsort.c_str());
	//}

    if (xsort.length() > 0) {
        SSNPRINTF(ret, query + ret, qr_size - ret, " x_user:xsort=%s", xsort.c_str());
    }

    if (info->parameters.getXdebugs().length() > 0) {
        SSNPRINTF(ret, query + ret, qr_size - ret, " x_debugid:%s",  info->parameters.getXdebugs().c_str());
    }
  
    if (info->categoryIdx.length() > 0) {
        SSNPRINTF(ret, query + ret , qr_size - ret, " x_ids:%s", info->categoryIdx.c_str());
    }
	
	if (ret < 0){
		return -1;
	}
	info->params.squery = ac_get_query(info, query + 1, ret);
	info->params.sid = ac_get_query(info, sid.c_str(), strlen(sid.c_str()));
	info->params.uid = cuid;
	info->params.uid_searched = uid;
	info->params.start = start;
	info->params.num = num;
	info->params.xtruncate = xtruncate;
	info->params.isbctruncate = isbctruncate;
   
	if (nofilter != 0 && !(nofilter & 0x1) && !(nofilter & 0x2) && !(nofilter & 0x4) && !(nofilter & 0x8) && !(nofilter & 0x10)){
		nofilter = 0;
	}
	info->params.nofilter = nofilter;
	if(pagesize >= 0){
		info->params.num = pagesize;
		if (page > 0){
			info->params.start = (page - 1) * pagesize;
		}
	}

	if(bcidx != -1){
		info->params.flag |= QSPARAMS_FLAG_BCSPEC;
		info->params.bc_idx = bcidx;
	}
	if(bsidx != -1){
		info->params.flag |= QSPARAMS_FLAG_BSSPEC;
		info->params.bs_idx = bsidx;
	}

	if(isred){
		info->params.flag |= QSPARAMS_FLAG_MARKRED;
	}
	return 0;
}

//?ڴ??????﷨?޸?
int _query_mem_inner(QueryPartHead *head, QueryPart *part, void *args){
	int type = part->type;
	if(type == QUERY_INFO_ARRAY || type == QUERY_INFO_CACHE){	//?ڴ???????֧?ָ??﷨
		return QUERY_CALL_DEL;
	}
	if (type == QUERY_INFO_SORT){
		if (part && strcmp(part->str, "x_sort:digit_time") == 0
				&& (head->info->parameters.getXsort() == std::string("social"))){
			return QUERY_CALL_DEL;
		}
	}

	return 0;
}

int _modify_dbs(QueryPartHead *head, int idx){

	int dbs, pos;
	char buf[256];

	//????dbs?﷨
    uint32_t i;
    
	for (i = 0; i < bcCaller.machines.size(); i ++){
		if (bcCaller.machines[i]->idx == idx){
			break;
		}
	}
	if (i == bcCaller.machines.size()){
		return -1;
	}
	dbs = bcCaller.machines[i]->dbs;
	if(dbs){
		int size, sbit;
		pos = snprintf(buf, 256, "x_dbs:");
		while(dbs){
			sbit = up_power2(size = dbs & (dbs ^ (dbs - 1)));
			dbs -= size;
			pos += snprintf(buf + pos, 256 - pos, "%d,", sbit);
		}
		buf[pos - 1] = 0;
		ac_query_modify(head, QUERY_INFO_DBS, buf);
	}
	return 0;
}

/* 
   实时和精选公用这个下传机制，实时和精选如果都标记了同一个位
   要求csort对应这个位的文件必须相同
*/
int AbtestToCsort(QueryPartHead *head)
{
	if (head == NULL || head->info == NULL)
		return 0;
	int abtest = head->info->parameters.abtest; 
	std::set<int> abtest_csort_del;
	AbtestSplit(abtest_csort_del, conf_get_str(g_server->conf_current, "abtest_csort_del",""));
	for (std::set<int>::iterator it = abtest_csort_del.begin(); it != abtest_csort_del.end(); it++)	{
		if (*it < 0 || *it > MAX_GRAY_NUM)
			continue;
		else
			abtest &=  ~(1 << (*it));
	}
	if(abtest > 0) {
		char buff_abtest[64];
		snprintf(buff_abtest, sizeof(buff_abtest), "x_abtest:%d", abtest);
		ac_query_add(head, QUERY_INFO_ABTEST,buff_abtest);
	}
	return 0;
}

//?ڴ?????
int _query_mem(QueryPartHead *head, int now){
	QsInfo *info = head->info;
	QueryPart *part;
	int pos = 0, nofilter = info->params.nofilter;
	char buf[256];

	//?޸?x_where???ֵ??﷨
	part = head->idx[QUERY_INFO_WHERE];

	pos += sprintf(buf, "digit_time>%d", digittimeHelper->get_digit_time(info->parameters.getKey(), now));
	
	if((nofilter & 0x1) || (nofilter & 0x2)){
		// 当nofilter=2时，不使用非白名单延迟20分钟策略
		ac_query_add(head, QUERY_INFO_WTIME, "x_wtime:0");		
	}

	// 和x_wtime字段配合使用，敏感词过滤
	if(nofilter != 17){
		ac_query_add(head, QUERY_INFO_QIFILTER,"x_qifilter:0");
	}

	char buff_us[64];
	snprintf(buff_us, sizeof(buff_us), "x_us:%d", info->parameters.getUs());
	ac_query_add(head, QUERY_INFO_US, buff_us);

	char buff_cate[64];
	snprintf(buff_cate, sizeof(buff_cate), "x_category:%d", info->parameters.getCategory());
	ac_query_add(head, QUERY_INFO_CATEGORY, buff_cate);
	
	AbtestToCsort(head);

	if (pos > 0){
		ac_query_modify_xwhere(head, buf);
	}

	//?޸?x_cluster
	part = head->idx[QUERY_INFO_CLUSTER];
	if(part && strcmp(part->str, "x_cluster:dup") == 0 && info->categoryIdx.length() == 0){	//???ڲ????? x_cluster:dup
		ac_query_modify(head, QUERY_INFO_CLUSTER, "x_cluster:dup,dup_cont,dup_url");
	}

	//???˵??ڴ???????֧?ֵ??﷨
	ac_query_work(head, _query_mem_inner, NULL);
	return 0;
}

int qualified(char* str, int len){
	if (len == 0){
		return -1;
	}

	if (strstr(str, "-") == str || strstr(str, "\"") == str || strstr(str, "(") == str || strstr(str, "“") == str || strstr(str, "（") == str){
		return -1;
	}
	int i;
	for (i = 0; i < len; i ++){
		if (str[i] == '~' || str[i] == '+'){
			return -1;
		}
	}
	return 0;
}

int _query_his_inner(QueryPartHead *head, QueryPart *part, void *args){
	if(part->type == QUERY_INFO_NORMAL || part->type == QUERY_INFO_FIELD || part->type == QUERY_INFO_TAG){
		if (part->type == QUERY_INFO_NORMAL){
			char query[part->len+1];
			strcpy(query, part->str);
			char* str = query;
			char* p = NULL;
			int len = 0;
            
			while ((p = strchr(str, ' ')) != NULL)
			{
                if (qualified(str, p-str) == 0)
				{
					memmove(part->str + len + 1, str, p-str+1);
					part->str[len] = '+';
					len += p-str+2;
				}
				else
                {
					memmove(part->str + len, str, p-str+1);
					len += p-str+1;
				}
				while((++p)[0] == ' ');

				str = p;
			}
			if (qualified(str, strlen(str)) == 0)
			{
				memmove(part->str + len + 1, str, strlen(str));
				part->str[len] = '+';
				len += strlen(str) + 1;
			}
			else
			{
				memmove(part->str + len, str, strlen(str));
				len += strlen(str);
			}
			part->str[len] = '\0';
			part->left -= len - part->len;
			part->len = len;
		}else{
			char* str = part->str;
			char* p = NULL;
			if (strstr(str, "-") == str || strstr(str, "\"") == str || strstr(str, "(") == str || strstr(str, "“") == str || strstr(str, "（") == str
				|| strstr(str, "+") || strstr(str, "~")){
				return 0;
			}
			
			p = strstr(str, ":");
			if (p && p[1] != '+'){
				memmove(p+2, p+1, strlen(p+1));
				p[1] = '+';
				part->len ++;
				part->left --;
				part->str[part->len] = '\0';
			}
		}
	}
	return 0;
}

//Ӳ?̼???
int _query_hard(QueryPartHead *head, int bcidx){
	char buf[256];
	QueryPart *part;
	int xlen = strlen("x_where:");

	//?????Ƿ???qi?ֶ?
	part = head->idx[QUERY_INFO_WHERE];
	if(part){
		char *query = part->str;
		char *start, *end;
		if(memcmp(query + xlen, "qi", 2) == 0 && !isvar(query + 9)){
			end = strstr(query + xlen, "&&");
			if(end == NULL){
				ac_query_del(head, part);
			}else{
				int len = end + 2 - query - xlen;
				memmove(query + xlen, end + 2, query + part->len - end - 1);
				part->len -= len;
				part->left += len;
			}
		}else{
			start = strstr(query, "&&qi");
			if(start && !isvar(start + 4)){
				end = strstr(start + 2, "&&");
				if(end == NULL){
					*start = 0;
				}else{
					int len = end - start;
					memmove(start, end, query + part->len - end + 1);
					part->len -= len;
					part->left += len;
				}
			}
		}
	}
	
	if(bcidx != ac_conf_dsample_idx() 
			&& !(head->info->params.nofilter & 0x1) && !(head->info->params.nofilter & 0x2)){
		if (!part || !strstr(part->str, "qihd[0:2]")){
			sprintf(buf, "qihd[0:2]==0");
			ac_query_modify_xwhere(head, buf);
		}
	}
	return 0;
}

//С??
int _query_small(QueryPartHead *head, int now){
	QsInfo *info = head->info;
	QueryPart *part;

	int pos = 0, nofilter = info->params.nofilter;
	char buf[256];

	//?޸?x_where???ֵ??﷨
	part = head->idx[QUERY_INFO_WHERE];

	pos += sprintf(buf, "digit_time<%d", digittimeHelper->get_digit_time(info->parameters.getKey(), now));
	
	if((nofilter & 0x1) || (nofilter & 0x2)){
		// 当nofilter=2时，不使用非白名单延迟20分钟策略
		ac_query_add(head, QUERY_INFO_WTIME, "x_wtime:0");		
	}

	// 和x_wtime字段配合使用，敏感词过滤
	if(nofilter != 17){
		ac_query_add(head, QUERY_INFO_QIFILTER,"x_qifilter:0");
	}

	char buff_us[64];
	snprintf(buff_us, sizeof(buff_us), "x_us:%d", info->parameters.getUs());
	ac_query_add(head, QUERY_INFO_US, buff_us);
	char buff_cate[64];
	snprintf(buff_cate, sizeof(buff_cate), "x_category:%d", info->parameters.getCategory());
	ac_query_add(head, QUERY_INFO_CATEGORY, buff_cate);
	
	AbtestToCsort(head);

	if (pos > 0){
		ac_query_modify_xwhere(head, buf);
	}

	//?޸?x_cluster
	part = head->idx[QUERY_INFO_CLUSTER];
	if(part && strcmp(part->str, "x_cluster:dup") == 0 && info->categoryIdx.length() == 0){	//???ڲ????? x_cluster:dup
		ac_query_modify(head, QUERY_INFO_CLUSTER, "x_cluster:dup,dup_cont,dup_url");
	}

	//???˵??ڴ???????֧?ֵ??﷨
	ac_query_work(head, _query_mem_inner, NULL);
}

//?п?
/*
int _query_middle(QueryPartHead *head){
//	ac_query_modify(head, QUERY_INFO_DBS, "x_dbs:2,3");
	_query_hard(head, BC_MIDDLE);
//	ac_query_work(head, _query_his_inner, NULL);
	return 0;
}
*/
//??????????
int _query_sample_inner(QueryPartHead *head, QueryPart *part, void *args){
	if(part->type != QUERY_INFO_NORMAL && part->type != QUERY_INFO_FIELD && part->type != QUERY_INFO_WHERE && 
		part->type != QUERY_INFO_DOCPHRASE && part->type != QUERY_INFO_DUID && part->type != QUERY_INFO_TAG && 
		part->type != QUERY_INFO_DOCTYPE &&	part->type != QUERY_INFO_PHRASE && part->type != QUERY_INFO_IDS){
		return QUERY_CALL_DEL;
	}else if (part->type == QUERY_INFO_NORMAL){
		int count = trim_char(part->str, part->len, " ~", 2);
		part->len -= count;
		part->left += count;
	}
	return 0;
}

int _query_m_inner(QueryPartHead* head, QueryPart* part, void* args){
	if (part->type == QUERY_INFO_CLUSTER || part->type == QUERY_INFO_WTIME){
		return QUERY_CALL_DEL;
	}
	return 0;
}

int _query_m_sample(QueryPartHead* head){
	int now = (int)time(NULL);
	_query_mem(head, now);
	ac_query_work(head, _query_m_inner, NULL);
    return 0;
}

//??????
int _query_sample(QueryPartHead *head){
	time_t now = time(NULL);
	char buf[256];
	int pos = snprintf(buf, sizeof(buf), "digit_time<%d",
			digittimeHelper->get_digit_time(head->info->parameters.getKey(), now));
	if (pos > 0) {
		ac_query_modify_xwhere(head, buf);
	}

	ac_query_work(head, _query_sample_inner, NULL);
	_query_hard(head, ac_conf_dsample_idx());
	return 0;
}

//??ʷ??
int _query_history(QueryPartHead *head, int idx, int now){
//	ac_query_modify(head, QUERY_INFO_DBS, "x_dbs:1,2,3,4,5");
	QsInfo *info = head->info;
	QueryPart *part;
	int pos = 0, nofilter = info->params.nofilter;
	char buf[256];
	//?޸?x_where???ֵ??﷨
	part = head->idx[QUERY_INFO_WHERE];
	pos += sprintf(buf, "digit_time<%d", digittimeHelper->get_jx_his_digit_time(info->parameters.getKey(), now));

	if((nofilter & 0x1) || (nofilter & 0x2)){
		// 当nofilter=2时，不使用非白名单延迟20分钟策略
		ac_query_add(head, QUERY_INFO_WTIME, "x_wtime:0");		
	}

	// 和x_wtime字段配合使用，敏感词过滤
	if(nofilter != 17){
		ac_query_add(head, QUERY_INFO_QIFILTER,"x_qifilter:0");
	}

	char buff_us[64];
	snprintf(buff_us, sizeof(buff_us), "x_us:%d", info->parameters.getUs());
	ac_query_add(head, QUERY_INFO_US, buff_us);

	char buff_cate[64];
	snprintf(buff_cate, sizeof(buff_cate), "x_category:%d", info->parameters.getCategory());
	ac_query_add(head, QUERY_INFO_CATEGORY, buff_cate);
	
	AbtestToCsort(head);
	int jx_his_time_switch = conf_get_int(g_server->conf_current, "jx_his_time_switch", 0);
	if(strcmp(get_bcgroup_name(idx), "jx_history") == 0) {
		if (pos > 0 && jx_his_time_switch == 1){
			ac_query_modify_xwhere(head, buf);
		}
	}
	//?޸?x_cluster
	part = head->idx[QUERY_INFO_CLUSTER];
	if(part && strcmp(part->str, "x_cluster:dup") == 0
       && info->categoryIdx.length() == 0){	//???ڲ????? x_cluster:dup
		ac_query_modify(head, QUERY_INFO_CLUSTER, "x_cluster:dup,dup_cont,dup_url");
	}

	//???˵??ڴ???????֧?ֵ??﷨
	ac_query_work(head, _query_mem_inner, NULL);
	return 0;
}

//???ݲ?ͬ??BC???ɲ?ͬ?Ĳ?ѯ??
int _query_inner(QueryPartHead *head, int idx){
	int now = (int)time(NULL);
	if(idx == ac_conf_mem_idx()){	//?ڴ?????
		_query_mem(head, now);
	}else if(idx == ac_conf_month_idx()){	//С??
		_query_small(head, now);
	}else if(idx == ac_conf_msample_idx()){
		_query_m_sample(head);
	}else if(idx == ac_conf_dsample_idx()){	//??????
		_query_sample(head);
	}else{	//?????Ķ?????ʷ??
		_query_history(head, idx, now);
	} 
	return 0;
}

int _second_normal_search(QueryPartHead *head, QueryPart *part, void *args){
	if(part->type == QUERY_INFO_SORT){
		if (strcmp(part->str, "x_sort:digit_time") == 0){
			return QUERY_CALL_DEL;
		}
	}else if (part->type == QUERY_INFO_CACHE){
		if (strcmp(part->str, "x_cache:notget") == 0){
			return QUERY_CALL_DEL;
		}
	}else if (part->type == QUERY_INFO_CLUSTER){
		if (strcmp(part->str, "x_cluster:dup") == 0){
			return QUERY_CALL_DEL;
		}
	}else if (part->type == QUERY_INFO_UID || part->type == QUERY_INFO_MARK){
		return QUERY_CALL_DEL;
	}

	return 0;
}

 
int _second_history_search(QueryPartHead *head, QueryPart *part, void *args){
	if (part->type == QUERY_INFO_CACHE){
		if (strcmp(part->str, "x_cache:notget") == 0){
			return QUERY_CALL_DEL;
		}
	}else if (part->type == QUERY_INFO_CLUSTER){
		if (strcmp(part->str, "x_cluster:dup") == 0){
			return QUERY_CALL_DEL;
		}
	}else if (part->type == QUERY_INFO_UID || part->type == QUERY_INFO_MARK){
		return QUERY_CALL_DEL;
	}
	return 0;
}

static int _query_second(QueryPartHead* head, int idx){
	int now = (int)time(NULL);
	if(idx == ac_conf_mem_idx()){		
		_query_mem(head, now);
		ac_query_work(head, _second_normal_search, NULL);
		_modify_dbs(head, idx);
	}else if(idx == ac_conf_month_idx()){
		_query_small(head, now);
		ac_query_work(head, _second_normal_search, NULL);
		ac_query_add(head, QUERY_INFO_SORT, "x_sort:digit_time");
		_modify_dbs(head, idx);
	}else{
		_query_history(head, idx, now);
		ac_query_work(head, _second_history_search, NULL);
		_modify_dbs(head, idx);
	}
	ac_query_add(head, QUERY_INFO_DEGRADE, "x_degrade:1");

	return 0;
}

QsDAQuerys* query_bc(QsInfo *info, int idx){
	QsDAQuerys *da_query;
	QsQuery *qs_query;
	QsQueryData *query_data;
	char *query;
	int daflag = 0;
	char *buffer = info->buffer + info->buf_pos;
	QueryPartHead *head = (QueryPartHead*)buffer;

	da_query = (QsDAQuerys*)ac_malloc(info, sizeof(QsDAQuerys));
	memset(da_query, 0, sizeof(QsDAQuerys));
	da_query->level = info->bcinfo.level;
	da_query->research = info->bcinfo.research;

	ac_query_split(head, info->params.query, info);
	if(info->second_search == 0 ){
		if(conf_get_int(info->conf, "query_change", CONF_QUERY_CHANGE) == CONF_QUERY_CHANGE){
			_query_inner(head, idx);
		}
		_modify_dbs(head, idx);
	}else{
		_query_second(head, idx);		
	}
	query = ac_query_concat(head);
	da_query->query = ac_get_query(info, query, strlen(query));

	ac_query_split(head, da_query->query, info);
	ac_query_work(head, _query_syntax, NULL);
	int cmd = DA_CMD_SYNTAXPARSE_NEW;
	query = ac_query_concat(head);
	if(*query){
		int query_len = strlen(query);
		query[query_len + 1] = daflag;
		query[query_len + 2] = cmd;
		qs_query = (QsQuery*)(ac_get_query(info, query, query_len + 3) - offsetof(QsQuery, query));
		if(qs_query->other == NULL){
			query_data = (QsQueryData*)ac_malloc(info, sizeof(QsQueryData));
			memset(query_data, 0, sizeof(QsQueryData));
			query_data->flag = cmd;
			query_data->daflag = daflag;
			query_data->query = qs_query->query;
			da_query->syntax = query_data;
			qs_query->other = query_data;
			query_data->next = info->dainfo.cur_data;
			info->dainfo.cur_data = query_data;
		}else{
			da_query->syntax = (QsQueryData*)qs_query->other;
		}
	}

	ac_query_split(head, da_query->query, info);
	ac_query_work(head, _query_logic, NULL);
	cmd = DA_CMD_LOGICPARSE_NEW;
	query = ac_query_concat(head);
	if(*query){
		int query_len = strlen(query);
		query[query_len + 1] = daflag;
		query[query_len + 2] = cmd;
		qs_query = (QsQuery*)(ac_get_query(info, query, query_len + 3) - offsetof(QsQuery, query));
		if(qs_query->other == NULL){
			query_data = (QsQueryData*)ac_malloc(info, sizeof(QsQueryData));
			memset(query_data, 0, sizeof(QsQueryData));
			query_data->flag = cmd;
			query_data->daflag = daflag;
			query_data->query = qs_query->query;
			da_query->logic = query_data;
			qs_query->other = query_data;
			query_data->next = info->dainfo.cur_data;
			info->dainfo.cur_data = query_data;
		}else{
			da_query->logic = (QsQueryData*)qs_query->other;
		}
	}
	return da_query;
}

int _query_mblog_search(QueryPartHead *head, QueryPart *part, void *args){
	if(part->type != QUERY_INFO_NORMAL && part->type != QUERY_INFO_FIELD && part->type != QUERY_INFO_TAG){
		return QUERY_CALL_DEL;
	}
	if(part->type == QUERY_INFO_FIELD || part->type == QUERY_INFO_TAG){
		char *q = strchr(part->str, ':') + 1;
		while(*q && (*q == '*' || *q == '+' || *q == '-')){
			q ++;
		}
		if(*q == 0){
			return QUERY_CALL_DEL;
		}
		part->len -= q - part->str;
		part->str = q;
	}
	return 0;
}

char* query_filter_pre(QsInfo *info, int idx, int *dict){
	*dict = (1 << 1) + (1 << 2) + (1 << 3) + (1 << 4) + (1 << 5);
	*dict >>= 1;
	char *buffer = info->buffer + info->buf_pos;
	QueryPartHead *head = (QueryPartHead*)buffer;
	ac_query_split(head, info->params.query, info);
	ac_query_work(head, _query_mblog_search, &idx);
	return ac_query_concat(head);
}

char* query_filter_after(QsInfo *info){
	if (strstr(info->params.query, "status==") == NULL){
 		// 增加status字段
 		char *buffer = info->buffer + info->buf_pos;
 		QueryPartHead *head = (QueryPartHead*)buffer;
 		ac_query_split(head, info->params.query, info); 
 		ac_query_modify_xwhere(head, "status==1");
		//��ʶstatus=1���д�
		change_mark(head,201);
 		return ac_query_concat(head);
	} else {
		return NULL;
	}


}

//?ִ?ǰ

char* query_lexicon_pre(QsInfo *info){
	char *buffer = info->buffer + info->buf_pos;
	QueryPartHead *head = (QueryPartHead*)buffer;
    if (info->params.key) {
         ac_query_split(head, info->params.key, info);
    } else {
         ac_query_split(head, info->params.squery, info);
    }
	ac_query_work(head, _query_mblog_search, NULL);
	return ac_query_concat(head);
}





