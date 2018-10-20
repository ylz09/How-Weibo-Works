#include "ac_include.h"
#include "ac_query_lib.h"
#include "ac_miniblog.h"
#include "unicode.h"
#include "assert.h"
#include "string_utils.h"
#include "ac_tool.h"
#include "ac_sort_model.h"
#include "call_data.h"
#include <dacore/auto_reference.h>
#include "ac_intervene_dict.h"

#define LOC_COUNT	128

int get_query_count(char* query, int len);


extern "C" int _de(const unsigned char *p, unsigned short *codep);
extern "C" int _en(unsigned short code, unsigned char *p);


typedef int loop_call(QueryPartHead*, QueryPart*, void*);

int grouping(const char *str, unsigned gnum)
{
	if (!str || !*str || !gnum) return -1;

	uint64_t h = 0;
	do {
		h = (h << 7) - h + *((unsigned char *)str) + 987654321L;
		h &= 0x0000ffffffffL;
	} while (*(++str));

	return h % gnum;
}

int del_char(char* origin, char* deleted, char* inserted)
{
	int count = 0;

	if (origin == NULL || deleted == NULL || inserted == NULL)
	{
		return count;
	}

	char* p0 = NULL;	
	char* p1 = NULL;
	char* p2 = NULL;
	int len = strlen(deleted);
	int step = strlen(inserted);
	int size = 0;

	p1 = strstr(origin, deleted);
	if (p1 == NULL)
	{
		return count;
	}

	p0 = p1;
	p1 += len;
	while (1)
	{
		p2 = strstr(p1, deleted);
		if (p2 == NULL)
		{
			size = strlen(p1);
			memmove(p0+step, p1, size);
			memcpy(p0, inserted, step);
			p0[size+step] = '\0';
			
			count ++;
			break;
		}
		
		size = strlen(p1);
		memmove(p0+step, p1, size);
		p0[step+size] = '\0';
		memcpy(p0, inserted, step);
		p0 = p2 - len + step;
		p1 = p0 + len;

		count ++;
	}
	return count;
}

void insert_char(char* origin, char* insert)
{
	int orilen = strlen(origin);
	int len = strlen(insert);
	memmove(origin+len, origin, strlen(origin));
	memmove(origin, insert, len);
	origin[orilen+len] = '\0';
}

int split_char(char* origin, char* sub, char* loc[], int size)
{
	int count = 0;
	int len = strlen(sub);

	char* start = origin;
	char* end = NULL;
	while ((end = strstr(start, sub)) && count < size)
	{
		if (start != end)
		{
			loc[count++] = start;
			end[0] = '\0';
		}
		start = end + len;
	}
	
	if (count == size && start)
	{
		char* last = NULL;
		while ((end = strstr(start, sub)) != NULL)
		{
			if (start != end)
			{
				last = start;
				end[0] = '\0';
			}
			start = end + len;
		}
		if (start[0] != '\0')
		{
			last = start;
		}
		loc[count-1] = last;
	}
	else
	{
		if (start[0] != '\0')
		{
			loc[count++] = start;
		}
	}
	return count;
}

int move_char(char* loc[], int src, char* dest, char sep)
{
	int len = strlen(loc[src]);
	if (loc[src] <= dest)
	{
		dest[len++] = sep;
		return len;
	}

	// ??????˳???ƶ???????ֻ??ά??dest+1λ?õ?ָ????ַ
	memmove(dest, loc[src], len);
	dest[len++] = sep;
	return len;
}



void del_pair(char* str, int* len, int* index, char start, char end)
{
	int words;
	int i = *index+1;
	int leftlen;
	int span;
	while (i < *len)
	{
		if (str[i] == start)
		{
			del_pair(str, len, &i, start, end);
		}
		else if (str[i] == end)
		{
			words = get_query_count(str+(*index+1), i-(*index+1));
			if (words <= 8)
			{
				leftlen = *len - i - 1;	
				if (leftlen > 0)
				{
					if (*index > 0)
					{
						memmove(str+(*index+1), str+i+1,leftlen);
						str[*index] = ' ';
						span = i - *index;

						*index += 1;
					}
					else
					{
						memmove(str+*index, str+i+1, leftlen);
						span = i - *index + 1;
					}
				}
				else
				{
					span = i - *index + 1;
				}
				*len -= span;
			}
			else
			{
				*index = i + 1;
			}
			break;
		}
		else
		{
			i ++;
		}
	}
	*index = i;
}

void del_expression(QueryPart* part)
{
	if (part == NULL)
	{
		return;
	}
	int orilen = part->len;
	int i = 0;
	while (i < part->len)
	{
		if (part->str[i] == '[')
		{
			del_pair(part->str, (int*)&part->len, &i, '[', ']');
		}
		else
		{
			i ++;
		}
	}
	part->left += orilen - part->len;
	part->str[part->len] = '\0';
}

void filter_x_query(char* query, char* filter)
{
	char* loc[LOC_COUNT];
	int count = split_char(query, " ", loc, LOC_COUNT);
	int len = strlen(filter);
	int i = 0;
	int newlen = 0;
	while (i < count)
	{
		while (i < count && memcmp(loc[i], filter, len) == 0)	i ++;

		if (i < count)
		{
			newlen += move_char(loc, i, query+newlen, ' ');
			i ++;
		}
		else
		{
			query[newlen] = '\0';
			return;
		}
	}
	query[newlen-1] = '\0';
}

void trim_from_querypart(QueryPart* part, char* deleted)
{
	if (part == NULL)
	{
		return;
	}

	int count = trim_char(part->str, part->len, deleted, strlen(deleted));
	part->len -= count;
	part->left += count;
}

void del_from_querypart(QueryPart* part, char* deleted, char* inserted)
{
	if (part == NULL)
	{
		return;
	}
	int count = del_char(part->str, deleted, inserted);

	int len = (strlen(deleted) - strlen(inserted)) * count;
	part->len -= len;
	part->left += len;
}

void get_two_random(int count, int* random1, int* random2)
{
	srand((unsigned int)(time((time_t*)NULL)));

	*random1 = rand() % (count - 3) + 1;
	*random2 = rand() % (count - 4) + 1;

	if (*random2 >= *random1)
	{
		(*random2) ++;
	}
	else
	{
		int temp = *random1;
		*random1 = *random2;
		*random2 = temp;
	}
}

void cutoff_from_querypart(QueryPart* part, char* p)
{
	char* loc[LOC_COUNT];
	int pos[4];
	int i;
	int len = 0;
	int count = split_char(part->str, p, loc, LOC_COUNT);
	if (count <= 4)
	{
		for (i = 0; i < count; i ++) 
		{
			pos[i] = i;
		}
	}
	else
	{
		int random1, random2;
		get_two_random(count, &random1, &random2);
		pos[0] = 0;
		pos[1] = random1;
		pos[2] = random2;
		pos[3] = count - 1;
		count = 4;
	}

	for (i = 0; i < count; i ++)
	{
		len += move_char(loc, pos[i], part->str+len, '~');
	}
	len -= 1;
	part->str[len] = '\0';

	part->left += part->len - len;
	part->len = len;
}

int get_query_count(char* query, int len)
{
	int count = 0;

	int i = 0;
	uint8_t b;
	while (i < len)
	{
		b = query[i];

		if (b < 128)
		{
			i += 1;
		}
		else if ((b & 0xF8) == 0xF0)
		{
			i += 4;
		}
		else if ((b & 0xF0) == 0xE0)
		{
			i += 3;
		}
		else if ((b & 0xE0) == 0xC0)	
		{
			i += 2;
		}
		else
		{
			i += 1;
		}
		count ++;
	}
	return count;
}

void insert_before_querypart(QueryPart* part, char* p)
{
	insert_char(part->str, p);

	int len = strlen(p);
	part->len += len;
	part->left -= len;
}

void handle_sort(QueryPart* part, QueryPartHead* head)
{
	if (part == NULL || head == NULL)
	{
		return;
	}

	int count = get_query_count(part->str, part->len);
	//if (count == 2 || count == 3)
	//{
	//	if (part->str[0] != '+')
	//	{
	//		insert_before_querypart(part, "+");
	//	}
	//} else
	if (count == 1)
	{
		QueryPart* sort = head->idx[QUERY_INFO_SORT];
		if (sort == NULL)
		{
			return;
		}

		//if (strcmp(sort->str, "x_sort:social") == 0)
		//{
		//	ac_query_modify(head, QUERY_INFO_SORT, "x_sort:digit_time");	
		//}
	}	
}

int modify_words_querypart(QueryPartHead* head, QueryPart* part, void* args)
{
	char* words = "words:";
	char* pre = "x_doctype:5.";

	if (part && part->type == QUERY_INFO_FIELD)
	{
		if (strncmp(part->str, words, strlen(words)) == 0)
		{
//			insert_before_querypart(part, pre);
			int size = strlen(part->str)-strlen(words)+strlen(pre)+1;
			char str[size];
			snprintf(str, size, "%s%s", pre, part->str+strlen(words));

			ac_query_del(head, part);
			ac_query_add(head, QUERY_INFO_DOCTYPE, str);
				
			return QUERY_CALL_STOP;
		}
	}
	return 0;
}

int modify_zone_querypart(QueryPartHead* head, QueryPart* part, void* args)
{
	char* zone = "zone:";
	char* separate = ":";
	char* city = "其他";

	if (part && part->type == QUERY_INFO_FIELD)
	{
		int len = strlen(zone);
		if (strncmp(part->str, zone, len) == 0)
		{
			char* p = strstr(part->str+len, separate);
			if (p)
			{	
				if (strcmp(p+1, city) != 0)
				{
					part->str[len] = '+';
					int citylen = strlen(p+1);
					memmove(part->str+len+1, p+1, citylen);
					part->str[len+1+citylen] = '\0';

					part->len -= len - 1;
					part->left += len - 1;
				}
			}
			return QUERY_CALL_STOP;		
		}
	}
	return 0;
}

/**
 * if if uid != NULL means people want to search 
 */
int modify_uid_querypart(QueryPartHead* head, QueryPart* part, void* arg)
{
	char* uid = "uid:";

	if (part && part->type == QUERY_INFO_DUID)
	{
		int len = strlen(uid);
		if (strncmp(part->str, uid, len) == 0)
		{
			int bsidx = grouping(part->str+len, MAX_GROUP_NUM);		

			head->info->params.flag |= QSPARAMS_FLAG_BSSPEC;
			head->info->params.bs_idx = bsidx;
			
			if (head->info->params.flag & QSPARAMS_FLAG_BCSPEC) {
				head->info->params.bc_idx &= ac_conf_all_normal_idx();
			}else{
				head->info->params.flag |= QSPARAMS_FLAG_BCSPEC;
                head->info->params.bc_idx = ac_conf_all_normal_idx();
            }
		}
		return QUERY_CALL_STOP;
	}
	return 0;
}

void modify_fields(QueryPartHead* head, loop_call call)
{
	if (head == NULL)
	{
		return;
	}
	ac_query_work(head, call, NULL);	
}

void modify_filter(QueryPartHead* head)
{
// 不再根据sid选择nofilter
/*
	if (head->info->params.nofilter == 0)
	{
		char* sid[] = {"1-1-main_topic",
						"2-1-wap",
						"t_wap",
						"t_wap_c",
						"t_search",
						"1-3-zhuanti",
						"hottopic_num",
						"s_newtips",
						"t_search_nologin"};
	
		int i;
		int count = sizeof(sid) / sizeof(char*);
		for (i = 0; i < count; i ++)
		{
			if (strcmp(head->info->params.sid, sid[i]) == 0)
			{
				break;
			}
		}
		if (i == count)
		{
			head->info->params.nofilter = 0x2;
		}
*/
	if (head->info->params.nofilter & 0x4){
		char audit[] = "audit==0";
		ac_query_modify_xwhere(head, audit);
	}
}

int modify_nox_querypart(QueryPartHead* head, QueryPart* part, void* args)
{
	if (part && (part->type == QUERY_INFO_NORMAL || part->type == QUERY_INFO_FIELD || part->type == QUERY_INFO_DOCPHRASE || part->type == QUERY_INFO_TAG))
	{
		char* p = NULL;

		p = "~";
		cutoff_from_querypart(part, p);
	
		p = "@";
		del_from_querypart(part, p, " ");

		p = "#";
		del_from_querypart(part, p, " ");

		p = "*";
		del_from_querypart(part, p, " ");

		p = "//";
		del_from_querypart(part, p, " ");

		p = " ";
		trim_from_querypart(part, p);
	}
	return 0;
}

int full_to_half(unsigned char* utf8, unsigned bytes)
{
	unsigned short ucs2;                //decode into ucs2
	unsigned int _bytes = 0;            //decode bytes
	unsigned int _bytes2 = 0;           //encode bytes
	while (_bytes < bytes && *(utf8 + _bytes)) {
	    int clen = _de(utf8 + _bytes, &ucs2);
	    if (clen == 3) {
			if (ucs2 > 0xFF00 && ucs2 < 0xFF5F) // full conner
			    ucs2 -= 0xFEE0;
			else if (ucs2 == 0x3000) //zh blank
			    ucs2 = 0x0020;
			else if (ucs2 == 0x3002) //zh dot
			    ucs2 = 0x002E;			
	    }else if (clen < 0){
			_bytes += -clen;
			continue;
		}else if (clen == 0){
			break;
		}
	    int clen2 = _en(ucs2, utf8 + _bytes2);
	    assert(clen2 > 0 && clen2 <= clen);
	    _bytes += clen;
	    _bytes2 += clen2;
	}
	while ( *(utf8 + _bytes2 - 1) == ' ' || *(utf8 + _bytes2 - 1) == '\t' )  {
	    _bytes2 --;
	}	

	*(utf8 + _bytes2) = '\0';
	return _bytes2;
}

int modify_code_querypart(QueryPartHead* head, QueryPart* part, void* args)
{
	if (part && (part->type == QUERY_INFO_NORMAL || part->type == QUERY_INFO_FIELD || 
		part->type == QUERY_INFO_DOCTYPE || part->type == QUERY_INFO_DOCPHRASE || part->type == QUERY_INFO_TAG))
	{
		int len = strlen(part->str);
		int byte = full_to_half((unsigned char*)part->str, len);
		part->len += byte - len;
		part->left -= byte - len;
	}
	return 0;
}

int modify_query_querypart(QueryPartHead* head, QueryPart* part, void* args)
{
	if (part && (part->type == QUERY_INFO_NORMAL || part->type == QUERY_INFO_DOCPHRASE || part->type == QUERY_INFO_TAG))
	{
		char* p = "搜索微博、找人";
		del_from_querypart(part, p, "");

		// 去掉表情
		del_expression(part);

		handle_sort(part, head);
	}
	return 0;
}

int getminlimit(char* where)
{
	char* pre = "digit_time>";
	char* p = strstr(where, pre);

	if (p)
	{
		p += strlen(pre);
		if (p[0] == '=')
		{
			p ++;
		}
		int i = 0;
		char time[16];

		while (*p >= 48 && *p <= 57)
		{
			time[i++] = *p ++;
		}
		time[i] = '\0';

		return atoi(time);
	}
	return -1;
}

void modify_time(QueryPartHead* head)
{
	if (head == NULL)
	{
		return;
	}
	QueryPart* part = head->idx[QUERY_INFO_WHERE];

	if (part)
	{
		int start = getminlimit(part->str);

		if (start == -1)
		{
			return;
		}
		
		int cur = time((time_t*)NULL);
		int hour_bf = digittimeHelper->get_digit_time(head->info->parameters.getKey().c_str(), cur);	// ��Сʱ??
		int week_bf = cur - 21*24*3600;		// ??????
		
		if (start > hour_bf)	
		{
			if (head->info->params.flag & QSPARAMS_FLAG_BCSPEC)
			{
				head->info->params.bc_idx &= 1 << ac_conf_mem_idx();
			}
			else
			{	
				head->info->params.flag |= QSPARAMS_FLAG_BCSPEC;	
				head->info->params.bc_idx = 1 << ac_conf_mem_idx();
			}
		}
		else if (start > week_bf)
		{
			if (head->info->params.flag & QSPARAMS_FLAG_BCSPEC)
			{
				head->info->params.bc_idx &= (1 << ac_conf_mem_idx()) | (1 << ac_conf_month_idx());
			}
			else
			{
				head->info->params.flag |= QSPARAMS_FLAG_BCSPEC;
				head->info->params.bc_idx = (1 << ac_conf_mem_idx()) | (1 << ac_conf_month_idx());
			}
		}
	}
}

void change_mark(QueryPartHead* head, int mark_value)
{
	int xmark;
	char str[16];
	QueryPart* part = head->idx[QUERY_INFO_MARK];

	if (part)
	{
		xmark = atoi(part->str+strlen("x_mark:"));
		if (xmark < mark_value)
		{
			xmark += mark_value;
			snprintf(str, 16, "x_mark:%d", xmark);
			ac_query_modify(head, QUERY_INFO_MARK, str);
		}
	}
	else
	{
		xmark = mark_value;
		snprintf(str, 16, "x_mark:%d", xmark);
		ac_query_add(head, QUERY_INFO_MARK, str);
	}

}

void modify_mark(QueryPartHead* head)
{
	if (head == NULL)
	{
		return;
	}

	if (head->info->params.nofilter & 0x1)
	{	
		change_mark(head, 5000);
	}
	if (head->info->params.nofilter & 0x10)
	{
		change_mark(head, 10000);
	}
	if (head->info->parameters.getCategory() == DOC_MEDIA_USER) {
		change_mark(head, 1000);
	} else if (head->info->parameters.getCategory() == DOC_FAMOUS_USER) {
		change_mark(head, 2000);
	}
}

void modify_sort(QueryPartHead* head)
{
	if (head == NULL)
	{
		return;
	}

	QueryPart* sort = head->idx[QUERY_INFO_SORT];

	if (sort == NULL)
	{
	//	ac_query_add(head, QUERY_INFO_SORT, "x_sort:digit_time");
	}	
	else
	{
		if (strcmp(sort->str, "x_sort:social") == 0)
		{
			// we don't use flag to decide the social flag, use info->parameters instead
			//head->info->params.flag |= QSPARAMS_FLAG_SOCIAL;
			//ac_query_modify(head, QUERY_INFO_SORT, "x_sort:digit_time");
		}
		else if (strcmp(sort->str, "x_sort:digit_time") != 0)
		{
			//ac_query_modify(head, QUERY_INFO_SORT, "x_sort:digit_time");
		}
	}
}

void modify_trend(QueryPartHead* head)
{
	if (head == NULL)
	{
		return;
	}

	QueryPart* trend = head->idx[QUERY_INFO_TREND];

	if (trend)
	{
		if (head->idx[QUERY_INFO_SORT])
		{
			ac_query_del(head, head->idx[QUERY_INFO_SORT]);
		}
		if (head->idx[QUERY_INFO_MARK])
		{
			ac_query_del(head, head->idx[QUERY_INFO_MARK]);
		}
	}
}

void modify_general(QueryPartHead *head, QsInfo * info)
{
	QueryPart* part = NULL;

	//过滤掉x_cache:语法
	part = head->idx[QUERY_INFO_CACHE];
	if(part){
		if (strcmp(part->str, "x_cache:notget") == 0){ 
			head->info->params.flag |= QSPARAMS_FLAG_BSNOGET; 
		} else if (strcmp(part->str, "x_cache:notput") == 0){
			head->info->params.flag |= QSPARAMS_FLAG_BSNOPUT; 
		} 
		ac_query_del(head, part);
	}

	//过滤掉x_dbs:语法
	part = head->idx[QUERY_INFO_DBS];
	if(part){
		int flag = atoi(part->str + strlen("x_dbs:"));
		uint32_t bc_idx = 0;
		if(flag == 4 || flag == 5 || flag == 2 || flag == 3){ 
			bc_idx |= (1 << ac_conf_month_idx());
		} 

		if(bc_idx)
		{
			if(head->info->params.flag & QSPARAMS_FLAG_BCSPEC)
			{
				head->info->params.bc_idx &= bc_idx;
			}
			else
			{
				head->info->params.flag |= QSPARAMS_FLAG_BCSPEC;
				head->info->params.bc_idx = bc_idx;
			}
		}   
		ac_query_del(head, part);
	}   

	//过滤x_uid语法
	part = head->idx[QUERY_INFO_UID];
	if(part){
		// we don't use flag to decide the social flag, use info->parameters instead
		//head->info->params.flag |= QSPARAMS_FLAG_SOCIAL;
		head->info->params.uid = strtoul(part->str + 6, NULL, 0);
		ac_query_del(head, part);
	}

	//过滤x_sort:social语法
	part = head->idx[QUERY_INFO_SORT];
	if(part){
		if (strcmp(part->str, "x_sort:social") == 0){
			// we don't use flag to decide the social flag, use info->parameters instead
			//head->info->params.flag |= QSPARAMS_FLAG_SOCIAL;
			ac_query_modify(head, QUERY_INFO_SORT, "x_sort:digit_time");
		}
	} 

	//过滤uid语法
	modify_fields(head, modify_uid_querypart);

	// x_truncate
	if (!head->idx[QUERY_INFO_DUID] && info->params.isbctruncate){
		ac_query_add(head, QUERY_INFO_TRUNCATE, "x_truncate:1");
	}

	// set bit which mark important sid
	if (sid_match(head->info->params.sid, &head->info->conf->value_sid) != 0){
		head->info->params.flag |= QSPARAMS_FLAG_SID;
	}

	head->info->params.ori_bc_idx = head->info->params.bc_idx;
}

static void modify_sid(QueryPartHead* head)
{
	if (head == NULL) {
		return;
	}
    auto_reference<AcSidDelay> sid_delay("sid_delay.txt");
    std::string data_prefix = "x_sid_delay:";
    std::string data = "30-30";
    if (*sid_delay) {
        sid_delay->find(head->info->parameters.sid, &data);
    }
    data_prefix.append(data);
    QueryPart* part = head->idx[QUERY_INFO_SID];
    if (part) {
        ac_query_modify(head, QUERY_INFO_SID, (char*)data_prefix.c_str());
    } else {
        ac_query_add(head, QUERY_INFO_SID, (char*)data_prefix.c_str());
    }
}
static void modify_dup(QueryPartHead* head)
{
	if (head == NULL) {
		return;
	}
	int dup = head->info->parameters.dup;
	char str[16];
    snprintf(str, 16, "x_dup:%d", dup);
	QueryPart* part = head->idx[QUERY_INFO_DUP];
	if (part) {
        ac_query_modify(head, QUERY_INFO_DUP, str);
    } else {
        ac_query_add(head, QUERY_INFO_DUP, str);
	}
}

//??ѯ?ʱ?׼??
char* query_work_pre(QsInfo *info, char* query){
	char *buffer = info->buffer + info->buf_pos;
	QueryPartHead *head = (QueryPartHead*)buffer;
	ac_query_split(head, query, info);
	//todo ????
	// ??????ѯ??
	if(conf_get_int(info->conf, "query_change", CONF_QUERY_CHANGE) == CONF_QUERY_CHANGE){
		modify_fields(head, modify_nox_querypart);

		modify_fields(head, modify_query_querypart);
	
		modify_fields(head, modify_code_querypart);

		// words?ֶβ?ѯ?﷨Ϊ"x_doctype:5."
		modify_fields(head, modify_words_querypart);      

		// ?????????˲?ѯʱ???Ĳ?ѯ????????ָ???˿?ʼʱ?䣬????��??Сʱ?ڣ???ֻ?????ڴ棬??ָ?????????ڣ???ֻ???ڴ??͵?һ?㣬?????㲻??????
		modify_time(head);

		// ????sid??????1-1-main_topic, 2-1-wap, t_wap, t_wap_c, t_search, 1-3-zhuanti, hottopic-num, s_newtips, ????nofilter=2 
		modify_filter(head);

		modify_mark(head);

		modify_sort(head);

		modify_trend(head);
		// ?????˶?zone?ֶεĲ?ѯ?Ż???????ѯ??????zone:?﷨ʱ????zone:?㶫:???? ???Ƶ??﷨??????zone:+???ݣ?
		// ????zone:ĳʡ:???? ????zone:ĳʡ ????zone:ĳ?в????ı?
		modify_fields(head, modify_zone_querypart);	

		modify_dup(head);

        if (conf_get_int(info->conf, "use_sid_delay", 1)) {
            modify_sid(head);
        }
	}

	// general modify
	modify_general(head, info);

	return ac_query_concat(head);
}
/*
//?????ֶ????֣??õ??ֶε???Ϣ
MField* field_get(MFieldInfo *info, char *label){
    int k_len = strlen(label) + 1;
    int idx = 0;
    MField *field = info->slots[idx];
    strlwr(label);
    while(field){
        if(memcmp(field->low_label, label, k_len) == 0){ 
            return field;
        }   
        field = field->next;
    }   
    return NULL;
}
*/


