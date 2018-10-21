#include "ac_head.h"
#include "ac_utils.h"
#include <vector>
#include "kwfilter.h"
#include "stand.h"
#include "slog.h"
#include "config.h"

/// version
#ifdef __VERSION_ID__
static const char *version_id = "\33[40;32m" __VERSION_ID__ "\33[0m";
#else
static const char *version_id = "\33[40;31munknown\33[0m";
#endif

/// data of building
#if defined(__DATE__) && defined(__TIME__)
static const char server_built[] = "\33[40;32m" __DATE__ " " __TIME__ "\33[0m";
#else
static const char server_built[] = "\33[40;31munknown\33[0m";
#endif

using namespace kwfilter_namespace;
using namespace std;

admin_t* admin = NULL;
vector<uint64_t> g_whites[2];
int g_cindex = 0;
uint32_t g_mtime = 0;



#include "ac_so.h"
extern ACServer *g_server;

void load_whitelist(const char* conf){
	if (conf == NULL){
		SLOG_WARNING("whitelist conf is null");
		return;
	}

	struct stat st;
	if(stat(conf, &st)){
		SLOG_WARNING("whitelist file not exist:%s", conf);
		return;
	}	

	if (st.st_mtime == g_mtime){
		return;
	}
	g_mtime = st.st_mtime;

	int index = 1 - g_cindex;
	g_whites[index].clear();

	char line[32];
	uint64_t uid;
	FILE* file = fopen(conf, "r");
	if (file == NULL){
		slog_write(LL_WARNING, "fail to open whitelist file");
		return;
	}	

	while (fgets(line, 32, file)){
		uid = strtoll(line, NULL, 10);
		g_whites[index].push_back(uid);
	}

	fclose(file);
	sort(g_whites[index].begin(), g_whites[index].end());
	
	g_cindex = index;

	slog_write(LL_NOTICE, "successfully open whitelist file");
}

static inline int in_whitelist(uint64_t uid){
	return binary_search(g_whites[g_cindex].begin(), g_whites[g_cindex].end(), uid);
}

static int match_at(char* src, char** shoot, int* slen){
	char *end = src + strlen(src);
	char *start = strstr(src, "@");
	if (start){
		char *find = start + 1;
		while (find < end){
			if ((find[0] & 0xF8) == 0xF0){
				find += 4;					
			}else if ((find[0] & 0xF0) == 0xE0){
				find += 3;
			}else if ((find[0] & 0xE0) == 0xC0){
				find += 2;
			}else if ((find[0] >= 97 && find[0] <= 122) || (find[0] >= 65 && find[0] <= 90) ||
					(find[0] >= 48 && find[0] <= 57) || find[0] == 45 || find[0] == 47 || find[0] == 95){
				find += 1;
			}else{
				break;
			}
		}
	
		*shoot = start;
		*slen = find >= end ? (end - start) : (find - start);	

		return 0;
	}else{
		return -1;
	}
}

int match_link(char* src, char** shoot, int* slen){
	char *find, *start;
	find = strstr(src, "<sina:link");
	
	if (find){
		start = strstr(find, ">");
		if (start){
			*shoot = find;
			*slen = start - find + 1;

			return 0;
		}else{
			return -1;
		}	
	}else{
		return -1;
	}	
}

int match_red(char* src, char** shoot, int* slen, char** rep, int* rlen){
	char *find, *start;
	char* match1 = "<span style=\"color:#C03\">";
	char* match2 = "</span>";
	int len1 = strlen(match1);
	int len2 = strlen(match2);
	start = strstr(src, match1);

	if (start){
		find = strstr(start, match2);
		if (find){
			*shoot = start;
			*slen = find + len2 - start;

			*rep = start + len1;
			*rlen = find - start - len1;

			return 0;
		}else{
			return -1;
		}
	}else{
		return -1;
	}
}

int match_punc(char* src, char** shoot, int* slen, char** rep, int* rlen){
	char* punc[] = {"，",
					"。",
					"！",
					"：",
					"？",
					"【",
					"】"
					};	
	int count = sizeof(punc) / sizeof(char*);
	int i;
	uint32_t len = strlen(src);
	char* end = src + len;

	while (src < end){
		for (i = 0; i < count; i ++){
			if (len < strlen(punc[i])){
				continue;
			}
			if (strncmp(src, punc[i], strlen(punc[i])) == 0){
				*shoot = src;
				*slen = strlen(punc[i]);
				*rep = " ";
				*rlen = strlen(*rep);

				return 0;
			}
		}
		src ++;
		len --;
	}

	return -1;
}

void filter_del(char* src, int(*call)(char* src, char** shoot, int* slen)){
	char* shoot;
	int len;
	int llen;  

	while (call(src, &shoot, &len) == 0){
		llen = strlen(shoot+len);
		memset(shoot, ' ', len);
		src = shoot;
	}
}

void filter_rep(char* src, int(*call)(char* src, char** shoot, int* slen, char** rep, int* rlen)){
	char *shoot, *rep;
	int slen, rlen;

	while (call(src, &shoot, &slen, &rep, &rlen) == 0){
        if (slen <= rlen) {
            SLOG_FATAL("slen can not less than rlen");
            break;
        }

		memmove(shoot, rep, rlen);

        if (slen - rlen < 0) {
            SLOG_WARNING("slen - rlen can not less than 0 %d ", slen -  rlen);
            break;
        }
        memset(shoot + rlen, ' ', slen - rlen);
		src = shoot + slen;		
	}
}

void trim(char* src){
	int begin, end;
	int len = strlen(src);
	for (begin = 0; begin < len; begin ++){
		if (src[begin] != ' '){
			break;
		}
	}

	if (begin == len){
		src[0] = '\0';
		return;
	}

	for (end = len - 1; end > begin; end --){
		if (src[end] != ' '){
			break;
		}
	}
	int newlen = end - begin + 1;
	memmove(src, src+begin, newlen);
	src[newlen] = '\0';
}

void filter_out(char* src){
	filter_del(src, match_at);
	filter_del(src, match_link);
	filter_rep(src, match_red);
	filter_rep(src, match_punc);
	//trim(src);
}

void filter_key(char* src){
	filter_rep(src, match_punc);
}

int ac_filter_keyword(const char* src){
	char buff[1024];
	char filter[strlen(src)];
	strcpy(filter, src);
	filter_key(filter);

	Search_t search(admin->get_root());
	int ret = search.find_dict_without_user_info(filter,time(0));
    //int ret = search.find(filter,time(0));
	if (strcmp(filter, "淡美的薰衣草") == 0) {
        ret = 2;
    }
    seLogEx(g_server->query_log, "Counter:1");

	if (ret <= 0){
		return 0;
	}else if (ret & 0x7){
		search.GetMatchedRulesContent(buff, 1024);
        seLogEx(g_server->query_log, "Filter: %s because of %s", src, buff);
		return 2;
	}else{
		search.GetMatchedRulesContent(buff, 1024);
        seLogEx(g_server->query_log, "Filter: %s because of %s", src, buff);
		return 1;
	}
}
/*
int ac_filter_output(const char* src, uint64_t uid, int status){
	char buff[1024];
	char filter[strlen(src)];
	strcpy(filter, src);
	filter_out(filter);
	if (strlen(filter) == 0){
		return 0;
	}

	Search_t search(admin->get_root());

	if (in_whitelist(uid)){
		//int ret = search.find_dict_without_user_info(filter,time(0));
		int ret = search.find(filter,time(0));
		if (ret == 1){
			search.GetMatchedRulesContent(buff, 1024);
            seLogEx(g_server->content_log, "Filter: fake_url because of %s", buff);
			return 1;
		}else{
			return 0;
		}
	}else{

		//int ret = search.find_dict_without_user_info(filter,time(0));
		int ret = search.find(filter,time(0));
		if (ret <= 0 || ret == 4 || ((ret & 0x11) == 0 && status == 1)){
			return 0;
		}else{
			search.GetMatchedRulesContent(buff, 1024);
            seLogEx(g_server->content_log, "Filter: fake_url because of %s", buff);
			return 1;
		}
	}
}
*/
static void* timer(oop_source_t *op, struct timeval tv, void *args){
	char* p = conf_get_str(g_server->conf_current, "white_list", "/data1/apache2/config/kw_whiteuser.txt");
	load_whitelist(p);

	gettimeofday(&tv, NULL);
	tv.tv_sec += 5;
	oop_add_time(g_server->oop, tv, timer, NULL);	

	return OOP_CONTINUE;
}
 
/*static void __attribute__ ((constructor)) */
void ac_filter_init(void){
	// 初始化admin
	char* p = conf_get_str(g_server->conf_current, "filter_file", "/data1/apache2/config/searchswitch.binnew.data.v10.new");
	admin = new (std::nothrow) admin_t(p, "./log/kwfilter.log");
	if (admin == NULL){
		slog_write(LL_WARNING, "new admin_t wrong");
		suicide();
	}

	// 初始化白名单
	struct timeval tv;
	gettimeofday(&tv, NULL);
	oop_add_time(g_server->oop, tv, timer, NULL);
	
	slog_write(LL_NOTICE, "ac_filter so inited.");
	return;
}

static void __attribute__ ((destructor)) ac_filter_fini(void){
	if (admin){
		delete admin;
	}
	slog_write(LL_NOTICE, "ac_filter so destroyed");
	return;
}


int filter_print_version(void) {
	printf(" Version\t: %s\n", version_id);
	printf(" Built date\t: %s\n", server_built);
	printf("\n");
    return 0;
}




