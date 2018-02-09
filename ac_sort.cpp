/**
 * @file ac_sort.c
 * @author 
 * @version 1.0
 * @brief 
 **/
#include <vector.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <utility>
#include <ext/hash_map>
#include <ext/hash_set>
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
#include "ac_hot_ext_data_parser.h"
#include "ac_hot_sort_model.h"
#include "intention_def.h"
#include "ac_sandbox.h"
#include <dacore/latency_controller.h>
#include "ac/src/utils/md5.h"
#include "ac_global_conf.h"
#include "jsoncpp/jsoncpp.h"
#include "ac_expr.h"
#include <sstream>

using namespace std;

//#include "google/profiler.h"

/// data of building
#if defined(__DATE__) && defined(__TIME__)
static const char server_built[] = "\33[40;32m" __DATE__ " " __TIME__ "\33[0m";
#else
static const char server_built[] = "\33[40;31munknown\33[0m";
#endif

#define MAX_DUP_SIZE	(MAX_DOCID_NUM * 3)
#define DOC_VALUE_ZERO (1)
#define TMP_LOG_LEN 1024
#define PRINT_CONTINUE_LOG_LELVE 3
#define PRINT_SOCIAL_LOG_LELVE 2
#define PRINT_SOCIAL_HEAD_INFO_LOG_LELVE 2
#define PRINT_BLACK_UID_LOG_LELVE 3
#define PRINT_RERANK_LOG_LELVE 3
#define SHOW_DEBUG_NEWLINE 2
#define PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE 2
#define PRINT_CLUSTER_LOG_LELVE 2
#define PRINT_CLUSTER_LOG_LELVE3 2
#define PRINT_CLUSTER_LOG_LELVE_2 2
#define REPOST_MID_REVISE 3000000000000000
#define PRINT_RERANK_UPDATE_LOG 2
#define PRINT_RERANK_UPDATE_ROOT_MID_DEL_LOG 1
#define IOS_CLUSTER_LOG 2
#define PRIU64 "%lu"
#define PRI64 "%ld"
#define CLUSTER_SOCIAL_VIP_NUM 3
#define PRINT_CLUSTER_DUP_LOG_LELVE 2
#define PRINT_RERANK_UPDATE_LOG_2 2
#define MAX_UID_SOURCE_COUNT 5
#define PRINT_FAKE_DOC 2
#define MAX_INTENTION_STRATEGY_NUM 3

typedef map<int,  vec_expr_t, greater<int> >::iterator expr_it;

enum {
	RERANK_UPDATE_DEGRADE_STATE_NONE = 0,
	RERANK_UPDATE_DEGRADE_STATE_1 = 1,
	RERANK_UPDATE_DEGRADE_STATE_2 = 2,
	RERANK_UPDATE_DEGRADE_STATE_DEGRADE = 3,
};

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
    // del
	DEGREE_0 = 0,
	DEGREE_1_FORWARD_TIME = 1,
	DEGREE_2_RANK_ERROR = 2,
    // recall
	DEGREE_3_DUP_DUP = 3,
	DEGREE_4_DUP = 4,
	DEGREE_5_LOW = 5,
	DEGREE_6_RECALL_LOW_DUP = 6,
	DEGREE_7_NOACT_DUP = 7,
	DEGREE_8_RECALL = 8,
    // good
	DEGREE_9_GOOD = 9,
	DEGREE_10_RECALL_GOOD_DUP = 10,
	DEGREE_11_CATAGORY_FILTER = 11,
	DEGREE_12_VALID_FWNUM = 12,
	DEGREE_13_VALID_INTEN_PLOY = 13,
	DEGREE_14_SOCIAL_AT = 14,
	DEGREE_15_GOOD_PASS_FILTER = 15,
	DEGREE_100_VIP = 100,
	DEGREE_200_SOCIAL = 200,
};

typedef struct {
	void *bitmap;
	int uid_black_type;
	int source_black_type;
	int source_white_type;
} AcSort;

typedef struct {
    uint64_t rank;
    uint64_t cont_sign;
    uint64_t docid;
    uint64_t uid;
} AcClusterRepost;

typedef struct {
    uint64_t uniq;
    uint64_t newest_time;
    // mid <> doc*
    // for bigger mid , newest time
    std::vector<SrmDocInfo*> all_users;
    SrmDocInfo* vip_doc[CLUSTER_SOCIAL_VIP_NUM];
    SrmDocInfo* social_doc[CLUSTER_SOCIAL_VIP_NUM];
    // SrmDocInfo* valid_user_doc[CLUSTER_SOCIAL_VIP_NUM];
    SrmDocInfo* fwnum_doc;
    SrmDocInfo* fwnum_newest_doc;
    // for have root
    SrmDocInfo* root_doc;
    // for root is 0
    SrmDocInfo* first_ok_doc;
    uint64_t fwnum_num;
    uint64_t fwnum_num_skip_updatetime_mid;
    uint64_t fwnum_newest_rank;
    int count;
    int valid_count;
    int all_doc;
    int keep_root_doc;
    bool root_cluster;
} AcDupRepostClusterDataUpdate;

typedef struct {
    uint32_t keep_top_n;
    int max_act_rerank_doc;
    int cluster_top_buff;
    int degrade_state;
    bool degrade;
} AcRerankData;
typedef struct {
    uint64_t dup;
    uint32_t act;
} ClusterDocid;

typedef struct {
	int degree_0;//垃圾
	int degree_11;
	int degree_12;
	int degree_13;
	int degree_14;
	int degree_16;
	int degree_17;
	int total_num;
	int original_num;
	int black_num;
	int recall_qi_low_doc;
	int recall_qi_low_doc_week;
	int recall_qi_low_doc_month;
    int dup_flag;
	int cluster_del_degree1;
	int cluster_del;
	uint32_t curtime;
    uint32_t recall_time;
    uint32_t recall_time_week;
    uint32_t recall_time_month;
    __gnu_cxx::hash_map<uint64_t, SrmDocInfo*> contsign_map;
    __gnu_cxx::hash_map<uint64_t, SrmDocInfo*> simhash_map;
    __gnu_cxx::hash_map<uint64_t, SrmDocInfo*> simhash2_map;
    __gnu_cxx::hash_map<uint64_t, SrmDocInfo*> samedup_map;
    __gnu_cxx::hash_map<uint64_t, SrmDocInfo*> samedup_repost_map;
	__gnu_cxx::hash_map<uint64_t, pair<SrmDocInfo*,int> > uid_map;
	__gnu_cxx::hash_map<uint64_t, int> mid_map;
    __gnu_cxx::hash_map<uint64_t, AcDupRepostClusterDataUpdate> cluster_data_update;
    __gnu_cxx::hash_set<uint64_t> already_del_mid;
    __gnu_cxx::hash_set<uint64_t> already_fake_doc;
    std::map<uint64_t, ClusterDocid> cluster_for_picsource_check;
    int min_repost_num;
    int repost_root_mid_del_count;
    int cluster_root_valid_num;
    int cluster_repost_valid_num;
    int is_dup_replace_del;
    int act_index;
    int degree_small;
    int doc_null;
    int too_few_limit_doc_num;
    int top_time_num;
    int now_del_doc;
    int no_act_doc_num;
    int act_keep_doc;
    int act_del_time;
    int recall_doc_num;
    int uid_social_num_buf;
    int left_time_bad;
    int left_bad;
    int left_dup;
    int left_cluser_time_error;
    int left_recall;
    int update_repost;
    int del_uid_dup_num;
    int uid_dup_actindex;
    int repost_cluster_del_num;
    int min_topN_num_for_recall;
    int topN_top;
    int need_recall_dup_num;
    int recall_dup_num;
    int bc_input_num;
    int min_num_skip_dup;
    int fwnum_update_cluster_time;
    uint64_t cluster_fwnum;
    uint64_t top_n_time;
    uint64_t top_n_left_better_time;
    uint64_t top_cluster_time;
    uint64_t cluster_validfans_num;
    int uid_source_count;
    int uid_source_time_pass;
    int ac_use_source_dict;
    int hot_cheat_check_qi_bits;
    DASearchDict* uid_source_set[MAX_UID_SOURCE_COUNT];
    DASearchDictSet* uid_source_suspect_dict;
    DASearchDictSet* uid_source_mul_dict;
    ACLongSet* uid_source_black_source;
    ACLongSet* uid_source_white_source;
    DASearchDictSet* pic_source;
    DASearchDictSet* zone_data;
    int inten_ploy_fwnum[MAX_INTENTION_STRATEGY_NUM];
    int inten_ploy_likenum[MAX_INTENTION_STRATEGY_NUM];
    int inten_ploy_docnum_min;
    int inten_ploy_fwnum_filter;
    int inten_ploy_likenum_filter;
    int need_inten_ploy_filter;
    int need_category_filter;

	int super_grade;
	int good_grade;
	int del_grade;
	int low_grade;
	int recall_good_grade;//优质召回档次
	int qualified_num;
	int original_pic_num;
	int orig_video_num;
	int all_pic_num;
	int all_video_num;
	int del_pic_num;
	int del_other_num;
	int del_video_num;
	int min_act_pic_cheat_num;
	int is_check_open;
	int first_page_num;
	int cluster_first_page_num_after_cluster;
	int cluster_first_page_num_actcluster;
	int cluster_first_page_num_degree;
	int pic_cheat_delay_time;
	int pic_cheat_act_weibo_count;
	int delay_time;
	int check_digitattr;
	int new_hot_pic_source_pic_as_video;
	int pass_by_validfans;
	int check_source_zone_normal_type;
	int check_source_zone_dict_type;
	int check_source_zone_normal_digit_type;
	int check_source_zone_dict_digit_type;
    int check_source_zone_qi_source;
    int check_source_zone_qi_or;
    int check_source_zone_qi_zone;
    int check_pic_source_dict;
    int check_pic_source_normal;
	int check_source_zone_normal_use_acdict;
	int check_source_zone_dict_use_acdict;
	string default_exprs;
	string default_thresholds;
	map<int, vec_expr_t, greater<int> > exprs;
	vector<pair<string, string> > grade_threshold;
	vector<pair<string, string> > conditions;
	__gnu_cxx::hash_map<int, int> grade_threshold_filter;
	__gnu_cxx::hash_map<int, int> degree_num;
    bool expr_init_ok;
    bool check_pic_source;
    bool check_pic_source_dict_query;
    bool is_cluster_repost;
    bool is_socialtime;
	//vector<int> degree_num;

} Ac_statistic;

extern ACServer *g_server;
AcSort g_sort;
dau::LatencyController ac_reranking_degrade_latency_ctrl;
dau::LatencyController ac_reranking_latency_ctrl;
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

static int getGranularity_docattr(const SrmDocInfo* doc);
static int getGranularity_docattr(uint64_t qi);
static bool is_white_list(SrmDocInfo *doc);
static bool is_white_list(uint64_t qi);
static int reset_rank_use_time(QsInfo *info, int ranktype);

static int  check_doc_is_black(QsInfo *info, SrmDocInfo *doc,
            uint64_t uid,
            const AcExtDataParser& attr,
            const auto_reference<ACLongSet>& uid_dict,
            const auto_reference<DASearchDictSet>& black_uids,
            //const auto_reference<DASearchDictSet>& dup_black_uids,
            const auto_reference<DASearchDictSet>& plat_black);
static bool is_bad_source_doc(QsInfo *info, SrmDocInfo *doc,SourceIDDict* source_dict);
static bool is_bad_source_doc(QsInfo *info, SrmDocInfo *doc,
            const AcExtDataParser& attr,
            const auto_reference<SourceIDDict>& source_dict);
static bool is_bad_blue_vip_doc(QsInfo *info, SrmDocInfo *doc);
static bool doc_is_bad_result(QsInfo *info, SrmDocInfo *doc, SourceIDDict* source_dict);
static bool doc_is_bad_result(QsInfo *info, SrmDocInfo *doc,
            const AcExtDataParser& attr,
            const auto_reference<SourceIDDict>& source_dict);
bool ac_reranking_update(QsInfo *info);
bool ac_reranking_time(QsInfo *info);
bool ac_reranking_degrade_update(QsInfo *info);
static int low_quality_can_be_deleted(const int &good_num, const int &total_num);
static void init_stat_nod(QsInfo *info, Ac_statistic &node);
static void release_stat_nod(QsInfo *info, Ac_statistic &node);
static bool is_uid_dup(QsInfo *info, SrmDocInfo *doc, Ac_statistic &statistic_data,
            bool isSocial,int page_num);
//static bool is_valid_forward(SrmDocInfo *doc, const AcExtDataParser& attr, int valid_forward_fwnum);
bool is_valid_forward(SrmDocInfo *doc, const AcExtDataParser& attr, int valid_forward_fwnum);
static int get_valid_forward(SrmDocInfo *doc);
static bool my_time_cmp(const SrmDocInfo * a, const SrmDocInfo *b);
static void make_atresult_weibo(QsInfo *info);
static int adjust_nick_split(QsInfo *info);
static void adjust_nick_partial_result(QsInfo *info);
static int adjust_movie_result(QsInfo *info);
static int hot_cheat_check_qi(QsInfo *info, Ac_statistic &ac_sta, SrmDocInfo *doc, uint64_t qi);
static int hot_cheat_check_del(QsInfo *info, SrmDocInfo *doc, uint64_t qi, uint64_t source,
            uint64_t digit_attr, Ac_statistic &ac_sta);
static int hot_cheat_check_addinfo(QsInfo *info, uint64_t digit_attr, int index,
            Ac_statistic &ac_sta);
static int hot_cheat_check_qi_or(QsInfo *info, Ac_statistic &ac_sta, SrmDocInfo *doc, uint64_t qi);
static int hot_cheat_check_qi_and(QsInfo *info, Ac_statistic &ac_sta, SrmDocInfo *doc, uint64_t qi);
/* end*/

static inline int get_degree(SrmDocInfo *doc)
{
	return (doc->filter_flag >> 20) & 0xFF; 
}

static inline void set_degree(SrmDocInfo *doc, int degree)
{
	doc->filter_flag = 0; 
	doc->filter_flag |= degree  << 20;
}

static uint32_t ac_reset_docvalue(uint32_t value) {
	return value;
}

static uint64_t get_dup_by_mid(uint64_t mid) {
    uint64_t dup = mid - REPOST_MID_REVISE;
    // for dup 52
    dup = dup & 0xFFFFFFFFFFFFF;
    return dup;
}

static void insert_already_del_mid(QsInfo *info, SrmDocInfo *doc, Ac_statistic* statistic_data) {
	AcExtDataParser attr(doc);
    if (GET_FORWARD_FLAG(attr.get_digit_attr())) {
        return;
    }
    uint64_t dup = get_dup_by_mid(doc->docid);
    statistic_data->already_del_mid.insert(dup);
    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
            " [%d insert already del mid dup:"PRIU64", mid:"PRIU64"]", __LINE__, dup, doc->docid);
}

static int hot_cheat_check_qi_type(QsInfo *info, Ac_statistic &ac_sta, SrmDocInfo *doc, uint64_t qi,
            int check_type) {
    bool hit_source = false, hit_zone = false, hit_or = false;
    if (qi & ac_sta.check_source_zone_qi_zone) {
        hit_zone = true;
    }
    if (qi & ac_sta.check_source_zone_qi_source) {
        hit_source = true;
    }
    if (qi & ac_sta.check_source_zone_qi_or) {
        hit_or = true;
    }
    if (check_type == 0) {
        if ((hit_source && hit_zone) || hit_or) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                "[hit_cheat_check_qi:"PRIU64","PRIU64", hot_cheat_check_qi_bits:%d]",
                doc->docid, qi, ac_sta.hot_cheat_check_qi_bits);
            return 1;
        }
    } else if (check_type == 1) {
        if (hit_source || hit_zone || hit_or) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                "[hit_cheat_check_qi:"PRIU64","PRIU64", hot_cheat_check_qi_bits:%d]",
                doc->docid, qi, ac_sta.hot_cheat_check_qi_bits);
            return 1;
        }
    }
    return 0;
}

static int CheckPicSourceType(QsInfo *info, SrmDocInfo *doc, Ac_statistic &ac_sta,
            const AcExtDataParser& attr, uint64_t digit_attr,
            int check_type, int check_digit, int use_acdict) {
    uint64_t qi = attr.get_qi();
    uint64_t source = attr.get_digit_source();
    uint64_t province = attr.get_province();
    // check qi, set in antispam queue
    // check_type == 0: (hit_source and hit_zone) || (hit_short)
    // check_type == 1: hit_source or hit_zone or hit_short
    if (hot_cheat_check_qi_type(info, ac_sta, doc, qi, check_type)) {
        hot_cheat_check_del(info, doc, qi, source, digit_attr, ac_sta);
        AC_WRITE_LOG(info, " [hot_q,DEL_LOG:hot_qi_del,"PRIU64","PRIU64","PRIU64"]",
                    doc->docid, GetDomain(doc), source);
        return 1;
    }
    // flag pass
    if (!ac_sta.ac_use_source_dict || !use_acdict) {
        return 0;
    }
    // check ac's dict
    // check_digit==-1, check every weibo
    if ((check_digit == -1) || (digit_attr & check_digit)) {
        char s_uid[128], s_zone[128];
        int len = snprintf(s_uid, 128, "%lld:%lld", GetDomain(doc), source);
        s_uid[len] = 0;
        int zone_len = snprintf(s_zone, 128, "%lld:%lld", GetDomain(doc), province);
        s_zone[zone_len] = 0;
        bool hit_source = false, hit_zone = false;
        if (ac_sta.pic_source && ac_sta.pic_source->Search(s_uid, len) == -1) {
            hit_source = true;
        }
        if (ac_sta.zone_data && ac_sta.zone_data->Search(s_zone, zone_len) == -1) {
            hit_zone = true;
        }
        /*
        if (qi & ac_sta.check_source_zone_qi_zone) {
            hit_zone = true;
        }
        */
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                                " [hot_q:"PRIU64","PRIU64",h_s:%d,h_z:%d, qi:"PRIU64",s:"PRIU64",z:"PRIU64",t:%d]",
                                doc->docid, GetDomain(doc), hit_source, hit_zone, qi, source, province, check_type);
        if (check_type == 0) {
            if (hit_source && hit_zone) {
                hot_cheat_check_del(info, doc, qi, source, digit_attr, ac_sta);
                AC_WRITE_LOG(info, " [hot_q,DEL_LOG:hot_pic_del,"PRIU64","PRIU64","PRIU64"]",
                            doc->docid, GetDomain(doc), source);
                return 1;
            } else {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [hot_q:"PRIU64","PRIU64","PRIU64",pic_save]",
                            doc->docid, GetDomain(doc), source);
                return 0;
            }
        } else if (check_type == 1) {
            if (hit_source || hit_zone) {
                hot_cheat_check_del(info, doc, qi, source, digit_attr, ac_sta);
                AC_WRITE_LOG(info, " [hot_q,DEL_LOG:hot_pic_del,"PRIU64","PRIU64","PRIU64"]",
                            doc->docid, GetDomain(doc), source);
                return 1;
            } else {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [hot_q:"PRIU64","PRIU64","PRIU64",pic_save]",
                            doc->docid, GetDomain(doc), source);
                return 0;
            }
        }
    }
    return 0;
}

static int CheckPicSource(QsInfo *info, SrmDocInfo *doc, Ac_statistic &ac_sta,
            const AcExtDataParser& attr, uint64_t digit_attr) {
    if (doc->rank < ac_sta.curtime - ac_sta.uid_source_time_pass) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [time < data's time pass CheckPicSource:"PRIU64", "PRIU64", %d]",
            doc->docid, doc->rank, ac_sta.uid_source_time_pass);
        return 0;
    }
    int degree = get_degree(doc);
    if (degree >= DEGREE_15_GOOD_PASS_FILTER) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [good_uid or social pass CheckPicSource:"PRIU64",d:%d]",
            doc->docid, degree);
        return 0;
    }
    if (doc->validfans >= ac_sta.pass_by_validfans) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [validfans:%d >= %d, pass CheckPicSource:"PRIU64"]",
            doc->validfans, ac_sta.pass_by_validfans, doc->docid);
        return 0;
    }
    if (ac_sta.check_pic_source_dict_query) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " ["PRIU64":dict_query]", doc->docid);
        return CheckPicSourceType(info, doc, ac_sta, attr, digit_attr,
                    ac_sta.check_source_zone_dict_type, ac_sta.check_source_zone_dict_digit_type,
                    ac_sta.check_source_zone_dict_use_acdict);
    }
    return CheckPicSourceType(info, doc, ac_sta, attr, digit_attr,
                ac_sta.check_source_zone_normal_type,
                ac_sta.check_source_zone_normal_digit_type,
                ac_sta.check_source_zone_normal_use_acdict);
}

static void DelDocAddDupPrintLog(QsInfo *info, SrmDocInfo *doc,
            const AcExtDataParser& attr, const char* reason, bool insert_del,
            Ac_statistic &statistic_data) {
    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
        " [%s, DEL_LOG:"PRIU64"]", reason, doc->docid);
    if (insert_del && !(attr.get_digit_attr() & (0x1 << 2))) {
        uint64_t dup = get_dup_by_mid(doc->docid);
        statistic_data.already_del_mid.insert(dup);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [%d insert already del mid dup:"PRIU64", mid:"PRIU64"]", __LINE__, dup, doc->docid);
    }
    info->bcinfo.del_doc(doc);
}

static void CheckClusterDocPicSource(QsInfo *info, SrmDocInfo *doc, Ac_statistic &ac_sta,
            const AcExtDataParser& attr, bool &already_check_pic_source,
            bool &check_pic_source_failed) {
    ac_sta.qualified_num++;
    uint64_t digit_attr = attr.get_digit_attr();
    hot_cheat_check_addinfo(info, digit_attr, ac_sta.cluster_data_update.size(), ac_sta);
    already_check_pic_source = true;
    if (CheckPicSource(info, doc, ac_sta, attr, digit_attr)) {
        check_pic_source_failed = true;
        int degree = DEGREE_0;
        set_degree(doc, degree);
        ac_sta.degree_0++;
        DelDocAddDupPrintLog(info, doc, attr, "source_check_failed", true, ac_sta);
    }
}


static void set_ac_log_info(SrmDocInfo *doc,int flag, const char * detail = NULL) {
	if (!doc) {
		return;
	}

	AcExtDataParser attr(doc);
	uint64_t dup_tmp = attr.get_dup();
	uint64_t cont_tmp = attr.get_cont_sign();
	uint64_t qi = attr.get_qi();
	int degree = get_degree(doc);
	bool is_social = (doc->filter_flag & 0x40);
	bool has_valid_fwnum = (doc->filter_flag & 0x800);

	DOC_WRITE_ACDEBUG(doc, " qi:%lu dup:%lu cont_sign:%lu is_social:%d valid_fwnum:%d degree:%d", 
			qi,dup_tmp,cont_tmp,is_social,has_valid_fwnum,degree);

	if (flag) {
		switch(degree)
		{
			case 0:
				DOC_WRITE_ACDEBUG(doc, "rubbish delete");
				break;
			case 1:
				DOC_WRITE_ACDEBUG(doc, "dup delete");
				break;
			case 2:
			case 3:
			case 4:
			case 5:
				DOC_WRITE_ACDEBUG(doc, "low quality delete");
				break;
			case 6:
				DOC_WRITE_ACDEBUG(doc, "bad vip or source delete");
				break;
			case 7:
			case 8:
				DOC_WRITE_ACDEBUG(doc, "common delete");
				break;
			case 9:
			case 10:
				DOC_WRITE_ACDEBUG(doc, "good delete");
				break;
			case 11:
				DOC_WRITE_ACDEBUG(doc, "social or fw delete");
				break;
			default:
				break;
		}
	}

	if (detail) {
		DOC_WRITE_ACDEBUG(doc, "%s", detail);
	}
}

uint64_t ac_sort_uid(SrmDocInfo *doc) {
	return GetDomain(doc);
}

int ac_abs_dup(QsInfo *info){
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

	assert(info->magic == MAGIC_QSINFO);

	if (bcinfo->get_doc_num() == 0 || RankType(info) != RANK_SOCIAL || info->parameters.nofilter != 0) {
		return 0;
	}

	QsInfoSocial *social = &info->socialinfo;
	if (!social->has_social) {
		return 0;
	}
	struct timeval start_time;
	gettimeofday(&start_time, NULL);

	char log[1024]; //
    int log_len = sprintf(log, "{");

	int i;
	int social_count = 0; //
	int friend_count = 0;
	int intv_max_follow = int_from_conf(INTV_MAX_FOLLOW_CONF, INTV_MAX_FOLLOW);
	int social_up_follower = int_from_conf(SOCIAL_UP_FOLLOWER_CONF, SOCIAL_UP_FOLLOWER);
	for (i = 0; i < (int) bcinfo->get_doc_num(); i++) {
		SrmDocInfo *doc = bcinfo->get_doc_at(i);

		if (doc == NULL || doc->getCategory() == DOC_HOT)
			continue;
/*
		if (info->parameters.debug) {
			sprintf(doc->ac_debug + doc->ac_curlen,
					" scofial[value0:%u rank0:%u]", doc->value, doc->rank);
			doc->ac_curlen = strlen(doc->ac_debug);
		}
*/
		UNode *node;
		HASH_SEEK(social->social_uids, GetDomain(doc), node);
		if (node) {
			/*if (isBlueV == 0) {
			 doc->relation = node->relation;
			 } else {
			 doc->relation = node->relation & SOCIAL_FOLLOWING;
			 }*/
			if (info->parameters.debug) {
                AC_WRITE_LOG(info, " [ac_sort_get_social:social:uid:"PRIU64",%d]",
                            GetDomain(doc), node->relation);
            }

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
/*			if (info->parameters.debug) {
				sprintf(doc->ac_debug + doc->ac_curlen,
						" [value1:%u rank1:%u]", doc->value, doc->rank);
				doc->ac_curlen = strlen(doc->ac_debug);
			}*/
		}
	}

	if (social_count) {
		bcinfo->sort();
		log_len += sprintf(log + log_len, "social_count:%d ", social_count);
	}

    log_len += sprintf(log + log_len, "ac_sort_social:time:%d}", calc_time_span(&start_time) / 1000);
	AC_WRITE_LOG(info, "\t%s", log);

	return 0;
}

static void ac_get_extinfo(QsInfo *info, uint64_t debugid) {
	QsInfoBC *bcinfo = &info->bcinfo;
	for (int i = 0; i < (int) bcinfo->get_doc_num(); i++) {
		SrmDocInfo *doc = bcinfo->get_doc_at(i);

		if (doc == NULL || doc->extra == NULL) {
			continue;
		}

		if (bcinfo->bc[doc->bcidx] == NULL) {
			continue;
		}

		if (debugid != 0 && doc->docid == debugid) {
			AC_WRITE_LOG(info, " debugid:%lu in topk[%d:%d]", doc->docid, doc->bcidx, doc->bsidx);
		}

		if (info->parameters.debug) {
			AcExtDataParser attr(doc);

			SLOG_DEBUG("docid:%lu, rank:%u, fwnum:%d, cmtnum:%d, reserved1:%d, uid:"PRIU64", bcidx:%d, "
					"value:%u, qi:%d, dup:%lu, cont_sign:%u, validfw:%u",
					doc->docid, doc->rank, attr.get_fwnum(), attr.get_cmtnum(), doc->reserved1, 
					GetDomain(doc), doc->bcidx, doc->value, attr.get_qi(), attr.get_dup(), attr.get_cont_sign(),
					attr.get_validfwnm());
		}
        
		//旧的扩展数据解析方式
        if (doc->reserved1 == 0)
        {
            int flag = bcinfo->bc[doc->bcidx]->flag;

            int head_len = doc->attr_version > 0 ? sizeof(AttrExtra) : 0;
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
		} else if ((info->params.nofilter & 0x1) || (info->params.nofilter & 0x2)) {
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
			SLOG_DEBUG("docid %u %u of query %s in black doc_query",
					(uint32_t) doc->docid, doc->bsidx, info->params.squery);

			doc->value = ac_reset_docvalue(DOC_VALUE_ZERO);
			bcinfo->del_doc(doc);
			adjust_num++;
			continue;
		}
		uint64_t uid = GetDomain(doc);
		if (uid && BITDUP_EXIST(uid_black, uid,/* MAX_UID_SIZE*/0xffffffff)) {
			uid_black_count++;

			doc->value = ac_reset_docvalue(DOC_VALUE_ZERO);
			bcinfo->del_doc(doc);
			adjust_num++;
			continue;
		}
        AcExtDataParser attr(doc);
		int source = attr.get_digit_source();
		if (source && BITDUP_EXIST(source_black, source, MAX_SOURCE_SIZE)) {
			source_black_count++;

			doc->value = ac_reset_docvalue(DOC_VALUE_ZERO);
			bcinfo->del_doc(doc);
			adjust_num++;
			continue;
		}
	}

	bcinfo->refresh_weak();
	SLOG_DEBUG("doc_total:%d, the num of adjust results:%d", doc_total,
			adjust_num);
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
				doc->value = doci->value + 1;

				optimes++;

				SLOG_DEBUG(
						"INNER_UIDS:morei [%d %d %d] [%d %d %d %d] ["PRIU64" %u %u %u]",
						qtype, utype, timel, i, lasti, lessi, morei,
						GetDomain(doc), old_value, doci->value, doc->value);

				lasti = morei;
			}
		} else if (utype != -1) {
			SrmDocInfo *doci = info->bcinfo.get_doc_at(lessi);

			if (doci != NULL) {
				doc->value = doci->value + 1;

				//optimes++;

				SLOG_DEBUG(
						"INNER_UIDS:lessi [%d %d %d] [%d %d %d %d] ["PRIU64" %u %u %u]",
						qtype, utype, timel, i, lasti, lessi, morei,
						GetDomain(doc), old_value, doci->value, doc->value);

				//lasti = lessi;
			}
		}
	}

	info->bcinfo.sort(0, i);

	return 0;
}

int update_validfans_doc(QsInfo *info, SrmDocInfo *doc, uint64_t uid,
            const auto_reference<DASearchDict>& uids_validfans_week,
            const auto_reference<DASearchDict>& uids_validfans_hour) {
    if (uid && *uids_validfans_week) {
        int validfans = uids_validfans_week->Search(uid);
        if (validfans != -1) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [validfans %d in uids_validfans_week,docid:"PRIU64"]",validfans, doc->docid);
            doc->validfans = validfans;
        } 
        else {
            if (uid && *uids_validfans_hour) {
                validfans = uids_validfans_hour->Search(uid);
                if (validfans != -1) { 
                    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                            " [validfans %d in uids_validfans_hour,docid:"PRIU64"]", validfans, doc->docid);
                    doc->validfans = validfans;
                }
            }
        }
    }
    return 0;
}

int update_validfans(QsInfo *info)
{
	auto_reference<DASearchDict> uids_validfans_week("uids_validfans_week");
	auto_reference<DASearchDict> uids_validfans_hour("uids_validfans_hour");
	if (!(*uids_validfans_week) && !(*uids_validfans_hour)) {
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
        update_validfans_doc(info, doc, uid, uids_validfans_week, uids_validfans_hour);
	}
	return 0;
}

int sort_print_version(void) {

	return 0;
}
/** add by lyk*/ 

int get_doc_detail(SrmDocInfo *doc, char * debug_info,size_t buff_size)
{
	if (doc == NULL || debug_info == NULL) {
		return -1;
	}
	
	AcExtDataParser attr(doc);
	bool is_social = (doc->filter_flag & 0x40);
	bool has_valid_fwnum = (doc->filter_flag & 0x800);
	int degree = get_degree(doc);

	int info_len = snprintf(debug_info, buff_size, "[from:%s attrver:%d uid:"PRIU64" qi:%lu dup:%lu"
			" cont_sign:%lu fwd:%lu like:%lu cmt:%lu is_social:%d valid_fwnum:%d degree:%d",
				get_bcgroup_name(doc->bcidx), doc->attr_version, GetDomain(doc), attr.get_qi(), 
				attr.get_dup(), attr.get_cont_sign(), attr.get_fwnum(), attr.get_likenum(), 
				attr.get_cmtnum(), is_social, has_valid_fwnum, degree);

	if (buff_size > info_len) {
		switch (degree)
		{
			case 0:
				info_len += snprintf(debug_info+info_len, buff_size - info_len, " rubbish or black list]");
				break;
			case 1:
				info_len += snprintf(debug_info+info_len, buff_size - info_len, " dup doc]");
				break;
			case 2:
				info_len += snprintf(debug_info+info_len, buff_size - info_len, " special qi low quality and not white list]");
				break;
			case 3:
				info_len += snprintf(debug_info+info_len, buff_size - info_len, " special qi low quality and white list]");
				break;
			case 4:
				info_len += snprintf(debug_info+info_len, buff_size - info_len, " qi16 low quality and not white list]");
				break;
			case 5:
				info_len += snprintf(debug_info+info_len, buff_size - info_len, " qi16 low quality and white list]");
				break;
			case 6:
				info_len += snprintf(debug_info+info_len, buff_size - info_len, " bad source or bad VIP]");
				break;
			case 7:
				info_len += snprintf(debug_info+info_len, buff_size - info_len, " common and not white list]");
				break;
			case 8:
				info_len += snprintf(debug_info+info_len, buff_size - info_len, " commom and white list]");
				break;
			case 9:
				info_len += snprintf(debug_info+info_len, buff_size - info_len, " good and not white list]");
				break;
			case 10:
				info_len += snprintf(debug_info+info_len, buff_size - info_len, " good and white list]");
				break;
			case 11:
				info_len += snprintf(debug_info+info_len, buff_size - info_len, " has social relation or has vilid forwardi]");
				break;
			default:
				break;
		}
	}

	if (info_len >= buff_size) {
		info_len = buff_size - 1;
	}

	debug_info[info_len] = '\0';
	return info_len;
}

static int FilterDocattr(const SrmDocInfo* doc) {
	if(doc->docattr & (0x169LL << 53))
		return 1;
    return 0;
}
static int getGranularity_docattr(const SrmDocInfo* doc)
{
	if(doc == NULL)
		return 0;
	
    AcExtDataParser attr(doc);
    // old 58 61 63 if(doc->docattr & (0x29ll << 58))
    // F_PARAM filter
    // 53 56 58 59 61
    // 53 CalcBlackLikenum
    // 56 machine uid
    // 58 black uid
    // 59 qi
    // 61 Calcfwnum
    // 0x169LL << 53
	// if(doc->docattr & (0x169LL << 53))
    //    return 0;
    if (FilterDocattr(doc)) {
		return 0;
    }
    // uint64_t uid = GetDomain(doc);
    // F_PARAM strategy
    if (!(GET_FORWARD_FLAG(attr.get_digit_attr())) &&
        (doc->docattr & (0x1ll << 52))) {
        return 0;
    }
    // F_PARAM strategy
    // old 54 55 56 57 59 60 if(doc->docattr & (0x6Fll << 54))
    // 54 55 57 60 63
    // 54 source==53
    // 55 !authority || authority==2
    // 57 qi_low
    // 60 FNOTTOPWORDS
    // 63 hitnum >= NEW_HIT_CHEATMINNUM
    // 0x125LL << 55
    if(doc->docattr & (0x125LL << 55))
		return 1;
	return 2;
}

static inline int getSocialRelation(QsInfo *info,
            uint64_t uid, uint64_t docattr,
            QsInfoSocial *social) {
	if (social == NULL) {
		return 0;
	}
	UNode *node = NULL;
	HASH_SEEK(social->social_uids, uid, node);
	if (node != NULL && (node->relation & SOCIAL_FOLLOWING)) {
		return 1;
	} else if ((docattr & (0x1ll<<32))) {
		return 2;
	}
	return 0;
}

//static inline int getSocialRelation(QsInfo *info, SrmDocInfo *doc)
int getSocialRelation(QsInfo *info, SrmDocInfo *doc)
{
	if (info == NULL || doc == NULL) {
		return 0;
	}
	
	QsInfoSocial *social = &info->socialinfo;
	if (social == NULL) {
		return 0;
	}
	
	UNode *node = NULL;
	HASH_SEEK(social->social_uids, GetDomain(doc), node);
	
	if (node != NULL && (node->relation & SOCIAL_FOLLOWING)) {
		doc->filter_flag |= 0x40;
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
            " [social,"PRI64",uid:"PRIU64"]", doc->docid, GetDomain(doc));
		return 1;
	} else if ((doc->docattr & (0x1ll<<32))) {
		doc->filter_flag |= 0x40;
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
            " [social,"PRI64",uid:"PRIU64",docattr:"PRIU64"]", doc->docid, GetDomain(doc), doc->docattr);
		return 2;
	}
	
	return 0;
}

static bool is_uid_dup(QsInfo *info, SrmDocInfo *doc, Ac_statistic &statistic_data,bool isSocial,int page_num)
{
	if(!doc)
		return false;
	uint64_t uid;
	uid = GetDomain(doc);
	if(page_num%20 == 0)
		statistic_data.uid_map.clear();
	int degree = get_degree(doc);
	if(uid)
	{
		//if(degree >= DEGREE_15_GOOD_PASS_FILTER)
		if(degree >= statistic_data.super_grade)
		{
            		if (degree != DEGREE_100_VIP) 
			{
                		AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        		" [is_uid_dup pass degree:"PRI64", %d]", doc->docid, degree);
                		if ( statistic_data.uid_map.find(uid) != statistic_data.uid_map.end())
                    			statistic_data.uid_map[uid].second++;
                		else
                    			statistic_data.uid_map[uid] = make_pair(doc,1);
                		return false;
            		} else 
			{
                		AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        		" [is_uid_dup VIP no_pass:"PRI64", %d]", doc->docid, degree);
            		}
		}
		if(get_valid_forward(doc) > 50)
		{
            		if (degree != DEGREE_100_VIP) {
                		AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        	" [is_uid_dup pass valid_forward:"PRI64",%d,%d]",
                        	doc->docid, degree, get_valid_forward(doc));

                		if ( statistic_data.uid_map.find(uid) != statistic_data.uid_map.end())
                    			statistic_data.uid_map[uid].second++;
                		else
                    			statistic_data.uid_map[uid] = make_pair(doc,1);
                		return false;
            		} else 
			{
                		AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        		" [is_uid_dup VIP no_pass:"PRI64",%d,fw:%d]",
                        				doc->docid, degree, get_valid_forward(doc));
            		}
		}
		if (statistic_data.uid_map.find(uid) != statistic_data.uid_map.end())
		{
			//if(degree < DEGREE_11_CATAGORY_FILTER)
			if(degree <= statistic_data.recall_good_grade)
				return true;
			if(statistic_data.uid_map[uid].second < 2)
			{
                		AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        		" [is_uid_dup pass uids<2:"PRI64",%d,%d,%d]",
                        				doc->docid, degree, get_valid_forward(doc), statistic_data.uid_map[uid].second);
				statistic_data.uid_map[uid].second++;
				return false;
			}
			return true;
		}
		else
		{
			statistic_data.uid_map[uid] = make_pair(doc,1);
		}
	}
	return false;
}

static bool is_white_list(uint64_t qi) {
    if(qi & 0x80ll)
        return false;
    return true;
}

static bool is_white_list(SrmDocInfo *doc)
{
	uint64_t qi;
	if(doc == NULL)
		return false;
		
	AcExtDataParser attr(doc);
	
	qi = attr.get_qi();
	if(qi & 0x80ll)
		return false;
	doc->filter_flag |= 0x20;	
	return true;
}

int ac_sort_filter(QsInfo *info) 
{
	int ret = 0;
	struct timeval begin;
	gettimeofday(&begin, NULL);

	int resort_degrade = int_from_conf(RESORT_DEGRADE, 0);
 
	if (ac_reranking_degrade_latency_ctrl.control()) {
		resort_degrade = 1;
	}
	if (int_from_conf(STOP_RESORT_DEGRADE, 1)) {
        resort_degrade = 0;
    }

	AC_WRITE_LOG(info, "\t{ac_sort_filter%s: ", resort_degrade?"_derade":"");
	/*
    {
        int profiler_count = int_from_conf(DUMP_PROFILTER_COUNT, 0);
        if (profiler_count) {
            static uint64_t work_count = 0;
            if (work_count % profiler_count == 0) {
                if (work_count != 0) {
                    ProfilerStop();
                }
                char p_name[128];
                memset(p_name, 0, 128);
                sprintf(p_name, "/tmp/test_pprof_%d.out", (work_count / profiler_count));
                ProfilerStart(p_name);
                slog_write(LL_NOTICE, "ProfilerStart:%s", p_name);
            }
            ++work_count;
        }
    }
    */
	QsInfoBC *bcinfo = NULL;
	if (NULL == info) {
		ret = -1;
		goto Done;
	}

	assert(info->magic == MAGIC_QSINFO);
	
	bcinfo = &info->bcinfo;

	if (NULL == bcinfo) {
		ret = -2;
		goto Done;
	}
	
    // F_PARAM nofilter
    // if (bcinfo->get_doc_num() <= 0 || info->parameters.nofilter != 0) {
	if (bcinfo->get_doc_num() <= 0 || (info->parameters.nofilter != 0)) {
		ret = 0;
		goto Done;
	}
    // xsort=time nofilter=0 us=1
    // xsort=social nofilter=0 us=1 
    // doc_num>0 && getXsort==social && nofilter==0
	ac_get_extinfo(info, info->parameters.debugid);

	ac_inner_uids(info);

	// update_validfans(info);	

    if (info->parameters.getXsort() == std::string("social")) {
        if (resort_degrade) {
            ac_reranking_degrade_update(info);	
        } else {
            ac_reranking_update(info);	
        }
        make_atresult_weibo(info);	
	} else if (info->parameters.getXsort() == std::string("time")) {
        // AcBcCaller::bc_after rerank by doc.rank
        // filter and dup
        // xsort=time nofilter=0 us=1
        ac_reranking_time(info);	
    } else {
        AC_WRITE_LOG(info, " [bad params]");
    }

Done:

	if (info->parameters.debugid > 0) {
		for (int i = 0; i < (int) bcinfo->get_doc_num(); ++i) {
			SrmDocInfo *doc = bcinfo->get_doc_at(i);
			if (doc == NULL) {
				continue;
			}
			
			if (doc->docid == info->parameters.debugid) {
				char buf[1024];
				buf[0] = '\0';
				get_doc_detail(doc, buf, sizeof(buf));
				AC_WRITE_LOG(info, " debugid:%lu in top-%d doc_detail{%s}, social_head:%d, %d:%d",
                            doc->docid, i, buf, doc->social_head_output, doc->bcidx, doc->bsidx);
				break;
			}
		}
	}
	
	int reranking_cost = calc_time_span(&begin) / 1000;
	AC_WRITE_LOG(info, " ac_sort_filter:time:%d %d}", reranking_cost, ret);
	ac_reranking_latency_ctrl.latency(reranking_cost, false);
	ac_reranking_degrade_latency_ctrl.latency(reranking_cost, false);
    if (conf_get_int(g_server->conf_current, "output_show_more_log", 1) &&
        info->parameters.getUs() == 1 &&
        info->parameters.getXsort() == std::string("social") &&
        info->params.num <= 20 &&
        info->parameters.nofilter == 0) {
        for (int i = 0; i < info->params.num; ++i) {
            SrmDocInfo *doc = bcinfo->get_doc_at(i);
            if (doc == NULL) {
                continue;
            }
            bool social = false, cluster = false;
            if ((doc->social_head_output ||
                doc->famous_concern_info.person_info[0].time ||
                doc->social_head.comment[0].comment_id > 0)) {
                social = true;
            }
            if (doc->cluster_info.time) {
                cluster = true;
            }
            AcExtDataParser attr(doc);
            uint64_t user_type = attr.get_user_type();
            int degree = get_degree(doc);
            AC_WRITE_LOG(info, " [:%lu,S:%d,C:%d,D:%d,%lu-%lu-%lu-%lu]",
                        doc->docid, social, cluster, degree,
                        attr.get_fwnum(), attr.get_cmtnum(), attr.get_likenum(), user_type);
        }
    }
    return ret;
}

static int reset_rank_use_time(QsInfo *info, int ranktype) {
	QsInfoBC *bcinfo = &info->bcinfo;
	int num = bcinfo->get_doc_num();

	for (int i = 0; i < num; i++)
	{
		SrmDocInfo *doc = bcinfo->get_doc_at(i);

		if (doc == NULL)
		{
			continue;
		}
		doc->value = doc->rank;
	}

	return 0;
}

static bool is_bad_source_doc(QsInfo *info, SrmDocInfo *doc,SourceIDDict* source_dict)
{
	int mask_sid = 0;
	uint64_t fwnum;
	uint64_t qi;
	uint64_t source;
	if(doc == NULL || source_dict == NULL)
		return false;
	
	AcExtDataParser attr(doc);
	fwnum = attr.get_fwnum();
	qi = attr.get_qi();
	source = attr.get_digit_source();
	mask_sid = source_dict->find(source);
   //如果在词典中，且转发数小于4，且（是个挖坟结果，或不在白名单）
    if (mask_sid && fwnum < 2
            && (GET_DUP_FLAG_OF_INDEX(qi) || GET_NOT_WHITE_FLAG(qi)))
    {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [bad source,docid:"PRIU64"]", doc->docid);
        return  true;
    }

  return false;
}

static bool is_bad_source_doc(QsInfo *info, SrmDocInfo *doc,
            const AcExtDataParser& attr,
            const auto_reference<SourceIDDict>& source_dict) {
	int mask_sid = 0;
	uint64_t fwnum;
	uint64_t qi;
	uint64_t source;
	if(doc == NULL || *source_dict == NULL)
		return false;
	fwnum = attr.get_fwnum();
	qi = attr.get_qi();
	source = attr.get_digit_source();
	mask_sid = source_dict->find(source);
    if (mask_sid && fwnum < 2
            && (GET_DUP_FLAG_OF_INDEX(qi) || GET_NOT_WHITE_FLAG(qi)))
    {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [bad source,docid:"PRIU64"]", doc->docid);
        return  true;
    }

  return false;
}

static bool is_bad_blue_vip_doc(QsInfo *info, SrmDocInfo *doc)
{	 
	uint64_t fwnum;
	uint64_t vtype;
	if(doc == NULL)
		return false;
    AcExtDataParser attr(doc);
    vtype = attr.get_verified_type();
    fwnum = attr.get_fwnum();     
	/* 团体蓝v 有效粉丝 < 1500 */
	if (attr.get_verified() && vtype == 7 && doc->validfans < 1500)
	{
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [bad vip insuff validfans,docid:"PRIU64"]", doc->docid);
		return true;
	}
    if (attr.get_verified()
        && vtype != 1 && vtype != 0 && vtype != 3
        && fwnum < 2)
    {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [bad vip,docid:"PRIU64"]", doc->docid);
        return true;
    }  
	return false;
}

static bool is_bad_blue_vip_doc(QsInfo *info, SrmDocInfo *doc,
            const AcExtDataParser& attr) {	 
	uint64_t fwnum;
	uint64_t vtype;
	if(doc == NULL)
		return false;
    vtype = attr.get_verified_type();
    fwnum = attr.get_fwnum();     
	/* 团体蓝v 有效粉丝 < 1500 */
	if (attr.get_verified() && vtype == 7 && doc->validfans < 1500)
	{
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [bad vip insuff validfans,docid:"PRIU64"]", doc->docid);
		return true;
	}
    if (attr.get_verified()
        && vtype != 1 && vtype != 0 && vtype != 3
        && fwnum < 2)
    {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [bad vip,docid:"PRIU64"]", doc->docid);
        return true;
    }  
	return false;
}

static bool doc_is_bad_result(QsInfo *info, SrmDocInfo *doc, SourceIDDict* source_dict)
{
	if(doc == NULL || source_dict == NULL)
		return false;
	if(is_bad_blue_vip_doc(info, doc) || is_bad_source_doc(info, doc,source_dict))
	{
		doc->filter_flag |= 0x200;
		return true;
	}
	return false;
}

static bool doc_is_bad_result(QsInfo *info, SrmDocInfo *doc,
            const AcExtDataParser& attr,
            const auto_reference<SourceIDDict>& source_dict) {
	if(doc == NULL || *source_dict == NULL)
		return false;
	if(is_bad_blue_vip_doc(info, doc, attr) || is_bad_source_doc(info, doc, attr, source_dict))
	{
		doc->filter_flag |= 0x200;
		return true;
	}
	return false;
}

static void RecallDict(QsInfo *info, Ac_statistic &ac_sta) {
    for (int i = 0; i < ac_sta.uid_source_count; ++i) {
        if (ac_sta.uid_source_set[i]) {
            ac_sta.uid_source_set[i]->UnReference();
        }
    }
    if (ac_sta.uid_source_black_source) {
        ac_sta.uid_source_black_source->UnReference();
    }
    if (ac_sta.uid_source_white_source) {
        ac_sta.uid_source_white_source->UnReference();
    }
    if (ac_sta.uid_source_suspect_dict) {
        ac_sta.uid_source_suspect_dict->UnReference();
    }
    if (ac_sta.uid_source_mul_dict) {
        ac_sta.uid_source_mul_dict->UnReference();
    }
}

static int hot_cheat_check_addinfo(QsInfo *info, uint64_t digit_attr, int index,
            Ac_statistic &ac_sta) {
    if (digit_attr & (0x1 << 4)) {
        ++ac_sta.all_video_num;
        if (index <= ac_sta.pic_cheat_act_weibo_count) {
            ++ac_sta.orig_video_num;
        }
    }
    if (digit_attr & 0x1) {
        ++ac_sta.all_pic_num;
        if (index <= ac_sta.pic_cheat_act_weibo_count) {
            ++ac_sta.original_pic_num;
        }
    }
}
static int hot_cheat_check_addinfo(QsInfo *info, uint64_t digit_attr, int index,
            int pic_cheat_act_weibo_count, int& all_pic_num, int& original_pic_num,
            int& all_video_num, int& orig_video_num) {
    if (digit_attr & (0x1 << 4)) {
        ++all_video_num;
        if (index <= pic_cheat_act_weibo_count) {
            ++orig_video_num;
        }
    }
    if (digit_attr & 0x1) {
        ++all_pic_num;
        if (index <= pic_cheat_act_weibo_count) {
            ++original_pic_num;
        }
    }
}

static int hot_cheat_check_del(QsInfo *info, SrmDocInfo *doc,
            uint64_t qi, uint64_t source, uint64_t digit_attr,
            Ac_statistic &ac_sta) {
    if (doc->rank > ac_sta.delay_time) {
        --ac_sta.qualified_num;
        uint64_t uid = GetDomain(doc); 
        if (digit_attr & 0x1) {
            ++ac_sta.del_pic_num;
        } else if (digit_attr & (0x1 << 4)) {
            ++ac_sta.del_video_num;
        } else {
            ++ac_sta.del_other_num;
        }
        // AC_WRITE_LOG(info, " [h_d:"PRIU64",qi:"PRIU64","PRIU64","PRIU64",%d]",
          //          doc->docid, qi, digit_attr, uid, source);
        if (ac_sta.is_check_open) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG hot_cheat_check:"PRI64",qi:"PRI64"]", doc->docid, qi);
            info->bcinfo.del_doc(doc);
        }
    }
    return 0;
}

static int hot_cheat_check_del(QsInfo *info, SrmDocInfo *doc,
            uint64_t qi, uint64_t source, uint64_t digit_attr,
            int delay_time, int is_check_open,
            int& qualified_num, int& del_pic_num, int& del_other_num, int& del_video_num) {
    if (doc->rank > delay_time) {
        --qualified_num;
        uint64_t uid = GetDomain(doc); 
        if (digit_attr & 0x1) {
            ++del_pic_num;
        } else if (digit_attr & (0x1 << 4)) {
            ++del_video_num;
        } else {
            ++del_other_num;
        }
        // AC_WRITE_LOG(info, " [h_d:"PRIU64",qi:"PRIU64","PRIU64","PRIU64",%d]",
          //          doc->docid, qi, digit_attr, uid, source);
        if (is_check_open) {
            // AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            //         " [DEL_LOG hot_cheat_check:"PRI64",qi:"PRI64"]", doc->docid, qi);
            info->bcinfo.del_doc(doc);
        }
    }
    return 0;
}

static int hot_cheat_check_qi(QsInfo *info, Ac_statistic &ac_sta, SrmDocInfo *doc, uint64_t qi) {
    if (qi & ac_sta.hot_cheat_check_qi_bits) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            "[hit_cheat_check_qi:"PRIU64","PRIU64", hot_cheat_check_qi_bits:%d]",
            doc->docid, qi, ac_sta.hot_cheat_check_qi_bits);
        return 1;
    }
    return 0;
}

static int hot_cheat_check_pic(QsInfo *info, Ac_statistic &ac_sta, SrmDocInfo *doc,
            uint64_t source, int use_uid_source_black_source) {
    uint64_t uid = GetDomain(doc); 
    // use ac source_dict check
    if (ac_sta.uid_source_white_source->find(source)) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [uid in white_source,"PRIU64","PRIU64","PRIU64"]",
            doc->docid, uid, source);
        return 0;
    } else if (ac_sta.uid_source_suspect_dict &&
               ac_sta.uid_source_suspect_dict->Search(uid) != -1) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [uid in suspect,"PRIU64","PRIU64","PRIU64"]",
            doc->docid, uid, source);
        return 1;
    } else if (ac_sta.uid_source_mul_dict &&
               ac_sta.uid_source_mul_dict->Search(uid) != -1) {
        if (!use_uid_source_black_source || ac_sta.uid_source_black_source->find(source) == false) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [uid in mul source,"PRIU64","PRIU64","PRIU64"]",
                doc->docid, uid, source);
            return 0;
        } else {
            return 1;
        }
    } else {
        // uid, source
        for (int i = 0; i < ac_sta.uid_source_count; ++i) {
            if (ac_sta.uid_source_set[i] == NULL) {
                continue;
            }
            int source_1 = ac_sta.uid_source_set[i]->Search(uid);
            if (source_1 != -1 && source_1 == source) {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [uid in source %d,"PRIU64","PRIU64","PRIU64"]",
                    i, doc->docid, uid, source);
                return 0;
            }
        }
        return 1;
    }
    return 0;
}

static int LoadPicDict(QsInfo *info, Ac_statistic &ac_sta,
            bool& load_uid_source_ok, int& load_uid_source_error) {
    if (ac_sta.ac_use_source_dict) {
        ac_sta.uid_source_black_source = Resource::Reference<ACLongSet>("uid_source_black_source");
        if (ac_sta.uid_source_black_source == NULL) {
            load_uid_source_ok = false;
            load_uid_source_error = 1;
            return 1;
        }
        ac_sta.uid_source_white_source = Resource::Reference<ACLongSet>("uid_source_white_source");
        if (ac_sta.uid_source_white_source == NULL) {
            load_uid_source_ok = false;
            load_uid_source_error = 2;
            return 2;
        }
        ac_sta.uid_source_suspect_dict = Resource::Reference<DASearchDictSet>("uid_source_suspect_dict");
        if (ac_sta.uid_source_suspect_dict == NULL) {
            load_uid_source_ok = false;
            load_uid_source_error = 3;
            return 3;
        }
        ac_sta.uid_source_mul_dict = Resource::Reference<DASearchDictSet>("uid_source_mul_dict");
        if (ac_sta.uid_source_mul_dict == NULL) {
            load_uid_source_ok = false;
            load_uid_source_error = 4;
            return 4;
        }
        for (int i = 0; i < ac_sta.uid_source_count; ++i) {
            char uid_source[64];
            int len = snprintf(uid_source, 64, "uid_source_dict%d", i+1);
            uid_source[len] = 0;
            DASearchDict* uid_source_dict = Resource::Reference<DASearchDict>(uid_source);
            if (uid_source_dict) {
                ac_sta.uid_source_set[i] = uid_source_dict;
            } else {
                load_uid_source_ok = false;
                load_uid_source_error = 5 + i;
                ac_sta.uid_source_set[i] = NULL;
                return 5 + i;
            }
        }
    }
    return 0;
}

// for qi 3, img and source change
// for qi 4, bad surl
static void hot_cheat_check_v2(QsInfo *info, Ac_statistic &ac_sta, int doc_num) {
    if(doc_num < ac_sta.min_act_pic_cheat_num) {
        AC_WRITE_LOG(info, " [doc_num:%d < ac_sta.min_act_pic_cheat_num:%d]",
                    doc_num, ac_sta.min_act_pic_cheat_num);
        return;  
    }
    //stringstream ss;
    ac_sta.qualified_num = 0;
    ac_sta.original_pic_num = 0;
    ac_sta.orig_video_num = 0;
    ac_sta.all_pic_num = 0;
    ac_sta.all_video_num = 0;
    ac_sta.del_pic_num = 0;
    ac_sta.del_other_num = 0;
    ac_sta.del_video_num = 0;
    for(int i = 0; i < doc_num && ac_sta.qualified_num <= ac_sta.first_page_num; ++i) {
        QsInfoBC *bcinfo = &info->bcinfo;
        SrmDocInfo *doc = bcinfo->get_doc_at(i);
        if(!doc) continue;
        ac_sta.qualified_num++;
        AcExtDataParser attr(doc);
        uint64_t digit_attr = attr.get_digit_attr();
        hot_cheat_check_addinfo(info, digit_attr, i, ac_sta);
        CheckPicSource(info, doc, ac_sta, attr, digit_attr);
    } 
    AC_WRITE_LOG(info, " [last:pic:(%d,%d,%d) video:(%d,%d,%d) del:(%d,%d]",
                ac_sta.original_pic_num, ac_sta.all_pic_num,
                (ac_sta.all_pic_num - ac_sta.del_pic_num),
                ac_sta.orig_video_num, ac_sta.all_video_num,
                (ac_sta.all_video_num - ac_sta.del_video_num),
                ac_sta.del_pic_num, ac_sta.del_other_num);
}
// for qi 3, img and source change
// for qi 4, bad surl
static void hot_cheat_check(QsInfo *info, Ac_statistic &ac_sta, int doc_num) {
    if(doc_num < ac_sta.min_act_pic_cheat_num) {
        AC_WRITE_LOG(info, " [doc_num:%d < ac_sta.min_act_pic_cheat_num:%d]",
                    doc_num, ac_sta.min_act_pic_cheat_num);
        return;  
    }
    int is_check_open = int_from_conf(REALTIME_HOTQUERY_CHEAT_CHECK, 1);
    int first_page_num = int_from_conf(FIRST_PAGE_KEEP_NUM, 20);
    int pic_cheat_delay_time = int_from_conf(PIC_CHEAT_DELAY_TIME, 172800);
    int use_uid_source_black_source = int_from_conf(USE_UID_SOURCE_BLACK_SOURCE, 1);
    int pic_cheat_act_weibo_count = int_from_conf(PIC_CHEAT_ACT_WEIBO_COUNT, 20);
    //stringstream ss;
    int qualified_num = 0;
    int original_pic_num = 0, orig_video_num = 0;
    int all_pic_num = 0, all_video_num = 0;
    int del_pic_num = 0, del_other_num = 0, del_video_num = 0;


    time_t time_now = ac_sta.curtime;
    time_t delay_time = time_now - pic_cheat_delay_time;
    bool load_uid_source_ok = true;
    int load_uid_source_error = 0;
    LoadPicDict(info, ac_sta, load_uid_source_ok, load_uid_source_error);
    AC_WRITE_LOG(info, " [load_uid_source:%d,%d]", load_uid_source_ok, load_uid_source_error);
    for(int i = 0; i < doc_num && qualified_num <= first_page_num; ++i) {
        QsInfoBC *bcinfo = &info->bcinfo;
        SrmDocInfo *doc = bcinfo->get_doc_at(i);
        if(!doc) continue;
        qualified_num++;
        AcExtDataParser attr(doc);
        uint64_t digit_attr = attr.get_digit_attr();
        hot_cheat_check_addinfo(info, digit_attr, i, pic_cheat_act_weibo_count, all_pic_num, original_pic_num,
                    all_video_num, orig_video_num);
        if (doc->rank < time_now - ac_sta.uid_source_time_pass) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [time < data's time pass hot_cheat_check:"PRIU64", "PRIU64", %d]",
                doc->docid, doc->rank, ac_sta.uid_source_time_pass);
            continue;
        }
        int degree = get_degree(doc);
        if (degree >= DEGREE_15_GOOD_PASS_FILTER) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [good_uid or social pass hot_cheat_check:"PRIU64",d:%d]",
                doc->docid, degree);
            continue;
        }
        uint64_t qi = attr.get_qi();
        uint64_t source = attr.get_digit_source();
        if (hot_cheat_check_qi(info, ac_sta, doc, qi)) {
            hot_cheat_check_del(info, doc, qi, source, digit_attr, delay_time, is_check_open,
                                qualified_num, del_pic_num, del_other_num, del_video_num);
            continue;
        }
        bool original = !((digit_attr >> 2) & 0x1);
        bool check_pic = false;
        if (((digit_attr & 0x1) && original)) {
            check_pic = true;
        }
        // check pic and surl
        if (!check_pic) {
            continue;
        }
        if (ac_sta.ac_use_source_dict && load_uid_source_ok && hot_cheat_check_pic(info, ac_sta, doc, source, 
                        use_uid_source_black_source)) {
            hot_cheat_check_del(info, doc, qi, source, digit_attr, delay_time, is_check_open,
                                qualified_num, del_pic_num, del_other_num, del_video_num);
            continue;
        }

    } 
    AC_WRITE_LOG(info, " [pic:(%d,%d,%d) del:(%d,%d]",
                original_pic_num, (all_pic_num - del_pic_num), all_pic_num, del_pic_num, del_other_num);
    RecallDict(info, ac_sta);
}

//Only take effect on the hot query
static void drop_duplicated_new_source(QsInfo *info, Ac_statistic &ac_sta, int doc_num)
{
	//get source dict: topn_source_dict
	auto_reference<ACLongSet> topn_source_dict("topn_source_dict.txt");
	if(!(*topn_source_dict)) return;
	AC_WRITE_LOG(info, "[dict_size:%d]", topn_source_dict->count());
	__gnu_cxx::hash_map<int, int> src_map;

	//Pseudo algorithm
	//set delay time: half day: 43200 sec
	//if the sourceid is in topn_source_dict, then do nothing
	//else check sourceid in the src_map
	//	if no: src_map[src]++
	//	else: drop
	//		if time of doc is newer than delay_time, delete
	//		else do nothing
	
	int new_src_delay_time	= int_from_conf(NEW_SRC_DELAY_TIME, 86400);// 24 hours,configurable
	int src_del_threshold	= int_from_conf(SRC_DEL_THRESHOLD,2);
	int new_src_delay_pages = int_from_conf(NEW_SRC_DELAY_PAGES,2);//first 5 pages need filter
	int filter_num 			= new_src_delay_pages * 20;

	//only for test
	//int new_src_delay_time	= 86400;
    //int src_del_threshold	= 2;
	//int new_src_delay_pages = 2;
    //int filter_num 			= 40;
	int src_del_num			= 0;

	time_t time_now 		= ac_sta.curtime;
	time_t delay_time 		= time_now - new_src_delay_time;

	for(int i = 0 ,keep_num = 0; i < doc_num && keep_num < filter_num ; ++i)//handle five pages
	{
		if(i%20 == 0)
		{
			src_map.clear();
		}

		QsInfoBC *bcinfo = &info->bcinfo;
		SrmDocInfo *doc = bcinfo->get_doc_at(i);
		if(!doc) continue;
		AcExtDataParser attr(doc);
		int source = attr.get_digit_source();
        int degree = get_degree(doc);

		if(doc->rank < delay_time || topn_source_dict->find(source) || degree > ac_sta.super_grade )
		{
			//old doc or good source, just skip
			keep_num ++;
			AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
				" [old doc or good src or good_uid or social pass new_src_check:"PRIU64",d:%d]",
				doc->docid, degree);
			continue;
		}
		else
		{
			// new and bad source, drop
			if(++src_map[source] >= src_del_threshold )
			{
				AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
						" [DEL_LOG drop_duplicated_source:"PRI64",source:"PRI64"]", doc->docid, source);
				info->bcinfo.del_doc(doc); 
				src_del_num++;
			}
			else keep_num++;
		}
	}
	AC_WRITE_LOG(info, "[src_del_num:%d]", src_del_num);
}

static bool is_dup_update_doc_act(QsInfo *info, SrmDocInfo *doc,
            const AcExtDataParser& attr,
            Ac_statistic &statistic_data,
            __gnu_cxx::hash_map<uint64_t, SrmDocInfo*>* contsign_map,
            __gnu_cxx::hash_map<uint64_t, SrmDocInfo*>* simhash_map,
            __gnu_cxx::hash_map<uint64_t, SrmDocInfo*>* simhash2_map,
            __gnu_cxx::hash_map<uint64_t, SrmDocInfo*>* samedup_map,
            __gnu_cxx::hash_map<uint64_t, SrmDocInfo*>* samedup_repost_map,
            bool need_repost_dup, int& dup_result) {
	uint64_t cont_sign = attr.get_cont_sign();
	uint64_t simhash1 = attr.get_dup_cont();
	uint64_t simhash2 = attr.get_simhash2();
    uint64_t dup = attr.get_dup();
    uint64_t repost_dup = 0;
    if (GET_FORWARD_FLAG(attr.get_digit_attr())) {
        repost_dup = dup;
        if (!need_repost_dup) {
            dup = 0;
        }
    } else {
        repost_dup = get_dup_by_mid(doc->docid);
    }
	if(cont_sign == 0 && simhash1 == 0 && simhash2 == 0 && dup == 0) {
        int degree = get_degree(doc);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [all dup=0 return:"PRIU64", degree:%d]",
                    doc->docid, degree);
		return false;
    }
    bool is_cont_dup = false, is_cont_replace = false;
    bool is_simhash_dup = false, is_simhash_replace = false;
    bool is_simhash2_dup = false, is_simhash2_replace = false;
    bool is_dup_dup  = false, is_dup_replace = false;
    bool is_repost_dup = false, is_repost_replace = false;
    __gnu_cxx::hash_map<uint64_t, SrmDocInfo*>::iterator it_cont = contsign_map->end();
    __gnu_cxx::hash_map<uint64_t, SrmDocInfo*>::iterator it_simhash = simhash_map->end();
    __gnu_cxx::hash_map<uint64_t, SrmDocInfo*>::iterator it_simhash2 = simhash2_map->end();
    __gnu_cxx::hash_map<uint64_t, SrmDocInfo*>::iterator it_dup = samedup_map->end();
    __gnu_cxx::hash_map<uint64_t, SrmDocInfo*>::iterator it_repost_dup = samedup_repost_map->end();
    // find cont_sign exist?
    if (cont_sign) {
        it_cont = contsign_map->find(cont_sign);
        if (it_cont != contsign_map->end()) {
            //int old_degree = (it_cont->second->filter_flag >> 20) & 0xF;
            int old_degree = get_degree(it_cont->second);
            int degree = get_degree(doc);
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [cont_sign:"PRIU64", degree:%d, old:%d,"PRIU64"]",
                        doc->docid, degree, old_degree, it_cont->second->docid);
            // check dup_count > max
            if (degree <= old_degree) {
                is_cont_dup = true;
                DOC_WRITE_ACDEBUG(doc, " the same cont_sign doc mid:%lu dup:"PRIU64"\t", doc->docid, cont_sign);
            } else {
                is_cont_replace = true;
            }
        }
    }
    if (simhash1) {
        it_simhash = simhash_map->find(simhash1);
        if (it_simhash != simhash_map->end()) {
            //int old_degree = (it_simhash->second->filter_flag >> 20) & 0xF;
            int old_degree = get_degree(it_simhash->second);
            int degree = get_degree(doc);
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [simhash1:"PRIU64", degree:%d, old:%d,"PRIU64"]",
                        doc->docid, degree, old_degree, it_simhash->second->docid);
            // check dup_count > max
            if (degree <= old_degree) {
                is_simhash_dup = true;
                DOC_WRITE_ACDEBUG(doc, " the same sim1 doc mid:%lu dup:"PRIU64"\t", doc->docid, simhash1);
            } else {
                is_simhash_replace = true;
            }
        }
    }
    if (simhash2) {
        it_simhash2 = simhash2_map->find(simhash2);
        if (it_simhash2 != simhash2_map->end()) {
            //int old_degree = (it_simhash2->second->filter_flag >> 20) & 0xF;
            int old_degree = get_degree(it_simhash2->second);
            int degree = get_degree(doc);
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [simhash2:"PRIU64", degree:%d, old:%d,"PRIU64"]",
                        doc->docid, degree, old_degree, it_simhash2->second->docid);
            // check dup_count > max
            if (degree <= old_degree) {
                is_simhash2_dup = true;
                DOC_WRITE_ACDEBUG(doc, " the same sim2 doc mid:%lu dup:"PRIU64"\t", doc->docid, simhash2);
            } else {
                is_simhash2_replace = true;
            }
        }
    }
    if (dup) {
        it_dup = samedup_map->find(dup);
        if (it_dup != samedup_map->end()) {
            //int old_degree = (it_dup->second->filter_flag >> 20) & 0xF;
            int old_degree = get_degree(it_dup->second);
            int degree = get_degree(doc);
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [dup:"PRIU64", degree:%d, old:%d,"PRIU64"]",
                        doc->docid, degree, old_degree, it_dup->second->docid);
            // check dup_count > max
            if (degree <= old_degree) {
                is_dup_dup = true;
                DOC_WRITE_ACDEBUG(doc, " the same dup doc mid:%lu dup:"PRIU64"\t", doc->docid, dup);
            } else {
                is_dup_replace = true;
            }
        }
    }
    if (repost_dup && need_repost_dup) {
        it_repost_dup = samedup_repost_map->find(dup);
        if (it_repost_dup != samedup_repost_map->end()) {
            //int old_degree = (it_repost_dup->second->filter_flag >> 20) & 0xF;
            int old_degree = get_degree(it_repost_dup->second);
            int degree = get_degree(doc);
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [repost_dup:"PRIU64", degree:%d, old:%d,"PRIU64"]",
                        doc->docid, degree, old_degree, it_repost_dup->second->docid);
            // check dup_count > max
            if (degree <= old_degree) {
                is_repost_dup = true;
                DOC_WRITE_ACDEBUG(doc, " the same repost_dup doc mid:%lu dup:"PRIU64"\t", doc->docid, dup);
            } else {
                is_repost_replace = true;
            }
        }
    }
    if (is_cont_dup || is_simhash_dup || is_simhash2_dup || is_dup_dup || is_repost_dup) {
        uint64_t old_cont_mid = 0, old_sim1_mid = 0, old_sim2_mid = 0, old_dup_mid = 0, old_repost_dup_mid = 0;
        if (it_cont != contsign_map->end()) {
            old_cont_mid = it_cont->second->docid;
        }
        if (it_simhash != simhash_map->end()) {
            old_sim1_mid = it_simhash->second->docid;
        }
        if (it_simhash2 != simhash2_map->end()) {
            old_sim2_mid = it_simhash2->second->docid;
        }
        if (it_dup != samedup_map->end()) {
            old_dup_mid = it_dup->second->docid;
        }
        if (it_repost_dup != samedup_repost_map->end()) {
            old_repost_dup_mid = it_repost_dup->second->docid;
        }
        if (is_dup_dup || is_repost_dup) {
            dup_result = 0;
        }
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [dup:"PRI64","PRI64","PRI64","PRI64","PRI64", docid:"PRIU64", "
                    "old:"PRI64","PRI64","PRI64","PRI64","PRI64"]",
                    cont_sign, simhash1, simhash2, dup, repost_dup, doc->docid,
                    old_cont_mid, old_sim1_mid, old_sim2_mid, old_dup_mid, old_repost_dup_mid);
        if (doc->docid == old_cont_mid ||
            doc->docid == old_sim1_mid ||
            doc->docid == old_dup_mid ||
            doc->docid == old_repost_dup_mid ||
            doc->docid == old_sim2_mid) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [already dup:"PRIU64", degree:%d]", 
                        doc->docid, get_degree(doc));
            return false;
        }
        return true;
    } 
    if (is_cont_replace) {
        //int old_degree = (it_cont->second->filter_flag >> 20) & 0xF;
        int old_degree = get_degree(it_cont->second);
        //if (old_degree < DEGREE_15_GOOD_PASS_FILTER) {
        if (old_degree < statistic_data.super_grade) {
            it_cont->second->filter_flag = 0;
            it_cont->second->filter_flag |= DEGREE_4_DUP << 20;
            AC_WRITE_DOC_DEBUG_LOG(info, it_cont->second, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE,
                        SHOW_DEBUG_NEWLINE,
                        " [cont:"PRI64", replace:"PRI64", degree(old:%d, new:%d, add:%d), add:"PRIU64"]",
                cont_sign, it_cont->second->docid, old_degree,
                get_degree(it_cont->second),
                get_degree(doc), doc->docid);
            contsign_map->erase(it_cont);
            contsign_map->insert(make_pair(cont_sign, doc));
            statistic_data.is_dup_replace_del++;
            statistic_data.topN_top--;
        }
    } else if (cont_sign && it_cont == contsign_map->end()) {
        contsign_map->insert(make_pair(cont_sign, doc));
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [dup:cont insert:"PRIU64","PRIU64"]", doc->docid, cont_sign);
    } else if (cont_sign) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [dup:cont no insert:"PRIU64","PRIU64"]", doc->docid, cont_sign);
    }
    if (is_simhash_replace) {
        //int old_degree = (it_simhash->second->filter_flag >> 20) & 0xF;
        int old_degree = get_degree(it_simhash->second);
        //if (old_degree < DEGREE_15_GOOD_PASS_FILTER) {
        if (old_degree < statistic_data.super_grade) {
            it_simhash->second->filter_flag = 0;
            it_simhash->second->filter_flag |= DEGREE_4_DUP << 20;
            AC_WRITE_DOC_DEBUG_LOG(info, it_simhash->second, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [sim1:"PRI64", replace:"PRI64", degree(old:%d,new:%d,add:%d), add:"PRIU64"]",
                    simhash1, it_simhash->second->docid, old_degree,
                    get_degree(it_simhash->second),
                    get_degree(doc), doc->docid);
            simhash_map->erase(it_simhash);
            simhash_map->insert(make_pair(simhash1, doc));
            statistic_data.is_dup_replace_del++;
            statistic_data.topN_top--;
        }
    } else if (simhash1 && it_simhash == simhash_map->end()) {
        simhash_map->insert(make_pair(simhash1, doc));
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [dup:sim1 insert:"PRIU64","PRIU64"]", doc->docid, simhash1);
    } else if (simhash1) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [dup:sim1 no insert:"PRIU64","PRIU64"]", doc->docid, simhash1);
    }
    if (is_simhash2_replace) {
        //int old_degree = (it_simhash2->second->filter_flag >> 20) & 0xF;
        int old_degree = get_degree(it_simhash2->second);
        //if (old_degree < degree_15_good_pass_filter) {
        if (old_degree < statistic_data.super_grade) {
            it_simhash2->second->filter_flag = 0;
            it_simhash2->second->filter_flag |= DEGREE_4_DUP << 20;
            AC_WRITE_DOC_DEBUG_LOG(info, it_simhash2->second, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [sim2:"PRI64", replace:"PRI64", degree(old:%d,new:%d,add:%d), add:"PRIU64"]",
                    simhash2, it_simhash2->second->docid, old_degree,
                    get_degree(it_simhash2->second),
                    get_degree(doc), doc->docid);
            simhash2_map->erase(it_simhash2);
            simhash2_map->insert(make_pair(simhash2, doc));
            statistic_data.is_dup_replace_del++;
            statistic_data.topN_top--;
        }
    } else if (simhash2 && it_simhash2 == simhash2_map->end()) {
        simhash2_map->insert(make_pair(simhash2, doc));
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [dup:sim2 insert:"PRIU64","PRIU64"]", doc->docid, simhash2);
    } else if (simhash2) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [dup:sim2 no insert:"PRIU64","PRIU64"]", doc->docid, simhash2);
    }
    if (is_dup_replace) {
        //int old_degree = (it_dup->second->filter_flag >> 20) & 0xF;
        int old_degree = get_degree(it_dup->second);
        //if (old_degree < DEGREE_15_GOOD_PASS_FILTER) {
        if (old_degree < statistic_data.super_grade) {
            it_dup->second->filter_flag = 0;
            it_dup->second->filter_flag |= DEGREE_3_DUP_DUP << 20;
            AC_WRITE_DOC_DEBUG_LOG(info, it_dup->second, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE,
                        SHOW_DEBUG_NEWLINE,
                    " [dup:"PRI64",replace:"PRI64", degree(old:%d,new:%d,add:%d), add:"PRIU64"]",
                    dup, it_dup->second->docid, old_degree,
                    get_degree(it_dup->second),
                    get_degree(doc), doc->docid);
            samedup_map->erase(it_dup);
            samedup_map->insert(make_pair(dup, doc));
            statistic_data.is_dup_replace_del++;
            statistic_data.topN_top--;
        }
    } else if (dup && it_dup == samedup_map->end()) {
        samedup_map->insert(make_pair(dup, doc));
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [dup:dup insert:"PRIU64", "PRIU64"]", doc->docid, dup);
    } else if (dup) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [dup:dup no insert:"PRIU64", "PRIU64"]", doc->docid, dup);
    }
    if (is_repost_replace) {
        //int old_degree = (it_repost_dup->second->filter_flag >> 20) & 0xF;
        int old_degree = get_degree(it_repost_dup->second);
        //if (old_degree < DEGREE_15_GOOD_PASS_FILTER) {
        if (old_degree < statistic_data.super_grade) {
            it_repost_dup->second->filter_flag = 0;
            it_repost_dup->second->filter_flag |= DEGREE_3_DUP_DUP << 20;
            AC_WRITE_DOC_DEBUG_LOG(info, it_repost_dup->second, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [repost_dup"PRI64", replace:"PRI64", degree(old:%d,new:%d,add:%d),add:"PRIU64"]",
                    repost_dup, it_repost_dup->second->docid, old_degree,
					get_degree(it_repost_dup->second),
                    get_degree(doc), doc->docid);
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [repost_dup:%d insert:"PRIU64", "PRIU64"]",
                        need_repost_dup, doc->docid, repost_dup);
            samedup_repost_map->erase(it_repost_dup);
            samedup_repost_map->insert(make_pair(repost_dup, doc));
            statistic_data.is_dup_replace_del++;
            statistic_data.topN_top--;
        }
    } else if (repost_dup && it_repost_dup == samedup_repost_map->end()) {
        samedup_repost_map->insert(make_pair(repost_dup, doc));
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [repost_dup:%d insert:"PRIU64", "PRIU64"]",
                    need_repost_dup, doc->docid, repost_dup);
    } else if (repost_dup) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [repost_dup:%d no insert:"PRIU64", "PRIU64"]",
                    need_repost_dup, doc->docid, repost_dup);
    }
    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [no message return:"PRIU64"]",
                doc->docid);
    return false;
}
static bool is_dup_update_doc(QsInfo *info, SrmDocInfo *doc,
            const AcExtDataParser& attr, Ac_statistic &statistic_data,
            bool repost_dup, int& dup_result) {
    // F_PARAM dup
	if (!doc || !statistic_data.dup_flag) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_INFO_CONT_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [!doc !dup_flag return:"PRIU64"]",
                    doc->docid);
		return false;
	}
    // bool is_dup = false;
    if (is_dup_update_doc_act(info, doc, attr, statistic_data, &statistic_data.contsign_map,
                &statistic_data.simhash_map, &statistic_data.simhash2_map,
                &statistic_data.samedup_map, &statistic_data.samedup_repost_map,
                repost_dup, dup_result)) {
        return true;
    }
    return false;
}

static int get_granularity_qi(uint64_t qi) {
	int quality = 0;
    // hit return 0:1 9 15 18
	if (qi & 0x48202ll) //qi的1、8、9、10、15、18位有值时，过滤粒度为0   12位待定
		quality =0;
    // hit return 1:0
	else if (qi & 0x10000001ll) //qi的0、13、16位有值时，过滤粒度为1   +7 28位
		quality = 1;
	else {
        // check qi 4 5 bit,
		quality = ((qi>>4)&0x3);
        if (quality > 0) {
            ++quality;
        }
    }
	return quality;
}

static int get_granularity_qi(SrmDocInfo *doc) 
{
	if( doc == NULL)
		return 0;
	uint64_t qi;
	AcExtDataParser attr(doc);
	qi = attr.get_qi();
	return get_granularity_qi(qi);
}

static bool my_time_cmp(const SrmDocInfo * a, const SrmDocInfo *b) {
	if (a->rank != b->rank){
		return a->rank > b->rank;   
	}                    
	else if (a->value != b->value) {
		return a->value > b->value; 
	}                               
	else if (a->docid != b->docid) {
		return a->docid > b->docid; 
	}                               
	else if (a->bcidx != b->bcidx) {
		return a->bcidx < b->bcidx;
	}
	else {                          
		return a->bsidx > b->bsidx; 
	}
}


//static bool is_valid_forward(SrmDocInfo *doc, const AcExtDataParser& attr, int valid_forward_fwnum) {
bool is_valid_forward(SrmDocInfo *doc, const AcExtDataParser& attr, int valid_forward_fwnum) {
	if (!doc) {
		return false;
	}
	if (GET_FORWARD_FLAG(attr.get_digit_attr())) {
		return false;
	}
	if (attr.get_validfwnm() * attr.get_fwnum() / 1000.0 >= valid_forward_fwnum) {
		return true;
	}
	return false;
}

static bool is_category_filter_doc(QsInfo *info, SrmDocInfo *doc, const AcExtDataParser& attr,
            Ac_statistic &ac_data) {
	if (!doc) {
		return true;
	}
	if (!ac_data.need_category_filter) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [pass category filter:"PRI64", !need_category_filter]", doc->docid);
		return false;
	}
    if (doc->docattr & (0x1ll << 46) || doc->docattr & (0x1ll << 47)) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [pass category filter:"PRI64","PRI64"]", doc->docid, doc->docattr);
        return false;
    }
	return true;
}

static bool is_valid_inten_ploy_doc(QsInfo *info, SrmDocInfo *doc, const AcExtDataParser& attr,
            Ac_statistic &ac_data) {
	if (!doc) {
		return false;
	}
	if (!ac_data.need_inten_ploy_filter) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [pass filter:"PRI64", !need_inten_ploy_filter]", doc->docid);
		return true;
	}
    if (attr.get_fwnum() >= ac_data.inten_ploy_fwnum_filter ||
        attr.get_likenum() >= ac_data.inten_ploy_likenum_filter) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [pass filter:"PRI64", fw:"PRI64",lk:"PRI64"]",
                doc->docid, attr.get_fwnum(), attr.get_likenum());
        return true;
    }
	return false;
}

static int get_valid_forward(SrmDocInfo *doc)
{
	if (!doc) {
		return 0;
	}

	AcExtDataParser attr(doc);
	if (GET_FORWARD_FLAG(attr.get_digit_attr())) {
		return 0;
	}
return attr.get_validfwnm() * attr.get_fwnum()/1000;
}

static bool CheckUidNotIn(QsInfo *info, uint64_t uid, uint64_t mid, std::set<uint64_t>* uid_set,
            std::map<uint64_t, AcClusterRepost>* mids) {
    std::set<uint64_t>::iterator it = uid_set->find(uid);
    if (it != uid_set->end()) {
        AC_WRITE_DOCID_DEBUG_LOG(info, mid, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
            " [CheckUidNotIn:"PRI64",dup uid:"PRI64"]", mid, uid);
        return false;
    }
    uid_set->insert(uid);
    std::map<uint64_t, AcClusterRepost>::iterator it_insert = mids->find(mid);
    if (it_insert != mids->end()) {
        AC_WRITE_DOCID_DEBUG_LOG(info, mid, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
            " [CheckUidNotIn:dup mid:"PRI64","PRI64"]", mid, uid);
        return false;
    }
    return true;
}

static int ClusterRepostDataUpdateAdd(QsInfo *info, int max_num, SrmDocInfo** doc,
            AcDupRepostClusterDataUpdate *data,
            std::set<uint64_t> *uid_set,
            std::map<uint64_t, AcClusterRepost>* mids,
            int *left_num, const string& type) {
    for (int i = 0; i < max_num; i++) {
        if (*left_num > 0 && *doc &&
            CheckUidNotIn(info, GetDomain(*doc), (*doc)->docid, uid_set, mids)) {
            *left_num -= 1;
            AcClusterRepost tmpdata;
            tmpdata.rank = (*doc)->rank;
            tmpdata.uid = GetDomain(*doc);
            mids->insert(make_pair((*doc)->docid, tmpdata));
            AC_WRITE_DOC_DEBUG_LOG(info, (*doc), PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster:"PRIU64" get_mid %s:"PRI64",%d,t:%u, size:%u,left_num:%d]",
                dup, type.c_str(), (*doc)->docid, (*doc)->filter_flag,
                (*doc)->rank, data->all_users.size(), *left_num);
        }
        ++doc;
    }
}
static int ClusterRepostDataUpdateAddOther(QsInfo *info,
            Ac_statistic &statistic_data,
            AcDupRepostClusterDataUpdate *data,
            SrmDocInfo* doc, int left_num,
            std::map<std::string, SrmDocInfo*>* time_order_other,
            const std::string& d_info) {
    if (doc) {
        char rank_mid[128];
        snprintf(rank_mid, 128, "%u_"PRIU64"", doc->rank, doc->docid);
        time_order_other->insert(make_pair(rank_mid, doc));
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster:"PRIU64" add other:%s,:"PRI64",t:%u, size:%lu,left_num:%d]",
            dup, d_info.c_str(), doc->docid, doc->rank, data->all_users.size(), left_num);
    }
}

static SrmDocInfo* UpdateClusterFwnumNewest(QsInfo *info,
            SrmDocInfo *doc,
            Ac_statistic &statistic_data,
            AcDupRepostClusterDataUpdate* data) {
    AcExtDataParser attr(doc);
    uint64_t fwnum = attr.get_fwnum();
    /*
    if (fwnum < statistic_data.cluster_fwnum) {
        return doc;
    }
    */
    if (data->fwnum_newest_doc == NULL) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster insert fwnum_newest_doc NULL:"PRI64","PRI64">"PRI64"]",
            doc->docid, fwnum, statistic_data.cluster_fwnum);
        data->fwnum_newest_doc = doc;
        data->fwnum_newest_rank = doc->rank;
        if (fwnum > data->fwnum_num) {
            data->fwnum_num = fwnum;
        }
        return NULL;
    }
    if (doc->rank > data->fwnum_newest_rank) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster update fwnum_newest_doc:"PRI64","PRI64">"PRI64"]",
            doc->docid, fwnum, statistic_data.cluster_fwnum);
        AC_WRITE_DOC_DEBUG_LOG(info, data->fwnum_newest_doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster update old fwnum_newest_doc:"PRI64"]",
            data->fwnum_newest_doc->docid);
        SrmDocInfo* old_doc = data->fwnum_newest_doc;
        data->fwnum_newest_doc = doc;
        data->fwnum_newest_rank = doc->rank;
        if (fwnum > data->fwnum_num) {
            data->fwnum_num = fwnum;
        }
        return old_doc;
    }
    return doc;
}

static int ClusterRepostDataUpdate(QsInfo *info,
            Ac_statistic &statistic_data, uint64_t dup,
            AcDupRepostClusterDataUpdate *data,
            std::map<uint64_t, AcClusterRepost>* mids, int left_num,
            int* social_add) {
    int vip_add = 0, first_add = 0;
    std::set<uint64_t> uid_set;
    if (data->root_doc) {
        if (data->valid_count < statistic_data.cluster_root_valid_num + 1) {
            AC_WRITE_DOC_DEBUG_LOG(info, data->root_doc, PRINT_CLUSTER_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [%d insert already del mid dup:"PRI64", mid:"PRI64",count:%d,root_cluster:%d,size:%u"
                    ", valid_count:%d < cluster_root_valid_num:%d + 1]",
                    __LINE__, dup, data->root_doc->docid, data->count, data->root_cluster,
                    data->all_users.size(), data->valid_count, statistic_data.cluster_root_valid_num);
            statistic_data.already_del_mid.insert(dup);
            std::vector<SrmDocInfo*>::iterator it = data->all_users.begin();
            for (; it != data->all_users.end();++it) {
                AC_WRITE_DOC_DEBUG_LOG(info, data->root_doc, PRINT_CLUSTER_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [root:"PRI64", mid:"PRI64","PRI64"]",
                        dup, data->root_doc->docid, (*it)->docid);
            }
            //int degree = ((data->root_doc->filter_flag)>>20) & 0xF;
            int degree = get_degree(data->root_doc);
            //if (degree >= DEGREE_9_GOOD) {
            if (degree >= statistic_data.good_grade) {
                data->keep_root_doc = 1;
                AC_WRITE_DOC_DEBUG_LOG(info, data->root_doc, PRINT_CLUSTER_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [keep_root_doc:"PRI64", degree:%d]",
                        data->root_doc->docid, degree);
            }
            return 1;
        }
    } else if (data->first_ok_doc) {
        if (data->valid_count >= statistic_data.cluster_repost_valid_num) {
            AC_WRITE_DOC_DEBUG_LOG(info, data->first_ok_doc, PRINT_CLUSTER_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [pass,valid_count:%d >= repost_valid_num:%d, dup:"PRI64", mid:"PRI64",size:%u]",
                    data->valid_count, statistic_data.cluster_repost_valid_num,
                    dup, data->first_ok_doc->docid, data->all_users.size());
        } else if (data->vip_doc[0] || data->social_doc[0]) {
            AC_WRITE_DOC_DEBUG_LOG(info, data->first_ok_doc, PRINT_CLUSTER_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [pass,have vip or social, dup:"PRI64", mid:"PRI64",size:%u]",
                    dup, data->first_ok_doc->docid, data->all_users.size());
        } else {
            statistic_data.already_del_mid.insert(dup);
            AC_WRITE_DOC_DEBUG_LOG(info, data->first_ok_doc, PRINT_CLUSTER_DUP_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [%d,insert already del mid dup:"PRI64", mid:"PRI64",size:%u"
                    ", valid_count:%d < cluster_repost_valid_num:%d]",
                    __LINE__, dup, data->first_ok_doc->docid, data->all_users.size(),
                    data->valid_count, statistic_data.cluster_repost_valid_num);
            return 2;
        }
    } else {
        statistic_data.already_del_mid.insert(dup);
        return 3;
    }
    // first newest doc
    if (left_num > 0 && data->fwnum_newest_doc &&
        CheckUidNotIn(info, GetDomain(data->fwnum_newest_doc),
            data->fwnum_newest_doc->docid, &uid_set, mids)) {
        --left_num;
        AcClusterRepost tmpdata;
        tmpdata.rank = data->fwnum_newest_doc->rank;
        tmpdata.uid = GetDomain(data->fwnum_newest_doc);
        mids->insert(make_pair(data->fwnum_newest_doc->docid, tmpdata));
        AC_WRITE_DOC_DEBUG_LOG(info, data->fwnum_newest_doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster:"PRIU64" get_mid newest_doc:"PRI64",%d,t:%u, size:%lu,left_num:%d]",
            dup, data->fwnum_newest_doc->docid, data->fwnum_newest_doc->filter_flag,
            data->fwnum_newest_doc->rank, data->all_users.size(), left_num);
    }
    if (left_num <= 0) {
        return 0;
    }
    ClusterRepostDataUpdateAdd(info, CLUSTER_SOCIAL_VIP_NUM, data->social_doc,
                data, &uid_set, mids, &left_num, "social");
    if (left_num <= 0) {
        return 0;
    }
    ClusterRepostDataUpdateAdd(info, CLUSTER_SOCIAL_VIP_NUM, data->vip_doc,
                data, &uid_set, mids, &left_num, "vip");
    if (left_num <= 0) {
        return 0;
    }
    if (data->fwnum_doc) {
        AcExtDataParser fwnum_attr(data->fwnum_doc);
        uint64_t fwnum = fwnum_attr.get_fwnum();
        if (fwnum == data->fwnum_num && left_num > 0 && data->fwnum_doc &&
            CheckUidNotIn(info, GetDomain(data->fwnum_doc), data->fwnum_doc->docid, &uid_set, mids)) {
            --left_num;
            AcClusterRepost tmpdata;
            tmpdata.rank = data->fwnum_doc->rank;
            tmpdata.uid = GetDomain(data->fwnum_doc);
            mids->insert(make_pair(data->fwnum_doc->docid, tmpdata));
            AC_WRITE_DOC_DEBUG_LOG(info, data->fwnum_doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster:"PRIU64" get_mid fwnum_doc:"PRI64",%d,t:%u, size:%lu,left_num:%d]",
                dup, data->fwnum_doc->docid, data->fwnum_doc->filter_flag,
                data->fwnum_doc->rank, data->all_users.size(), left_num);
        }
    }
    /*
    if (data->fwnum_doc) {
        AcExtDataParser fwnum_attr(data->fwnum_doc);
        uint64_t fwnum = fwnum_attr.get_fwnum();
        if (fwnum == data->fwnum_num) {
            ClusterRepostDataUpdateAddOther(info, statistic_data, data,
                        data->fwnum_doc, left_num, &time_order_other, "fwnum");
            data->fwnum_num_skip_updatetime_mid = data->fwnum_doc->docid;
        } else {
            UpdateClusterFwnumNewest(info, data->fwnum_doc, statistic_data, data);
        }
    }
    for (int i = 0; i < CLUSTER_SOCIAL_VIP_NUM; ++i) {
        if (data->valid_user_doc[i]) {
            ClusterRepostDataUpdateAddOther(info, statistic_data, data,
                        data->valid_user_doc[i], left_num, &time_order_other, "valid_user");
        }
    }
    std::map<std::string, SrmDocInfo*>::reverse_iterator rit = time_order_other.rbegin();
    for (; rit != time_order_other.rend(); ++rit) {
        if (left_num <= 0) {
            return 0;
        }
        if (left_num > 0 && rit->second &&
            CheckUidNotIn(info, GetDomain(rit->second), rit->second->docid, &uid_set, mids)) {
            --left_num;
            AcClusterRepost tmpdata;
            tmpdata.rank = rit->second->rank;
            mids->insert(make_pair(rit->second->docid, tmpdata));
            AC_WRITE_DOC_DEBUG_LOG(info, rit->second, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster:"PRIU64" get_mid other:"PRI64",%d,t:%u, size:%lu,left_num:%d]",
                dup, rit->second->docid, rit->second->filter_flag,
                rit->second->rank, data->all_users.size(), left_num);
        }
    }
    */
    return 0;
}

static int ClusterRepostSetDegree1Update(QsInfo *info,
            const std::vector<SrmDocInfo*>& all_users,
            Ac_statistic &statistic_data,
            const std::set<uint64_t>& cluster_show_mids,
            const SrmDocInfo* first_ok_doc) {
    std::vector<SrmDocInfo*>::const_iterator it = all_users.begin();
    for (; it != all_users.end(); ++it) {
        if (!statistic_data.is_cluster_repost &&
            cluster_show_mids.find((*it)->docid) != cluster_show_mids.end()) {
            AC_WRITE_DOC_DEBUG_LOG(info, (*it), PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                            " [is_cluster_repost=0, so donot del:"PRIU64"]", (*it)->docid);
            continue;
        }
        if (first_ok_doc &&
            first_ok_doc->docid == (*it)->docid && statistic_data.is_cluster_repost &&
            first_ok_doc->cluster_info.mid[0]) {
            AC_WRITE_DOC_DEBUG_LOG(info, first_ok_doc, IOS_CLUSTER_LOG, SHOW_DEBUG_NEWLINE,
                    " [set: pass first_ok_doc set degree 1:"PRIU64"]", first_ok_doc->docid);
            continue;
        }
        AC_WRITE_DOC_DEBUG_LOG(info, (*it), PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [cluster:set degree 1:"PRI64"]", (*it)->docid);
        (*it)->filter_flag = 0;
        (*it)->filter_flag |= DEGREE_1_FORWARD_TIME << 20;
        statistic_data.cluster_del_degree1++;
    }
    return 0;
}

static int GetClusterShowMidsUpdate(QsInfo *info,
            Ac_statistic &statistic_data,
            const AcDupRepostClusterDataUpdate& data,
            std::set<uint64_t>* cluster_show_mids,
            bool clear) {
    if (data.root_doc) {
        if (!(data.root_doc->filter_flag & 0x1)) {
            for (int i = 0; i < CLUSTER_INFO_CACHE_MID_COUNT; ++i) {
                if (data.root_doc->cluster_info.mid[i]) {
                    AC_WRITE_DOCID_DEBUG_LOG(info, data.root_doc->cluster_info.mid[i],
                                IOS_CLUSTER_LOG, SHOW_DEBUG_NEWLINE,
                                " [cluster_show:"PRIU64", %d]", data.root_doc->cluster_info.mid[i], clear);
                    cluster_show_mids->insert(data.root_doc->cluster_info.mid[i]);
                    if (!statistic_data.is_cluster_repost && clear) {
                        data.root_doc->cluster_info.mid[i] = 0;
                    }
                }
            }
            /*
            if (data.first_ok_doc && !(data.first_ok_doc->filter_flag & 0x1)) {
                AC_WRITE_DOC_DEBUG_LOG(info, data.first_ok_doc, IOS_CLUSTER_LOG, SHOW_DEBUG_NEWLINE,
                            " [cluster_show:"PRIU64", %d]", data.first_ok_doc->docid, clear);
                cluster_show_mids->insert(data.first_ok_doc->docid);
            }
            */
        } else {
            AC_WRITE_DOC_DEBUG_LOG(info, data.root_doc, IOS_CLUSTER_LOG, SHOW_DEBUG_NEWLINE,
                        " [cluster_show root 0x1:"PRIU64"]", data.root_doc->docid);
        }
    }
    if (data.first_ok_doc) {
        if (!(data.first_ok_doc->filter_flag & 0x1)) {
            for (int i = 0; i < CLUSTER_INFO_CACHE_MID_COUNT; ++i) {
                if (data.first_ok_doc->cluster_info.mid[i]) {
                    AC_WRITE_DOCID_DEBUG_LOG(info, data.first_ok_doc->cluster_info.mid[i],
                                IOS_CLUSTER_LOG, SHOW_DEBUG_NEWLINE,
                                " [cluster_show:"PRIU64", %d]", data.first_ok_doc->cluster_info.mid[i], clear);
                    cluster_show_mids->insert(data.first_ok_doc->cluster_info.mid[i]);
                    if (!statistic_data.is_cluster_repost && clear) {
                        data.first_ok_doc->cluster_info.mid[i] = 0;
                    }
                }
            }
        } else {
            AC_WRITE_DOC_DEBUG_LOG(info, data.first_ok_doc, IOS_CLUSTER_LOG, SHOW_DEBUG_NEWLINE,
                        " [cluster_show first_ok_doc 0x1:"PRIU64"]", data.first_ok_doc->docid);
        }
    }
    return 0;
}

static int ClusterRepostFlagUpdate(QsInfo *info,
            const AcDupRepostClusterDataUpdate& data, Ac_statistic &statistic_data) {
    std::set<uint64_t> cluster_show_mids;
    GetClusterShowMidsUpdate(info, statistic_data, data, &cluster_show_mids, false);
    if (!data.root_doc && data.first_ok_doc) {
        if (statistic_data.is_cluster_repost) {
            AC_WRITE_DOC_DEBUG_LOG(info, data.first_ok_doc, IOS_CLUSTER_LOG, SHOW_DEBUG_NEWLINE,
                        " [set: pass first_ok_doc set 0x1:"PRIU64"]", data.first_ok_doc->docid);
            cluster_show_mids.insert(data.first_ok_doc->docid);
        } else {
            AC_WRITE_DOC_DEBUG_LOG(info, data.first_ok_doc, IOS_CLUSTER_LOG, SHOW_DEBUG_NEWLINE,
                        " [set: del first_ok_doc set 0x1:"PRIU64"]", data.first_ok_doc->docid);
        }
        ClusterRepostSetDegree1Update(info, data.all_users,
                    statistic_data, cluster_show_mids, data.first_ok_doc);
    } else {
        if (data.all_users.size()) {
            AC_WRITE_DEBUG_LOG(info, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [cluster setdegree1 all]");
            ClusterRepostSetDegree1Update(info, data.all_users, statistic_data, cluster_show_mids, NULL);
        }
    }
    return 0;
}

static int ClusterRepostDelActOneUpdate(QsInfo *info,
            Ac_statistic &statistic_data,
            const AcDupRepostClusterDataUpdate& data,
            const std::vector<SrmDocInfo*>& all_users,
            const std::set<uint64_t>& cluster_show_mids,
            const SrmDocInfo* first_ok_doc) {
    std::vector<SrmDocInfo*>::const_iterator it = all_users.begin();
    for (; it != all_users.end(); ++it) {
        if (((*it)->filter_flag & 0x1) == 1) {
            continue;
        }
        if (!statistic_data.is_cluster_repost &&
            cluster_show_mids.find((*it)->docid) != cluster_show_mids.end()) {
            AC_WRITE_DOC_DEBUG_LOG(info, (*it), PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                            " [is_cluster_repost=0, so donot del:"PRIU64"]", (*it)->docid);
            continue;
        }
        if (statistic_data.is_cluster_repost && data.keep_root_doc &&
            data.root_doc == *it) {
            AC_WRITE_DOC_DEBUG_LOG(info, (*it), PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [keep_root_doc:"PRI64"]", data.root_doc->docid);
            continue;
        }
        if (first_ok_doc &&
            first_ok_doc->docid == (*it)->docid && statistic_data.is_cluster_repost &&
            first_ok_doc->cluster_info.mid[0]) {
            AC_WRITE_DOC_DEBUG_LOG(info, first_ok_doc, IOS_CLUSTER_LOG, SHOW_DEBUG_NEWLINE,
                    " [del: pass first_ok_doc del 0x1:"PRIU64"]", first_ok_doc->docid);
            continue;
        }
        (*it)->filter_flag |= 0x1;
        AC_WRITE_DOC_DEBUG_LOG(info, (*it), PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [cluster:del,0x1:"PRI64"]", (*it)->docid);
    }
    return 0;
}

static int ClusterRepostDelActUpdate(QsInfo *info, Ac_statistic &statistic_data,
            const AcDupRepostClusterDataUpdate& data) {
    std::set<uint64_t> cluster_show_mids;
    GetClusterShowMidsUpdate(info, statistic_data, data, &cluster_show_mids, true);
    if (data.first_ok_doc) {
        AC_WRITE_DOC_DEBUG_LOG(info, data.first_ok_doc, IOS_CLUSTER_LOG, SHOW_DEBUG_NEWLINE,
                    " [first_ok_doc info:"PRIU64",%u, %u]", data.first_ok_doc->docid,
                    (data.root_doc == NULL), cluster_show_mids.size());
    } else if (data.root_doc) {
        AC_WRITE_DOC_DEBUG_LOG(info, data.root_doc, IOS_CLUSTER_LOG, SHOW_DEBUG_NEWLINE,
                    " [root info:"PRIU64",%u,keep_root_doc:%d]",
                    data.root_doc->docid, cluster_show_mids.size(),data.keep_root_doc);
    }
    if (!data.root_doc && data.first_ok_doc) {
        if (statistic_data.is_cluster_repost) {
            AC_WRITE_DOC_DEBUG_LOG(info, data.first_ok_doc, IOS_CLUSTER_LOG, SHOW_DEBUG_NEWLINE,
                        " [del: pass first_ok_doc set 0x1:"PRIU64"]", data.first_ok_doc->docid);
            cluster_show_mids.insert(data.first_ok_doc->docid);
        } else {
            AC_WRITE_DOC_DEBUG_LOG(info, data.first_ok_doc, IOS_CLUSTER_LOG, SHOW_DEBUG_NEWLINE,
                        " [del: del first_ok_doc set 0x1:"PRIU64"]", data.first_ok_doc->docid);
        }
        ClusterRepostDelActOneUpdate(info, statistic_data, data, data.all_users, cluster_show_mids, data.first_ok_doc);
    } else {
        if (data.all_users.size()) {
            AC_WRITE_DEBUG_LOG(info, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [cluster ClusterRepostDelActUpdate all]");
            ClusterRepostDelActOneUpdate(info, statistic_data, data, data.all_users, cluster_show_mids, NULL);
        }
    }
    if (statistic_data.is_cluster_repost &&
        data.root_doc && data.first_ok_doc &&
        !(data.first_ok_doc->filter_flag & 0x1)) {
        AC_WRITE_DEBUG_LOG(info, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
            " [cluster ClusterRepostDelActUpdate first_ok_doc, data:"PRI64"]", data.uniq);
        data.first_ok_doc->filter_flag |= 0x1;
        AC_WRITE_DOC_DEBUG_LOG(info, data.first_ok_doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [root and first_ok_doc all:del first_ok_doc,0x1:"PRI64"]", data.first_ok_doc->docid);
    }
    return 0;
}

static int ClusterRepostDelUpdate(QsInfo *info, Ac_statistic &statistic_data) {
    AC_WRITE_LOG(info," [ClusterRepostDelUpdate,%d]",statistic_data.cluster_data_update.size());
    __gnu_cxx::hash_map<uint64_t, AcDupRepostClusterDataUpdate>::iterator it =
                statistic_data.cluster_data_update.begin();
    for (; it != statistic_data.cluster_data_update.end(); ++it) {
        ClusterRepostDelActUpdate(info, statistic_data, it->second);
        __gnu_cxx::hash_set<uint64_t>::iterator it_del = statistic_data.already_del_mid.find(it->first);
        if (it_del == statistic_data.already_del_mid.end()) {
            continue;
        }
        // in del map, so del dup_cluster
        if (it->second.root_doc && !(it->second.root_doc->filter_flag & 0x1)) {
            if (it->second.keep_root_doc == 0) {
                it->second.root_doc->filter_flag |= 0x1;
                AC_WRITE_DOC_DEBUG_LOG(info, it->second.root_doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                                " [in del_map root cluster_del_dup::"PRI64","PRI64"]",
                                it->second.root_doc->docid, it->first);
            } else {
                AC_WRITE_DOC_DEBUG_LOG(info, it->second.root_doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                                " [in del_map root keep by keep_root_doc:"PRI64","PRI64"]",
                                it->second.root_doc->docid, it->first);
            }
        }
        if (it->second.first_ok_doc && !(it->second.first_ok_doc->filter_flag & 0x1)) {
            it->second.first_ok_doc->filter_flag |= 0x1;
            AC_WRITE_DOC_DEBUG_LOG(info, it->second.first_ok_doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                            " [in del_map first_ok_doc cluster_del_dup::"PRI64","PRI64"]",
                            it->second.first_ok_doc->docid, it->first);
        }
    }
    return 0;
}

static int ClusterRepostCheckPicSourceOne(QsInfo *info, Ac_statistic &statistic_data,
            AcDupRepostClusterDataUpdate *data) {
    int have_error = 0;
    if (data->root_doc) {
        return 0;
    } else if (data->first_ok_doc) {
        if (data->fwnum_newest_doc) {
            AcExtDataParser attr(data->fwnum_newest_doc);
            uint64_t digit_attr = attr.get_digit_attr();
            if (CheckPicSource(info, data->fwnum_newest_doc, statistic_data, attr, digit_attr)) {
                info->bcinfo.del_doc(data->fwnum_newest_doc);
                data->fwnum_newest_doc = NULL;
                data->valid_count--;
                have_error++;
            }
        }
        // data->valid_count--;
        if (data->fwnum_doc) {
            AcExtDataParser attr(data->fwnum_doc);
            uint64_t digit_attr = attr.get_digit_attr();
            if (CheckPicSource(info, data->fwnum_doc, statistic_data, attr, digit_attr)) {
                info->bcinfo.del_doc(data->fwnum_doc);
                data->fwnum_doc = NULL;
                data->valid_count--;
                have_error++;
            }
        }
        for (int i = 0; i < CLUSTER_SOCIAL_VIP_NUM; i++) {
            if (data->vip_doc[i]) {
                AcExtDataParser attr(data->vip_doc[i]);
                uint64_t digit_attr = attr.get_digit_attr();
                if (CheckPicSource(info, data->vip_doc[i], statistic_data, attr, digit_attr)) {
                    info->bcinfo.del_doc(data->vip_doc[i]);
                    data->vip_doc[i] = NULL;
                    data->valid_count--;
                    have_error++;
                }
            }
        }
        for (int i = 0; i < CLUSTER_SOCIAL_VIP_NUM; i++) {
            if (data->social_doc[i]) {
                AcExtDataParser attr(data->social_doc[i]);
                uint64_t digit_attr = attr.get_digit_attr();
                if (CheckPicSource(info, data->social_doc[i], statistic_data, attr, digit_attr)) {
                    info->bcinfo.del_doc(data->social_doc[i]);
                    data->social_doc[i] = NULL;
                    data->valid_count--;
                    have_error++;
                }
            }
        }
    }
    return have_error;
}

static int ClusterRepostCheckPicSource(QsInfo *info, Ac_statistic &statistic_data) {
    std::map<uint64_t, ClusterDocid>::reverse_iterator rit = statistic_data.cluster_for_picsource_check.rbegin();
    int index = 0, act = 0, cluster_check = 0;
    for (; index < statistic_data.cluster_first_page_num_actcluster &&
           rit != statistic_data.cluster_for_picsource_check.rend(); ++rit) {
        act++;
        uint64_t dup = rit->second.dup;
        if (rit->second.act) {
            index++;
            continue;
        }
        uint32_t act = rit->second.dup;
        __gnu_cxx::hash_map<uint64_t, AcDupRepostClusterDataUpdate>::iterator it =
            statistic_data.cluster_data_update.find(dup);
        if (it != statistic_data.cluster_data_update.end()) {
            cluster_check++;
            int have_error = ClusterRepostCheckPicSourceOne(info, statistic_data, &(it->second));
            if (have_error) {
                AC_WRITE_DOC_DEBUG_LOG(info, it->second.first_ok_doc,
                            PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [cluster,repost:"PRI64", pic_check_failed:%d]",
                        it->second.first_ok_doc->docid, have_error);
            } else {
                index++;
            }
        }
    }
    AC_WRITE_LOG(info," [cluster_check_pic:ok%d,act:%d,check:%d,size:%d,%d]", index, act, cluster_check,
                statistic_data.cluster_for_picsource_check.size(),
                statistic_data.cluster_data_update.size());
}
static int ClusterRepostUpdate(QsInfo *info, Ac_statistic &statistic_data) {
    if(statistic_data.check_pic_source) {
        ClusterRepostCheckPicSource(info, statistic_data);
    }
    QsInfoBC *bcinfo = &info->bcinfo;
    int num = bcinfo->get_doc_num();
    __gnu_cxx::hash_map<uint64_t, AcDupRepostClusterDataUpdate>::iterator it =
                statistic_data.cluster_data_update.begin();
    for (; it != statistic_data.cluster_data_update.end(); ++it) {
        if (it->second.root_doc == NULL &&
            it->second.first_ok_doc == NULL) {
            AC_WRITE_DEBUG_LOG(info, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [cluster only_bad dup:"PRI64"]", it->first);
            ClusterRepostFlagUpdate(info, it->second, statistic_data);
            continue;
        }
        std::map<uint64_t, AcClusterRepost> mids;
        int left_num = CLUSTER_INFO_CACHE_MID_COUNT;
        int social_add = 0;
        if (ClusterRepostDataUpdate(info, statistic_data,
                        it->first, &(it->second), &mids, left_num, &social_add)) {
            AC_WRITE_DEBUG_LOG(info, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [cluster not much dup:"PRI64"]", it->first);
            ClusterRepostFlagUpdate(info, it->second, statistic_data);
            continue;
        }
        /*
        if (!statistic_data.is_cluster_repost) {
            AC_WRITE_DEBUG_LOG(info, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [cluster not cluster dup:"PRI64"]", it->first);
            ClusterRepostFlagUpdate(info, it->second, statistic_data);
            continue;
        }
        */
        std::map<uint64_t, AcClusterRepost>::reverse_iterator rit = mids.rbegin();
        int i = 0;
        uint64_t newest_time = 0, oldest_time = -1;
        if (it->second.root_doc) {
            std::string mid_str;
            for (; rit != mids.rend() && i < CLUSTER_INFO_CACHE_MID_COUNT; ++rit, ++i) {
                char tmp_mid_str[128];
                tmp_mid_str[0] = 0;
                int len = snprintf(tmp_mid_str, 128, ","PRI64, rit->first);
                mid_str += tmp_mid_str; 
                if (statistic_data.fwnum_update_cluster_time ||
                    it->second.fwnum_num_skip_updatetime_mid == 0 ||
                    rit->first != it->second.fwnum_num_skip_updatetime_mid) {
                    if (newest_time < rit->second.rank) {
                        newest_time = rit->second.rank;
                    }
                    if (oldest_time > rit->second.rank) {
                        oldest_time = rit->second.rank;
                    }
                }
                it->second.root_doc->cluster_info.mid[i] = rit->first;
                it->second.root_doc->cluster_info.cont_sign[i] = rit->second.cont_sign;
                it->second.root_doc->cluster_info.uid[i] = rit->second.uid;
                AC_WRITE_DOCID_DEBUG_LOG(info, rit->first, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [cluster,set_mid root:"PRI64"]", rit->first);
                AC_WRITE_DOC_DEBUG_LOG(info, it->second.root_doc,
                    PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [cluster,root:"PRI64", set_mid:"PRI64"]", it->second.root_doc->docid, rit->first);
            }
            if (statistic_data.is_cluster_repost) {
                if (oldest_time) {
                    it->second.root_doc->cluster_info.oldest_time = oldest_time;
                }
                if (newest_time && !it->second.root_cluster) {
                    AC_WRITE_DEBUG_LOG(info, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [root:"PRI64", rank:"PRI64"]",
                        it->second.root_doc->docid, newest_time);
                    it->second.root_doc->cluster_info.time = newest_time;
                    if (newest_time > it->second.root_doc->rank) {
                        it->second.root_doc->rank = newest_time;
                    }
                    AC_WRITE_DOC_DEBUG_LOG(info, it->second.root_doc,
                        PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [cluster,root:"PRI64", set_cluster_time:%u, root:%d,%s]",
                        it->second.root_doc->docid, it->second.root_doc->cluster_info.time,
                        it->second.valid_count, mid_str.c_str());
                } else {
                    if (mids.size() > 0) {
                        if (it->second.root_doc->time_old) {
                            it->second.root_doc->cluster_info.time = it->second.root_doc->time_old;
                        } else {
                            it->second.root_doc->cluster_info.time = it->second.root_doc->rank;
                        }
                    }
                    AC_WRITE_DOC_DEBUG_LOG(info, it->second.root_doc,
                        PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [set orig,root:"PRI64"]", it->second.root_doc->docid);
                    AC_WRITE_DOC_DEBUG_LOG(info, it->second.root_doc,
                        PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [cluster,root:"PRI64", set_cluster_time:%u root_itself:%d, %u,%s]",
                        it->second.root_doc->docid, it->second.root_doc->cluster_info.time,
                        it->second.valid_count, mids.size(), mid_str.c_str());
                }
            }
        } else if (it->second.first_ok_doc) {
            std::string mid_str;
            for (; rit != mids.rend() && i < CLUSTER_INFO_CACHE_MID_COUNT; ++rit, ++i) {
                char tmp_mid_str[128];
                tmp_mid_str[0] = 0;
                int len = snprintf(tmp_mid_str, 128, ","PRI64, rit->first);
                mid_str += tmp_mid_str; 
                /*
                if (statistic_data.fwnum_update_cluster_time ||
                    it->second.fwnum_num_skip_updatetime_mid == 0 ||
                    rit->first != it->second.fwnum_num_skip_updatetime_mid) {
                }
                */
                if (newest_time < rit->second.rank) {
                    newest_time = rit->second.rank;
                }
                if (oldest_time > rit->second.rank) {
                    oldest_time = rit->second.rank;
                }
                it->second.first_ok_doc->cluster_info.mid[i] = rit->first;
                it->second.first_ok_doc->cluster_info.cont_sign[i] = rit->second.cont_sign;
                it->second.first_ok_doc->cluster_info.uid[i] = rit->second.uid;
                AC_WRITE_DOCID_DEBUG_LOG(info, rit->first, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [cluster,set_mid repost:"PRI64"]", rit->first);
                AC_WRITE_DOC_DEBUG_LOG(info, it->second.first_ok_doc,
                    PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [cluster,first_ok repost:"PRI64", set_mid:"PRI64"]",
                    it->second.first_ok_doc->docid, rit->first);
            }
            if (statistic_data.is_cluster_repost) {
                if (oldest_time) {
                    it->second.first_ok_doc->cluster_info.oldest_time = oldest_time;
                }
                if (newest_time && !it->second.root_cluster) {
                    AC_WRITE_DEBUG_LOG(info, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [repost:"PRI64", rank:"PRI64"]", it->second.first_ok_doc->docid, newest_time);
                    it->second.first_ok_doc->cluster_info.time = newest_time;
                    it->second.first_ok_doc->rank = newest_time;
                    AC_WRITE_DOC_DEBUG_LOG(info, it->second.first_ok_doc,
                        PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [cluster,repost:"PRI64", set_cluster_time:%u, repost:%d,%s]",
                        it->second.first_ok_doc->docid, it->second.first_ok_doc->cluster_info.time,
                        it->second.valid_count, mid_str.c_str());
                } else {
                    if (mids.size() > 0) {
                        // set fake time 1230739200 2009-01-01
                        //it->second.first_ok_doc->cluster_info.time = it->second.first_ok_doc->rank;
                    }
                    AC_WRITE_DEBUG_LOG(info, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [set first time ERROR,repost:"PRI64"]", it->second.first_ok_doc->docid);
                    AC_WRITE_DOC_DEBUG_LOG(info, it->second.first_ok_doc,
                        PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [cluster,repost:"PRI64", set_cluster_time:%u repost_itself,%s]",
                        it->second.first_ok_doc->docid, it->second.first_ok_doc->cluster_info.time,
                        mid_str.c_str());
                }
            }
            if (statistic_data.is_cluster_repost) {
                it->second.first_ok_doc->docid = 0;
            }
        }
        ClusterRepostFlagUpdate(info, it->second, statistic_data);
    }
    return 0;
}

static int check_invalid_timel(QsInfo *info, Ac_statistic &statistic_data, SrmDocInfo *doc) {
    int timel = info->getTimeliness();
    int curtime = statistic_data.curtime;
    int timel_limit = int_from_conf(CLUSTER_TIMEL_LIMIT, 0);
    if (timel_limit && timel >= 4 && doc->rank < curtime - timel_limit) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CONTINUE_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        "[cluster failed:"PRIU64", timel >= 4 rank:%u < curtime:%d - %d]",
                        doc->docid, doc->rank, curtime, timel_limit);
        return true;
    }
    return false;
}

static SrmDocInfo* UpdateClusterBase(QsInfo *info,
            AcDupRepostClusterDataUpdate* data,
            SrmDocInfo *doc, const AcExtDataParser& attr,
            SrmDocInfo **update, const string& type) {
    uint64_t fwnum = attr.get_fwnum();
    SrmDocInfo **first = update;
    SrmDocInfo **second= update + 1;
    SrmDocInfo **third = update + 2;
    if (*first == NULL) {
        *first = doc;
        if (fwnum > data->fwnum_num) {
            data->fwnum_num = fwnum;
        }
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster update %s 0:"PRI64",%d]", type.c_str(), doc->docid, doc->filter_flag);
    } else if (*second == NULL) {
        uint64_t first_uid = GetDomain(*first);
        uint64_t uid = GetDomain(doc);
        if (first_uid && uid && first_uid == uid) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s miss for dup uid:"PRI64","PRI64","PRI64"]",
                type.c_str(), doc->docid, uid, first_uid);
            return doc;
        }
        if (fwnum > data->fwnum_num) {
            data->fwnum_num = fwnum;
        }
        if ((*first)->rank < doc->rank) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 0:"PRI64"]", type.c_str(), doc->docid);
            AC_WRITE_DOC_DEBUG_LOG(info, (*first), PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 1:"PRI64"]", type.c_str(), (*first)->docid);
            *second = *first;
            *first = doc;
        } else {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 1:"PRI64"]", type.c_str(), doc->docid);
            *second = doc;
        }
    } else if (*third == NULL) {
        uint64_t first_uid = GetDomain(*first);
        uint64_t second_uid = GetDomain(*second);
        uint64_t uid = GetDomain(doc);
        if (first_uid && uid && second_uid &&
            (uid == first_uid || uid == second_uid)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s miss for dup uid s:"PRI64","PRI64","PRI64","PRI64"]",
                type.c_str(), doc->docid, uid, first_uid, second_uid);
            return doc;
        }
        if (fwnum > data->fwnum_num) {
            data->fwnum_num = fwnum;
        }
        if ((*first)->rank < doc->rank) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 0:"PRI64"]", type.c_str(), doc->docid);
            AC_WRITE_DOC_DEBUG_LOG(info, (*first), PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 1:"PRI64"]", type.c_str(), (*first)->docid);
            AC_WRITE_DOC_DEBUG_LOG(info, (*second), PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 2:"PRI64"]", type.c_str(), (*second)->docid);
            *third = *second;
            *second = *first;
            *first = doc;
        } else if ((*second)->rank < doc->rank) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 1:"PRI64"]", type.c_str(), doc->docid);
            AC_WRITE_DOC_DEBUG_LOG(info, (*second), PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 1 old :"PRI64"]", type.c_str(), (*second)->docid);
            *third = *second;
            *second = doc;
        } else {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster update %s 2:"PRI64"]", type.c_str(), doc->docid);
            *third = doc;
        }
    } else {
        uint64_t first_uid = GetDomain(*first);
        uint64_t second_uid = GetDomain(*second);
        uint64_t third_uid = GetDomain(*third);
        uint64_t uid = GetDomain(doc);
        if (first_uid && uid && second_uid && third_uid &&
            (uid == first_uid || uid == second_uid || uid == third_uid)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s miss for dup uid s:"PRI64","PRI64","PRI64","PRI64","PRI64"]",
                type.c_str(), doc->docid, uid, first_uid, second_uid, third_uid);
            return doc;
        }
        if ((*first)->rank < doc->rank) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 0:"PRI64"]", type.c_str(), doc->docid);
            AC_WRITE_DOC_DEBUG_LOG(info, (*first), PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 1:"PRI64"]", type.c_str(), (*first)->docid);
            AC_WRITE_DOC_DEBUG_LOG(info, (*second), PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 2:"PRI64"]", type.c_str(), (*second)->docid);
            SrmDocInfo* old_doc = *third;
            *third = *second;
            *second = *first;
            *first = doc;
            if (fwnum > data->fwnum_num) {
                data->fwnum_num = fwnum;
            }
            return old_doc;
        } else if ((*second)->rank < doc->rank) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 1:"PRI64"]", type.c_str(), doc->docid);
            AC_WRITE_DOC_DEBUG_LOG(info, (*second), PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 1 old :"PRI64"]", type.c_str(), (*second)->docid);
            *third = *second;
            *second = doc;
            if (fwnum > data->fwnum_num) {
                data->fwnum_num = fwnum;
            }
        } else if ((*third)->rank < doc->rank) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 2:"PRI64"]", type.c_str(), doc->docid);
            AC_WRITE_DOC_DEBUG_LOG(info, (*third), PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update %s 2 old :"PRI64"]", type.c_str(), (*third)->docid);
            *third = doc;
            if (fwnum > data->fwnum_num) {
                data->fwnum_num = fwnum;
            }
        } else {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [cluster update %s miss:"PRI64","PRI64","PRI64","PRI64"]",
                        type.c_str(), doc->docid, (*first)->docid, (*second)->docid,
                        (*third)->docid);
            return doc;
        }
    }
    return NULL;
}

static SrmDocInfo* UpdateClusterFwnumMax(QsInfo *info,
            SrmDocInfo *doc, const AcExtDataParser& attr,
            Ac_statistic &statistic_data,
            AcDupRepostClusterDataUpdate* data) {
    uint64_t fwnum = attr.get_fwnum();
    if (fwnum < data->fwnum_num) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster update fwnum_doc failed:"PRI64","PRI64"<="PRI64"]",
            doc->docid, fwnum, data->fwnum_num, doc->rank);
        return doc;
    }
    if (data->fwnum_doc == NULL) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster insert fwnum_doc NULL:"PRI64","PRI64">"PRI64"]",
            doc->docid, fwnum, data->fwnum_num);
        data->fwnum_doc = doc;
        data->fwnum_num = fwnum;
        return NULL;
    }
    if (fwnum > data->fwnum_num ||
        ((fwnum == data->fwnum_num) && (doc->rank >= data->fwnum_doc->rank))) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster update fwnum_doc replace:"PRI64","PRI64">"PRI64"]",
            doc->docid, fwnum, data->fwnum_num);
        AC_WRITE_DOC_DEBUG_LOG(info, data->fwnum_doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster update old fwnum_doc:"PRI64"]",
            data->fwnum_doc->docid);
        SrmDocInfo* old_doc = data->fwnum_doc;
        data->fwnum_doc = doc;
        data->fwnum_num = fwnum;
        return old_doc;
    }
    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
        " [cluster update fwnum_doc failed:"PRI64","PRI64"<="PRI64" or %u<%u]",
        doc->docid, fwnum, data->fwnum_num, doc->rank, data->fwnum_doc->rank);
    return doc;
}

static int DupClusterActInsertRepostUpdate(QsInfo *info,
            SrmDocInfo *doc, const AcExtDataParser& attr,
            int degree, bool root,
            bool already_insert_root,
            AcDupRepostClusterDataUpdate* data,
            Ac_statistic &statistic_data) {
    uint64_t dup = attr.get_dup();
    if (root) {
        dup = get_dup_by_mid(doc->docid);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [cluster fix dup:"PRI64", dup:"PRI64"]", doc->docid, dup);
    }
    data->count++;
    if (!already_insert_root && root) {
        if (data->root_doc) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [ERROR cluster multi dup:"PRI64", "PRI64"]", dup, doc->docid);
            data->root_cluster = 1;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [cluster insert root_users:"PRI64"]", doc->docid);
            data->all_users.push_back(doc);
        } else {
            data->root_doc = doc;
            ClusterDocid cluster_docid;
            cluster_docid.dup = dup;
            cluster_docid.act = 0;
            statistic_data.cluster_for_picsource_check.insert(make_pair(doc->docid, cluster_docid));
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [cluster set root_doc:"PRI64", data:"PRI64"]", doc->docid, data->uniq);
            if (doc->rank > data->newest_time) {
                data->newest_time = doc->rank;
            }
        }
        return 0;
    }
    // for repost
    //if (degree < DEGREE_9_GOOD || !is_white_list(doc) || check_invalid_timel(info, statistic_data, doc) ||
    if (degree < statistic_data.good_grade || !is_white_list(doc) || check_invalid_timel(info, statistic_data, doc) ||
        //degree == DEGREE_14_SOCIAL_AT) {
        degree == statistic_data.super_grade-1) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster insert low_docs:"PRI64",degree:%d,is_white:%d]",
            doc->docid, degree, is_white_list(doc));
        data->all_users.push_back(doc);
        return 0;
    }
    data->valid_count++;
    if (data->root_doc == NULL) {
        if (root) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster set root, dup:"PRI64", "PRI64"]", dup, doc->docid);
            data->root_doc = doc;
            ClusterDocid cluster_docid;
            cluster_docid.dup = dup;
            cluster_docid.act = 0;
            statistic_data.cluster_for_picsource_check.insert(make_pair(doc->docid, cluster_docid));
            return 0;
        } else {
            if (data->first_ok_doc == NULL) {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [cluster set first_ok_doc, dup:"PRI64", "PRI64"]", dup, doc->docid);
                data->first_ok_doc = doc;
                data->all_users.push_back(doc);
                ClusterDocid cluster_docid;
                cluster_docid.dup = dup;
                cluster_docid.act = 0;
                statistic_data.cluster_for_picsource_check.insert(make_pair(doc->docid, cluster_docid));
                //return 0;
            }
        }
    }
    if (data->first_ok_doc) {
        if (data->first_ok_doc->rank < doc->rank) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update first_ok_doc:"PRI64"]", doc->docid);
            AC_WRITE_DOC_DEBUG_LOG(info, data->first_ok_doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [cluster update is old first_ok_doc:"PRI64"]", data->first_ok_doc->docid);
            data->first_ok_doc = doc;
            ClusterDocid cluster_docid;
            cluster_docid.dup = dup;
            cluster_docid.act = 0;
            statistic_data.cluster_for_picsource_check.insert(make_pair(doc->docid, cluster_docid));
        }
    } else {
        data->first_ok_doc = doc;
        ClusterDocid cluster_docid;
        cluster_docid.dup = dup;
        cluster_docid.act = 0;
        statistic_data.cluster_for_picsource_check.insert(make_pair(doc->docid, cluster_docid));
    }
    data->all_users.push_back(doc);
    if (data->root_doc) {
        AC_WRITE_DOC_DEBUG_LOG(info, data->root_doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
            " [valid_count:%d,root:root:"PRI64",add:"PRI64"]",
            data->valid_count, data->root_doc->docid, doc->docid);
    } else if (data->first_ok_doc) {
        AC_WRITE_DOC_DEBUG_LOG(info, data->first_ok_doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
            " [valid_count:%d,repost:first:"PRI64",add:"PRI64"]",
            data->valid_count, data->first_ok_doc->docid, doc->docid);
    }
    SrmDocInfo* old_doc = UpdateClusterFwnumNewest(info, doc, statistic_data, data);
    if (old_doc == NULL) {
        return 0;
    }
    //degree = ((old_doc->filter_flag)>>20) & 0xF;
    degree = get_degree(old_doc);
    if (degree == DEGREE_200_SOCIAL) {
        AC_WRITE_DOC_DEBUG_LOG(info, old_doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster insert social_users:"PRI64"]", old_doc->docid);
        UpdateClusterBase(info, data, old_doc, attr, data->social_doc, "social");
    } else if (degree == DEGREE_100_VIP) {
        AC_WRITE_DOC_DEBUG_LOG(info, old_doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster insert vip_user:"PRI64"]", old_doc->docid);
        UpdateClusterBase(info, data, old_doc, attr, data->vip_doc, "vip");
    } else {
        AC_WRITE_DOC_DEBUG_LOG(info, old_doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [cluster check fwnum_doc:"PRI64", size:%lu]", old_doc->docid, data->all_users.size());
        /*
        if (old_doc->validfans >= statistic_data.cluster_validfans_num) {
            old_doc = UpdateClusterBase(info, data, old_doc, attr,
                                data->valid_user_doc, "valid_user_doc");
        }
        */
        AcExtDataParser fwnum_attr(old_doc);
        UpdateClusterFwnumMax(info, old_doc, fwnum_attr, statistic_data, data);
    }
    return 0;
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
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_BLACK_UID_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [bccaller black:"PRIU64"]", doc->docid);
        return 1;
    } 
    if (uid && *uid_dict && uid_dict->find(uid)) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_BLACK_UID_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [manual uid black:"PRIU64"]", doc->docid);
        return 2;
    }
    if (uid && *black_uids) {
        int ret = black_uids->Search(uid);
        if (ret != -1) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_BLACK_UID_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [uid black:"PRIU64"]", doc->docid);
            return 4;
        }
    }
    /*
    if (uid && *dup_black_uids) {
        int ret = dup_black_uids->Search(uid);
        if (ret != -1) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_BLACK_UID_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [dup uid black:"PRIU64"]", doc->docid);
            return 5;
        }
    }
    */
    if (uid && *plat_black) {
        int ret = plat_black->Search(uid);
        if (ret != -1) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_BLACK_UID_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [plat uid black:"PRIU64"]", doc->docid);
            return 6;
        }
    }
	return 0;
}

bool check_invalid_doc_mid(QsInfo *info, SrmDocInfo *doc,
            uint64_t uid, const AcExtDataParser& attr,
            Ac_statistic &statistic_data,
            bool insert_del) {
    // for mid dup
    if(statistic_data.mid_map.find(doc->docid) == statistic_data.mid_map.end()) {
        statistic_data.mid_map[doc->docid] = 1;
    } else {
        if (info->parameters.debug)
            set_ac_log_info(doc, 0, " delete , mid dup");
        statistic_data.black_num++;
        DelDocAddDupPrintLog(info, doc, attr, "mid dup", insert_del, statistic_data);
        return true;
    }
    return false;
}
bool check_invalid_doc_mid(QsInfo *info, SrmDocInfo *doc,
            uint64_t uid, const AcExtDataParser& attr,
            Ac_statistic &statistic_data,
            const auto_reference<ACLongSet>& mask_mids,
            bool insert_del) {
	// for mids mask
    if (*mask_mids && mask_mids->find(doc->docid)) {
        statistic_data.black_num++;
        DelDocAddDupPrintLog(info, doc, attr, "mask_mid", insert_del, statistic_data);
        return true;
    }
    // for mid dup
    if(statistic_data.mid_map.find(doc->docid) == statistic_data.mid_map.end()) {
        statistic_data.mid_map[doc->docid] = 1;
    } else {
        if (info->parameters.debug)
            set_ac_log_info(doc, 0, " delete , mid dup");
        statistic_data.black_num++;
        DelDocAddDupPrintLog(info, doc, attr, "mid dup", insert_del, statistic_data);
        return true;
    }
    return false;
}

bool check_manual_black_uids(QsInfo *info, SrmDocInfo *doc,const AcExtDataParser& attr, Ac_statistic & statistic_data, uint64_t uid, auto_reference<ACLongSet> & uid_dict, time_t delay_time_qi20, bool insert_del) {
    if(uid && *uid_dict && uid_dict->find(uid)) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_BLACK_UID_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                        " [manual uid black:"PRIU64"]", doc->docid);

        statistic_data.black_num++;
        DelDocAddDupPrintLog(info, doc, attr, "black", insert_del, statistic_data);
        return true;
    }
    // check qi 20
    if (doc->rank >= delay_time_qi20) {
        if (attr.get_qi() & (0x1 << 20)) {
            statistic_data.black_num++;
            DelDocAddDupPrintLog(info, doc, attr, "del qi20", insert_del, statistic_data);
            return true;
        }
    }
	return false;
}


bool check_invalid_doc(QsInfo *info, SrmDocInfo *doc,
            uint64_t uid, const AcExtDataParser& attr,
            bool check_sandbox, time_t delay_time, time_t delay_time_qi20,
            Ac_statistic &statistic_data,
            const auto_reference<ACLongSet>& mask_mids,
            const auto_reference<ACLongSet>& manual_black_uids,
            const auto_reference<DASearchDictSet>& black_uids,
            const auto_reference<DASearchDictSet>& plat_black,
            const auto_reference<DASearchDict>& suspect_uids,
            uint64_t source, const auto_reference<SourceIDDict>& black_dict,
            bool insert_del) {
    // for sandbox
    int sandbox_doc_result = ac_sandbox_doc(info, doc, uid, delay_time, suspect_uids);
    if (check_sandbox && sandbox_doc_result) {
        statistic_data.black_num++;
        char error_str[128];
        snprintf(error_str, 128, "sandbox:%d, %u > %u ?", sandbox_doc_result, doc->rank, delay_time);
        DelDocAddDupPrintLog(info, doc, attr, error_str, insert_del, statistic_data);
        return true;
    }
    // check new black source
	if (black_dict->find(source)) {
        statistic_data.black_num++;
        char error_str[128];
        snprintf(error_str, 128, "black_source:"PRIU64",s:"PRIU64"", doc->docid, source);
        DelDocAddDupPrintLog(info, doc, attr, error_str, insert_del, statistic_data);
        return true;
    }

    // check qi 20
    if (doc->rank >= delay_time_qi20) {
        if (attr.get_qi() & (0x1 << 20)) {
            statistic_data.black_num++;
            DelDocAddDupPrintLog(info, doc, attr, "del qi20", insert_del, statistic_data);
            return true;
        }
    }
    // for black
    int black = check_doc_is_black(info, doc, uid, attr, manual_black_uids,
                                black_uids, plat_black);
    if (black) {
        if (info->parameters.debug) {
            switch (black) {
                case 1:
                    set_ac_log_info(doc, 0, " delete in black list");
                    break;
                case 2:
                    set_ac_log_info(doc, 0, " delete in uid black list");
                    break;
                case 3:
                    set_ac_log_info(doc, 0, " delete in source black list");
                    break;
                case 4:
                    set_ac_log_info(doc, 0, " delete in DASearchDictSet list");
                    break;
                case 5:
                    set_ac_log_info(doc, 0, " delete in dup DASearchDictSet list");
                    break;
                case 6:
                    set_ac_log_info(doc, 0, " delete in dup plat_black list");
                    break;
                default:
                    break;
            }
        }
        statistic_data.black_num++;
        DelDocAddDupPrintLog(info, doc, attr, "black", insert_del, statistic_data);
        return true;
    }
    return false;
}

static bool update_repost_fake_cluster_struct(QsInfo *info,
            SrmDocInfo *doc, const AcExtDataParser& attr,
            Ac_statistic &statistic_data) {
    if (!statistic_data.is_cluster_repost) {
        return true;
    }
    if (!GET_FORWARD_FLAG(attr.get_digit_attr())) {
        return true;
    }
    if (doc->cluster_info.time) {
        return true;
    }
    uint64_t dup = attr.get_dup();
    if (dup) {
        __gnu_cxx::hash_set<uint64_t>::iterator it_del = statistic_data.already_del_mid.find(dup);
        if (it_del != statistic_data.already_del_mid.end()) {
            statistic_data.repost_root_mid_del_count++;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG_2, SHOW_DEBUG_NEWLINE,
                            " [:"PRI64", "PRI64",d:%d]",
                            doc->docid, dup, get_degree(doc));
            return false;
        }
        int degree = get_degree(doc);
		//if (degree < DEGREE_15_GOOD_PASS_FILTER) {
        if (degree < statistic_data.super_grade) {
            __gnu_cxx::hash_set<uint64_t>::iterator it_fake = statistic_data.already_fake_doc.find(dup);
            if (it_fake != statistic_data.already_fake_doc.end()) {
                statistic_data.repost_root_mid_del_count++;
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_FAKE_DOC, SHOW_DEBUG_NEWLINE,
                                " [fake_doc dedup:"PRI64", "PRI64",d:%d]",
                                doc->docid, dup, get_degree(doc));
                return false;
            }
        }
        statistic_data.already_fake_doc.insert(dup);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG_2, SHOW_DEBUG_NEWLINE,
                        " [fake_doc insert_dup:"PRI64", "PRI64",d:%d]",
                        doc->docid, dup, get_degree(doc));
    }
    doc->cluster_info.time = doc->rank;
    doc->cluster_info.mid[0] = doc->docid;
    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG_2, SHOW_DEBUG_NEWLINE,
            " [repost doc fake cluster:"PRI64", %u]", doc->docid, doc->cluster_info.time);
    doc->docid = 0;
    return true;
}

static int DupClusterActOneUpdate(QsInfo *info,
            SrmDocInfo *doc, const AcExtDataParser& attr,
            Ac_statistic &statistic_data) {
    if (doc == NULL || (doc->filter_flag & 0x1) == 1) {
        return 0;
    }
    int degree = get_degree(doc);
    uint64_t dup = attr.get_dup();
    bool root = true;
    if (GET_FORWARD_FLAG(attr.get_digit_attr())) {
        root = false;
    }
    if (root) {
        dup = get_dup_by_mid(doc->docid);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
            " [cluster:root,md5,"PRI64","PRIU64"]", doc->docid, dup);
    } else {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
            " [cluster:repost,md5,"PRI64","PRI64"]", doc->docid, dup);
    }
    if (!dup) {
        // for update repost cluster_struct
        if (!update_repost_fake_cluster_struct(info, doc, attr, statistic_data)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [!dup and fakedoc error:"PRI64"]", doc->docid);
        }
        return 0;
    }
    __gnu_cxx::hash_map<uint64_t, AcDupRepostClusterDataUpdate>::iterator it =
                statistic_data.cluster_data_update.find(dup);
    if (it != statistic_data.cluster_data_update.end()) {
        bool already_insert_root = false;
        if (it->second.root_doc) {
            already_insert_root = true;
        }
        if ((it->second).first_ok_doc) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [cluster update dup:"PRI64", "PRI64", first_ok_doc "PRI64", data:"PRI64"]",
                dup, doc->docid,
                (it->second).first_ok_doc->docid, it->second.uniq);
        }
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
            " [cluster update dup:"PRI64", "PRI64", data:"PRI64"]", dup, doc->docid, it->second.uniq);
        DupClusterActInsertRepostUpdate(info, doc, attr, degree,
                    root, already_insert_root, &(it->second), statistic_data);
        return 0;
    }
    // first add
    static uint64_t uniq = 0;
    ++uniq;
    AcDupRepostClusterDataUpdate data;
    data.all_users.clear();
    data.newest_time = doc->rank;
    data.uniq = uniq;
    data.first_ok_doc = NULL;
    data.count = 1;
    data.valid_count = 1;
    data.root_cluster = 0;
    data.keep_root_doc = 0;
    for (int i = 0; i < CLUSTER_SOCIAL_VIP_NUM; ++i) {
        data.vip_doc[i] = NULL;
        data.social_doc[i] = NULL;
        // data.valid_user_doc[i] = NULL;
    }
    data.fwnum_doc = NULL;
    data.fwnum_newest_doc = NULL;
    data.fwnum_num = 0;
    data.fwnum_num_skip_updatetime_mid = 0;
    data.fwnum_newest_rank = 0;
    // check root
    if (root) {
        data.root_doc = doc;
        ClusterDocid cluster_docid;
        cluster_docid.dup = dup;
        cluster_docid.act = 0;
        statistic_data.cluster_for_picsource_check.insert(make_pair(doc->docid, cluster_docid));
    } else {
        // for repost
        data.root_doc = NULL;
        data.valid_count = 0;
        DupClusterActInsertRepostUpdate(info, doc, attr, degree, root, false, &data, statistic_data);
    }
    statistic_data.cluster_data_update.insert(make_pair(dup, data));
    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
        " [cluster insert dup:"PRI64", "PRI64", data:"PRI64"]", dup, doc->docid, data.uniq);
    return 0;
}

int is_invalid_topN_left_doc(QsInfo *info, SrmDocInfo *doc,
            const AcExtDataParser& attr, Ac_statistic &ac_sta) {
    // check repost
    if (GET_FORWARD_FLAG(attr.get_digit_attr())) {
        return 1;
    }
    uint64_t qi = attr.get_qi();
    int granularity = get_granularity_qi(qi);
    if (granularity < 3) {
        return 2;
    }
    return 0;
}

int degree_is_del(QsInfo *info, SrmDocInfo *doc, int granularity) {
    // F_PARAM strategy
    int stat_f = getGranularity_docattr(doc);
    if (stat_f == 0) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " ["PRI64", stat_fd==0,"PRIU64"]", doc->docid, doc->docattr);
        return 1;
    }
    if (granularity < 1) {
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " ["PRI64", granularity:%d < 1]", doc->docid, granularity);
        return 2;
    }
    return 0;
}


static void ac_uid_dup_and_socialtime_limit(QsInfo *info, const AcRerankData& data,
            int doc_num, Ac_statistic& ac_sta) {
	int uid_social_page = int_from_conf(UID_SOCIAL_PAGE, 20);
	int uid_social_num = int_from_conf(UID_SOCIAL_NUM, 10);
	int uid_social_use_gray = int_from_conf(UID_SOCIAL_USE_GRAY, 0);
    if (uid_social_use_gray) {
        int uid_social_num_gray_uid = int_from_conf(UID_SOCIAL_NUM_GRAY_UID, 9);
        if ((info->parameters.cuid/10) % 10 == uid_social_num_gray_uid) {
            uid_social_num = int_from_conf(UID_SOCIAL_NUM_GRAY, 10);
        }
    }
    int uid_social_count = 0;
    int now_uid_top = 0, social_top = 0;
    /*
    bool vip_gray = true;
    int use_vip_gray = int_from_conf(USE_VIP_GRAY, 1);
    if (use_vip_gray) {
        int vip_gray_uid = int_from_conf(VIP_GRAY_UID, 9);
        if ((info->parameters.cuid/10) % 10 != vip_gray_uid) {
            vip_gray = false;
        }
    }
    */
    for (; ac_sta.uid_dup_actindex < (doc_num - ac_sta.too_few_limit_doc_num) &&
           now_uid_top < data.keep_top_n &&
           ac_sta.del_uid_dup_num < ac_sta.uid_social_num_buf;
         ac_sta.uid_dup_actindex++) {
        SrmDocInfo *doc = info->bcinfo.get_doc_at(ac_sta.uid_dup_actindex);
        if (doc == NULL) {
            continue;
        }
        // uid dup
        bool uid_dup = is_uid_dup(info, doc, ac_sta, false, now_uid_top);
        if (uid_dup) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG uid_dup :"PRI64", "PRI64","PRI64","PRI64":%d]",
                    doc->docid, doc->cluster_info.mid[0], doc->cluster_info.mid[1],
                    doc->cluster_info.mid[2], ac_sta.uid_dup_actindex);
            info->bcinfo.del_doc(doc);
            ac_sta.del_uid_dup_num++;
            continue;
        } else {
            ++now_uid_top;
        }
        // social limit
        if (social_top % uid_social_page == 0) {
            uid_social_count = 0;
        }
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [:"PRIU64",%d social_top:%d, uid_social_count:%d, uid_social_num:%d"
                    ", uid_social_page:%d, now_uid_top:%d]",
                    doc->docid, doc->filter_flag, social_top, uid_social_count, uid_social_num,
                    uid_social_page, now_uid_top);
        // check social and cluster max
        if (doc->has_social_head && doc->cluster_info.time) {
            uint32_t max_social_time = 0;
            if (SOCIAL_ACTION_HEAD_PERSON == doc->social_head_output) {
                max_social_time = doc->social_head.person[0].time;
            } else if (SOCIAL_ACTION_HEAD_COLONIAL == doc->social_head_output) {
                max_social_time = doc->social_head.colonialinfo.colonial_time;
            } else if (SOCIAL_ACTION_HEAD_FOLLOWS == doc->social_head_output) {
                max_social_time = doc->social_head.person[0].time;
            }
            if (max_social_time && doc->cluster_info.time > max_social_time) {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [social_uid:"PRIU64" s_time:%u < :cluster_time:%u pass social_check,%u]",
                        doc->docid, max_social_time, doc->cluster_info.time, doc->rank);
                ++social_top;
                continue;
            }
        }
        if (doc->has_social_head) {
            if (uid_social_count >= uid_social_num) {
                if (doc->cluster_info.time) {
                    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                                " [social_uid more:"PRIU64", cluster:%u, now:%u]",
                                doc->docid, doc->cluster_info.time, doc->rank);
                    doc->rank = doc->cluster_info.time;
                } else {
                    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                                " [social_uid more:"PRIU64", old:%u, now:%u]",
                                doc->docid, doc->time_old, doc->rank);
                    doc->rank = doc->time_old;
                }
                doc->social_head_output = SOCIAL_ACTION_HEAD_NONE;
                continue;
            }
            ++uid_social_count;
        }
        ++social_top;
    }
}

static bool CheckForwardClusterDel(QsInfo *info, const AcExtDataParser attr,
            Ac_statistic& ac_sta, SrmDocInfo *doc) {
    if (GET_FORWARD_FLAG(attr.get_digit_attr()) &&
        doc->rank >= ac_sta.top_cluster_time) {
        return true;
    }
    return false;
}

static bool UpdateCheckRecallData(QsInfo *info, Ac_statistic& ac_sta, SrmDocInfo *doc) {
    bool recall_ = false;
    if (ac_sta.recall_qi_low_doc > 0 && doc->rank > ac_sta.recall_time) {
        --ac_sta.recall_qi_low_doc;
        recall_ = true;
    }
    if (ac_sta.recall_qi_low_doc_week > 0 && doc->rank > ac_sta.recall_time_week) {
        --ac_sta.recall_qi_low_doc_week;
        recall_ = true;
    }
    if (ac_sta.recall_qi_low_doc_month > 0 && doc->rank > ac_sta.recall_time_month) {
        --ac_sta.recall_qi_low_doc_month;
        recall_ = true;
    }
    return recall_;
}

static void TopNRecallDup(QsInfo *info, const AcRerankData& data,
            int doc_num, int top_n_degree, Ac_statistic& ac_sta) {
    // input:
        // act DEGREE_1_FORWARD_TIME[cluster_set] DEGREE_4_DUP[dup] DEGREE_7_NOACT_DUP
        // DEGREE_9_GOOD DEGREE_11_CATAGORY_FILTER DEGREE_14_SOCIAL_AT DEGREE_13_SOCIAL DEGREE_100_VIP
    // output:
        // add DEGREE_10_RECALL_GOOD_DUP
    for (int i = 0; i < ac_sta.act_index && i < doc_num; ++i) {
        if (ac_sta.need_recall_dup_num <= 0) {
            break;
        }
        SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
        if (doc == NULL) {
            continue;
        }
        int degree = get_degree(doc);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [TopNRecallDup:%d, i:%d, "PRI64", d:%d:%d]",
                ac_sta.need_recall_dup_num, i, doc->docid, degree, doc->filter_flag);
        if (degree != DEGREE_4_DUP) {
            continue;
        }
        AcExtDataParser attr(doc);
        if (CheckForwardClusterDel(info, attr, ac_sta, doc)) {
            degree = DEGREE_1_FORWARD_TIME;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [TopNRecallDup CheckForward set FORWARD_TIME:"PRIU64",i:%d]",
                    doc->docid, i);
            continue;
        }
        if(ac_sta.check_pic_source &&
           (ac_sta.topN_top+ac_sta.act_keep_doc) < ac_sta.cluster_first_page_num_after_cluster) {
            ac_sta.qualified_num++;
            uint64_t digit_attr = attr.get_digit_attr();
            hot_cheat_check_addinfo(info, digit_attr,
                        (ac_sta.topN_top+ac_sta.act_keep_doc), ac_sta);
            if (CheckPicSource(info, doc, ac_sta, attr, digit_attr)) {
                ac_sta.degree_0++;
                DelDocAddDupPrintLog(info, doc, attr, "source_check_failed", false, ac_sta);
                continue;
            }
        }
        if (!update_repost_fake_cluster_struct(info, doc, attr, ac_sta)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [TopNRecallDup update_repost_fake set FORWARD_TIME:"PRIU64",i:%d]",
                    doc->docid, i);
            degree = DEGREE_1_FORWARD_TIME;
            continue;
        }
        //degree = DEGREE_10_RECALL_GOOD_DUP;
        degree = ac_sta.recall_good_grade;
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [RECALL_GOOD_DUP, d:%d :"PRI64", t:%u]",
                degree, doc->docid, doc->rank);
        ac_sta.act_keep_doc++;
        doc->filter_flag = 0;
        doc->filter_flag |= degree<<20;
        ac_sta.need_recall_dup_num--;
        ac_sta.recall_dup_num++;
    }
}

static void TopNActDegreeNoactAndDelDup(QsInfo *info, const AcRerankData& data,
            int doc_num, int top_n_degree, Ac_statistic& ac_sta) {
    // 1: recall DEGREE_4_DUP
        // 1: check 24 hour recall
        // 2: check all_result < 40 recall
    //TopNActDegreeNoactAndDelDupDup();
    // 2: recall DEGREE_7_NOACT_DUP
    // for 3600*24 recall
    // 3: del all_dup
    if (ac_sta.recall_qi_low_doc > 0 ||
        ac_sta.recall_qi_low_doc_week > 0 ||
        ac_sta.recall_qi_low_doc_month > 0) {
        // input:
            // act DEGREE_1_FORWARD_TIME[cluster_set] DEGREE_4_DUP[dup] DEGREE_7_NOACT_DUP
        // output:
            // del dup
            // 1: degree <= DEGREE_4_DUP: DEL
            // 2: degree == DEGREE_6_RECALL_LOW_DUP && UpdateCheckRecallData: KEEP, recall
            // 2: degree == DEGREE_5_LOW and rank < top_n_time: KEEP, normal
            // 3: degree == DEGREE_5_LOW and rank > top_n_time: for recall
        for (int i = 0; i < ac_sta.act_index && i < doc_num; ++i) {
            SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
            if (doc == NULL) {
                continue;
            }
            int degree = get_degree(doc);
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [TopNActDegreeNoactAndDelDup d:%d:%d :"PRI64", i:%d]",
                    degree, doc->filter_flag, doc->docid, i);
            if (degree >= top_n_degree) {
                // for top keep, so pass
                continue;
            }
            AcExtDataParser attr(doc);
            // for update repost cluster_struct
            if (!update_repost_fake_cluster_struct(info, doc, attr, ac_sta)) {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG recall_topN update_repost d:%d :"PRI64", t:%u,root_mid del, "PRI64"]",
                        degree, doc->docid, doc->rank, attr.get_dup());
                info->bcinfo.del_doc(doc);
                ac_sta.act_del_time++;
                ac_sta.update_repost++;
                continue;
            }
            if (degree == DEGREE_7_NOACT_DUP) {
                // for repost must > last_cluster_time
                if (CheckForwardClusterDel(info, attr, ac_sta, doc)) {
                    degree = DEGREE_1_FORWARD_TIME;
                } else {
                    degree = DEGREE_5_LOW;
                    doc->filter_flag = 0;
                    doc->filter_flag |= degree<<20;
                    int dup_result = 0;
                    if(is_dup_update_doc(info, doc, attr, ac_sta, true, dup_result)) {
                        if (!dup_result && ac_sta.need_recall_dup_num > 0) {
                            if (doc->rank >= ac_sta.top_n_time) {
                                if (UpdateCheckRecallData(info, ac_sta, doc)) {
                                    ac_sta.need_recall_dup_num--;
                                    ac_sta.recall_dup_num++;
                                    degree = DEGREE_6_RECALL_LOW_DUP;
                                } else {
                                    degree = DEGREE_4_DUP;
                                }
                            } else {
                                ac_sta.need_recall_dup_num--;
                                ac_sta.recall_dup_num++;
                                degree = DEGREE_6_RECALL_LOW_DUP;
                            }
                        } else {
                            degree = DEGREE_4_DUP;
                        }
                    } else {
                        degree = DEGREE_5_LOW;
                    }
                }
                doc->filter_flag = 0;
                doc->filter_flag |= degree<<20;
            }
            degree = get_degree(doc);
            // pass degree > 1 and < top_n_degree
            if (((degree == DEGREE_5_LOW && doc->rank < ac_sta.top_n_time) ||
                (degree == DEGREE_6_RECALL_LOW_DUP))) {
                if (degree == DEGREE_6_RECALL_LOW_DUP) {
                    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                            " [RECALL_LOW_DUP, d:%d :"PRI64", t:%u]", degree, doc->docid, doc->rank);
                }
                if(ac_sta.check_pic_source &&
                   (ac_sta.topN_top+ac_sta.act_keep_doc) < ac_sta.cluster_first_page_num_after_cluster) {
                    ac_sta.qualified_num++;
                    uint64_t digit_attr = attr.get_digit_attr();
                    hot_cheat_check_addinfo(info, digit_attr,
                                (ac_sta.topN_top+ac_sta.act_keep_doc), ac_sta);
                    if (CheckPicSource(info, doc, ac_sta, attr, digit_attr)) {
                        ac_sta.degree_0++;
                        DelDocAddDupPrintLog(info, doc, attr, "source_check_failed", false, ac_sta);
                        ac_sta.act_del_time++;
                        continue;
                    }
                }
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [top_n keep d:%d<7 :"PRI64", t:%u:"PRIU64"]",
                        degree, doc->docid, doc->rank, ac_sta.top_n_time);
                ac_sta.act_keep_doc++;
            } else {
                if (degree <= DEGREE_4_DUP) {
                    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                            " [DEL_LOG recall_topN dup__del_time>=top_n d:%d<=1 :"PRI64", t:%u]",
                            degree, doc->docid, doc->rank);
                    info->bcinfo.del_doc(doc);
                    ac_sta.act_del_time++;
                }
            }
        }
        // recall degree=DEGREE_5_LOW
        for (int i = 0; i < ac_sta.act_index && i < doc_num; ++i) {
            SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
            if (doc == NULL) {
                continue;
            }
            int degree = get_degree(doc);
            if (degree != DEGREE_5_LOW) {
                // top_n_degree || degree <= DEGREE_4_DUP) {
                continue;
            }
            if (doc->rank < ac_sta.top_n_time) {
                continue;
            }
            AcExtDataParser attr(doc);
            if(ac_sta.check_pic_source &&
               (ac_sta.topN_top+ac_sta.act_keep_doc) < ac_sta.cluster_first_page_num_after_cluster) {
                ac_sta.qualified_num++;
                uint64_t digit_attr = attr.get_digit_attr();
                hot_cheat_check_addinfo(info, digit_attr,
                            (ac_sta.topN_top+ac_sta.act_keep_doc), ac_sta);
                if (CheckPicSource(info, doc, ac_sta, attr, digit_attr)) {
                    ac_sta.degree_0++;
                    DelDocAddDupPrintLog(info, doc, attr, "source_check_failed", false, ac_sta);
                    ac_sta.act_del_time++;
                    continue;
                }
            }
            // for update repost cluster_struct
            if (!update_repost_fake_cluster_struct(info, doc, attr, ac_sta)) {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG recall_topN update_repost degree2 d:%d :"PRI64", t:%u,root_mid del, "PRI64"]",
                        degree, doc->docid, doc->rank, attr.get_dup());
                info->bcinfo.del_doc(doc);
                ac_sta.act_del_time++;
                ac_sta.update_repost++;
                continue;
            }
            if (UpdateCheckRecallData(info, ac_sta, doc)) {
                ac_sta.recall_doc_num++;
                ac_sta.act_keep_doc++;
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [<1000 recall_doc_d keep:"PRI64", :%d]", doc->docid, i);
            } else {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG recall_topN1 del_time>=top_n d:%d :"PRI64", t:%u:"PRIU64", "
                        "recall_t:%u:%u:%u]",
                        degree, doc->docid, doc->rank, ac_sta.top_n_time,
                        ac_sta.recall_time, ac_sta.recall_time_week,
                        ac_sta.recall_time_month);
                info->bcinfo.del_doc(doc);
                ac_sta.act_del_time++;
            }
        }
    } else {
        for (int i = 0; i < ac_sta.act_index && i < doc_num; ++i) {
            SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
            if (doc == NULL) {
                continue;
            }
            int degree = get_degree(doc);
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [TopNActDegreeNoactAndDelDup d:%d:%d :"PRI64", i:%d]",
                    degree, doc->filter_flag, doc->docid, i);
            if (degree >= top_n_degree) {
                continue;
            }
            AcExtDataParser attr(doc);
            // for update repost cluster_struct
            if (!update_repost_fake_cluster_struct(info, doc, attr, ac_sta)) {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG recall_topN update_repost 3 d:%d :"PRI64", t:%u,root_mid del, "PRI64"]",
                        degree, doc->docid, doc->rank, attr.get_dup());
                info->bcinfo.del_doc(doc);
                ac_sta.act_del_time++;
                ac_sta.update_repost++;
                continue;
            }
            // no degree is DEGREE_7_NOACT_DUP
            // act is_dup
            if (degree == DEGREE_7_NOACT_DUP) {
                if (CheckForwardClusterDel(info, attr, ac_sta, doc)) {
                    degree = DEGREE_1_FORWARD_TIME;
                } else {
                    degree = DEGREE_5_LOW;
                    doc->filter_flag = 0;
                    doc->filter_flag |= degree<<20;
                    int dup_result = 0;
                    if(is_dup_update_doc(info, doc, attr, ac_sta, true, dup_result)) {
                        if (!dup_result && ac_sta.need_recall_dup_num > 0) {
                            ac_sta.need_recall_dup_num--;
                            ac_sta.recall_dup_num++;
                            degree = DEGREE_6_RECALL_LOW_DUP;
                            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                                    " [RECALL_LOW_DUP 2, d:%d :"PRI64", t:%u]",
                                    degree, doc->docid, doc->rank);
                        } else {
                            degree = DEGREE_4_DUP;
                        }
                    } else {
                        degree = DEGREE_5_LOW;
                    }
                }
                doc->filter_flag = 0;
                doc->filter_flag |= degree<<20;
            }
            if ((degree == DEGREE_5_LOW || degree == DEGREE_6_RECALL_LOW_DUP) &&
                (doc->rank < ac_sta.top_n_time)) {
                if(ac_sta.check_pic_source &&
                   (ac_sta.topN_top+ac_sta.act_keep_doc) < ac_sta.cluster_first_page_num_after_cluster) {
                    ac_sta.qualified_num++;
                    uint64_t digit_attr = attr.get_digit_attr();
                    hot_cheat_check_addinfo(info, digit_attr,
                                (ac_sta.topN_top+ac_sta.act_keep_doc), ac_sta);
                    if (CheckPicSource(info, doc, ac_sta, attr, digit_attr)) {
                        ac_sta.degree_0++;
                        DelDocAddDupPrintLog(info, doc, attr, "source_check_failed", false, ac_sta);
                        ac_sta.act_del_time++;
                        continue;
                    }
                }
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [top_n keep d:%d<7 :"PRI64", t:%u:"PRIU64"]",
                        degree, doc->docid, doc->rank, ac_sta.top_n_time);
                ac_sta.act_keep_doc++;
            } else {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG recall_topN del_time>=top_n d:%d :"PRI64", t:%u,"PRIU64"]",
                        degree, doc->docid, doc->rank, ac_sta.top_n_time);
                info->bcinfo.del_doc(doc);
                ac_sta.act_del_time++;
            }
        }
    }
}

static bool get_strategy_inten(QsInfo *info, Ac_statistic &ac_data) {
    AC_WRITE_LOG(info, " [get_strategy_inten]");
    int doc_num = info->bcinfo.get_doc_num();
    int social_threshold = INT_MIN;
    if (!(info->strategy_info.ac_social_threshold.empty()))
    {
	social_threshold = atoi(info->strategy_info.ac_social_threshold.c_str());
        AC_WRITE_LOG(info, " [strategy.threshold:%d]", social_threshold );
    }
    social_threshold = social_threshold == INT_MIN ? ac_data.inten_ploy_docnum_min : social_threshold ;

    // for category filter
    if ((info->intention & INTENTION_MOVIE)) 
    {
		int movie_num = 0;
    	for (int i = 0; i < doc_num; ++i) 
		{
			SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
			if (doc == NULL) 
			{
				continue;
			}
			//match or part match movie flag
			if (doc->docattr & (0x1ll << 46) || doc->docattr & (0x1ll << 47))
			{
				movie_num++;
			}
		}
		if( movie_num >= social_threshold )
		{
			ac_data.need_category_filter = 1;
		}
		else
		{
			ac_data.need_category_filter = -1;
		}
		AC_WRITE_LOG(info, "[need category filter:%d, movie num:%d]",ac_data.need_category_filter, movie_num);
	}

    if (info->strategy_info.ac_social_fwnum.empty() && info->strategy_info.ac_social_likenum.empty()) {
        AC_WRITE_LOG(info, " [fwnum and liknum all empty]");
        return false;
    }
    if (!info->strategy_info.ac_social_fwnum.empty()) {
        std::string val = info->strategy_info.ac_social_fwnum;
        AC_WRITE_LOG(info, " [strategy.fwnum:%s]", val.c_str());
        std::size_t found = val.find(","), start = 0;
        int index = 0;
        while (found != std::string::npos) {
	    std::string value = val.substr(start, found);
            if (index >= MAX_INTENTION_STRATEGY_NUM) {
                break;
            }
            ac_data.inten_ploy_fwnum[index] = atoi(value.c_str());
            AC_WRITE_LOG(info, " [i:%d,fw:%d]", index, ac_data.inten_ploy_fwnum[index]);
            found++;
            start = found;
            index++;
            found = val.find(",", found);
        }
        if (start <= val.size() && index < MAX_INTENTION_STRATEGY_NUM) {
            string value = val.substr(start, val.size());
            int num = atoi(value.c_str());
            if (num > 0) {
                ac_data.inten_ploy_fwnum[index] = num;
                AC_WRITE_LOG(info, " [i:%d,fw:%d]", index, ac_data.inten_ploy_fwnum[index]);
            }
        }
    }
    if (!info->strategy_info.ac_social_likenum.empty()) {
        std::string val = info->strategy_info.ac_social_likenum;
        AC_WRITE_LOG(info, " [strategy.likenum:%s]", val.c_str());
        std::size_t found = val.find(","), start = 0;
        int index = 0;
        while (found != std::string::npos) {
            string value = val.substr(start, found);
            if (index >= MAX_INTENTION_STRATEGY_NUM) {
                break;
            }
            ac_data.inten_ploy_likenum[index] = atoi(value.c_str());
            AC_WRITE_LOG(info, " [i:%d,lk:%d]", index, ac_data.inten_ploy_likenum[index]);
            found++;
            start = found;
            index++;
            found = val.find(",", found);
        }
        if (start <= val.size() && index < MAX_INTENTION_STRATEGY_NUM) {
            string value = val.substr(start, val.size());
            int num = atoi(value.c_str());
            if (num > 0) {
                ac_data.inten_ploy_likenum[index] = num;
                AC_WRITE_LOG(info, " [i:%d,lk:%d]", index, ac_data.inten_ploy_likenum[index]);
            }
        }
    }
    int left_num[MAX_INTENTION_STRATEGY_NUM] = {0};
    for (int i = 0; i < doc_num; ++i) {
        SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
        if (doc == NULL) {
            continue;
        }
        AcExtDataParser attr(doc);
        for (int j = 0; j < MAX_INTENTION_STRATEGY_NUM;j++) {
            if (ac_data.inten_ploy_fwnum[j] <= 0) {
                continue;
            }
            if (attr.get_fwnum() >= ac_data.inten_ploy_fwnum[j] ||
                attr.get_likenum() >= ac_data.inten_ploy_likenum[j]) {
                left_num[j] += 1;
            }
        }
    }
    AC_WRITE_LOG(info, " [inten_ploy_docnum_min:%d]", ac_data.inten_ploy_docnum_min);
    for (int j = MAX_INTENTION_STRATEGY_NUM-1; j >= 0;j--) {
        AC_WRITE_LOG(info, " [j:%d,num:%d]", j, left_num[j]);
        if (left_num[j] >= social_threshold) {
            ac_data.inten_ploy_fwnum_filter = ac_data.inten_ploy_fwnum[j];
            ac_data.inten_ploy_likenum_filter = ac_data.inten_ploy_likenum[j];
            ac_data.need_inten_ploy_filter = 1;
            AC_WRITE_LOG(info, " [filter:{fw:%d,like:%d}]",
                        ac_data.inten_ploy_fwnum_filter, ac_data.inten_ploy_likenum_filter);
            return true;
        }
	}

	ac_data.need_inten_ploy_filter = -1;
	AC_WRITE_LOG(info, " [filter:{fw:%d,like:%d}]",
	            ac_data.inten_ploy_fwnum_filter, ac_data.inten_ploy_likenum_filter);
	return false;
}
/*
static bool get_strategy(QsInfo *info, Ac_statistic &ac_data) {
    AC_WRITE_LOG(info, " [get_strategy]");
	Json::Reader reader;
	Json::Value  root;
	if (info->json_intention == NULL || info->json_intention->text == NULL ||
        !reader.parse(info->json_intention->text, root)) {
        AC_WRITE_LOG(info, " [json_intention NULL]");
        if (info->json_intention && info->json_intention->text) {
            AC_WRITE_LOG(info, " [json.parse error:%s]", info->json_intention->text);
        }
        return false;
	}
    // "ac_social_strategy":{"fwnum":"1,2,4","likenum":"1,5,10"}
    if (root.isMember("strategy_info") && root["strategy_info"].isMember("ac_social_strategy")) {
        if (root["strategy_info"]["ac_social_strategy"].isMember("fwnum") &&
            root["strategy_info"]["ac_social_strategy"]["fwnum"].type() == Json::stringValue) {
            std::string val = root["strategy_info"]["ac_social_strategy"]["fwnum"].asString();
            AC_WRITE_LOG(info, " [strategy.fwnum:%s]", val.c_str());
            std::size_t found = val.find(","), start = 0;
            int index = 0;
            while (found != std::string::npos) {
                string value = val.substr(start, found);
                if (index >= MAX_INTENTION_STRATEGY_NUM) {
                    break;
                }
                ac_data.inten_ploy_fwnum[index] = atoi(value.c_str());
                found++;
                start = found;
                AC_WRITE_LOG(info, " [start:%d,all:%d]", start, val.size());
                index++;
                found = val.find(",", found);
            }
            if (start <= val.size() && index < MAX_INTENTION_STRATEGY_NUM) {
                string value = val.substr(start, val.size());
                ac_data.inten_ploy_fwnum[index] = atoi(value.c_str());
            }
        }
        if (root["strategy_info"]["ac_social_strategy"].isMember("likenum") &&
            root["strategy_info"]["ac_social_strategy"]["likenum"].type() == Json::stringValue) {
            std::string val = root["strategy_info"]["ac_social_strategy"]["likenum"].asString();
            AC_WRITE_LOG(info, " [strategy.likenum:%s]", val.c_str());
            std::size_t found = val.find(","), start = 0;
            int index = 0;
            while (found != std::string::npos) {
                string value = val.substr(start, found);
                if (index >= MAX_INTENTION_STRATEGY_NUM) {
                    break;
                }
                ac_data.inten_ploy_likenum[index] = atoi(value.c_str());
                found++;
                start = found;
                index++;
                found = val.find(",", found);
            }
            if (start <= val.size() && index < MAX_INTENTION_STRATEGY_NUM) {
                string value = val.substr(start, val.size());
                ac_data.inten_ploy_likenum[index] = atoi(value.c_str());
            }
        }
        int doc_num = info->bcinfo.get_doc_num();
        int left_num[MAX_INTENTION_STRATEGY_NUM] = {0};
        for (int i = 0; i < doc_num; ++i) {
            SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
            if (doc == NULL) {
                continue;
            }
            AcExtDataParser attr(doc);
            for (int j = 0; j < MAX_INTENTION_STRATEGY_NUM;j++) {
                if (attr.get_fwnum() >= ac_data.inten_ploy_fwnum[j] ||
                    attr.get_likenum() >= ac_data.inten_ploy_likenum[j]) {
                    left_num[j] += 1;
                }
            }
        }
        for (int j = MAX_INTENTION_STRATEGY_NUM-1; j >= 0;j--) {
            if (left_num[j] >= ac_data.inten_ploy_docnum_min) {
                ac_data.inten_ploy_fwnum_filter = ac_data.inten_ploy_fwnum[j];
                ac_data.inten_ploy_likenum_filter= ac_data.inten_ploy_likenum[j];
                ac_data.need_inten_ploy_filter = true;
                return true;
            }
        }
    }
    return false;
}
*/

static inline bool str2json(const std::string & str, Json::Value & root)
{
    try 
    {   
        Json::Reader reader;

        bool rt = reader.parse(str, root);
        if (!rt)
        {   
            return false;
        }   
    }   
    catch(...)
    {   
        return false;
    }   

    return true;
}

static void get_json_member(QsInfo *info,Json::Value& tmp,vector<pair<string,string> >& res)
{
	Json::Value::Members members = tmp.getMemberNames(); 
	typedef Json::Value::Members::iterator mit;
	for(mit it = members.begin(); it != members.end(); ++it) 
	{
		string first = *it;
		string second = tmp[first].asString();
		res.push_back(make_pair(first,second));
		//AC_WRITE_LOG(info, "[json: frist:%s, second:%s]",first.c_str(),second.c_str());
	}

}

static bool get_da_grades(QsInfo *info, Ac_statistic& ac_sta)
{
	Json::Reader reader;
	Json::Value root;
	
	string grades;
	string good_grade;
	string super_grade;
	string low_grade;
	string del_grade;
	string threshold;

	if (info->json_intention == NULL || info->json_intention->text == NULL ||!reader.parse(info->json_intention->text, root))
    {    
        AC_WRITE_LOG(info, " [json_intention NULL]");
        if (info->json_intention && info->json_intention->text)
        {    
            AC_WRITE_LOG(info, " [json.parse error:%s]", info->json_intention->text);
        }    
        return false;
    }

	if (!root.isMember("strategy_info")) return false;
	if (!root["strategy_info"].isMember("ac_grade_info")) return false;
	
	//if (root.isMember("ac_grade_info"))
    //{    
		if(root["strategy_info"]["ac_grade_info"].isMember("grades"))
		{
			get_json_member(info,root["strategy_info"]["ac_grade_info"]["grades"],ac_sta.conditions);
		}
		else return false;
		
		if(root["strategy_info"]["ac_grade_info"].isMember("good_grade"))
		{
			good_grade = root["strategy_info"]["ac_grade_info"]["good_grade"].asString();
			if(!good_grade.empty())
				ac_sta.good_grade = atoi(good_grade.c_str());
		}
        else return false;
		
		if(root["strategy_info"]["ac_grade_info"].isMember("super_grade"))
		{
			super_grade = root["strategy_info"]["ac_grade_info"]["super_grade"].asString();
			if(!super_grade.empty())
				ac_sta.super_grade = atoi(super_grade.c_str());
		}
        else return false;
		
		if(root["strategy_info"]["ac_grade_info"].isMember("low_grade"))
		{
			low_grade = root["strategy_info"]["ac_grade_info"]["low_grade"].asString();
			if(!low_grade.empty())
				ac_sta.low_grade = atoi(low_grade.c_str());
		}
        else return false;
		
		if(root["strategy_info"]["ac_grade_info"].isMember("del_grade"))
		{
			low_grade = root["strategy_info"]["ac_grade_info"]["del_grade"].asString();
			if(!low_grade.empty())
				ac_sta.low_grade = atoi(low_grade.c_str());
		}
        else return false;

		if(root["strategy_info"]["ac_grade_info"].isMember("threshold"))
		{
			get_json_member(info, root["strategy_info"]["ac_grade_info"]["threshold"],ac_sta.grade_threshold);
		}
		
    //} 
	//else return false;
}
//create calc exprs from da exprs or default exprs
static bool create_calc_exprs(QsInfo* info,Ac_statistic& ac_sta)
{
    AC_WRITE_LOG(info, " [auto grade generating...]");
	int is_da_control = int_from_conf(IS_DA_CONTROL,0);

	//如果DA没有发送参数或参数解析失败，则走默认参数
	if(!is_da_control || !get_da_grades(info, ac_sta))
	{
		Json::Value head_body; 
		Json::Value body;
		str2json(ac_sta.default_exprs,head_body);
		body = head_body["info"];
		get_json_member(info,body, ac_sta.conditions);

		str2json(ac_sta.default_thresholds, head_body);
		body = head_body["threshold"];
		get_json_member(info,body ,ac_sta.grade_threshold);
	}

	//grades info:
	// grade     expr
	// 200:    [social]==1
	// 100:    [good_uid]>0
	//  14:    [social]==2
	//  13:    [intention]&0x04 && [fwnum]>=1 && [likenum]>=10
	//  12:    [valid_fwnum]==1
	//  11:    ([intention]&0x04)&& (([docattr]&(0x1<<46))||([docattr]&(0x1 << 47)))
    for( int i = 0 ; i < ac_sta.conditions.size(); ++ i)
    {
        char* condition = (char*) ac_sta.conditions[i].second.c_str();
		int degree = atoi(ac_sta.conditions[i].first.c_str());
        int ok = expr_create(info, condition, strlen(condition), ac_sta.exprs[degree]); // ac_sta.exprs要改成map： unordered_map<int, expr>;
        if( !ok ) 
		{
			ac_sta.expr_init_ok = false;
			return false;
		}
		//AC_WRITE_LOG(info, "[degree:%d,expr:%s]",degree, condition);
    }

    AC_WRITE_LOG(info, " [create expr success?:%d]",  ac_sta.expr_init_ok);
    return true;
}

static bool get_auto_grades(QsInfo* info,Ac_statistic& ac_sta)
{
	//如果表达式计算失败则走老逻辑
	//默认参数的存在使得这种情况基本不会出现
	bool ok = create_calc_exprs(info, ac_sta);
	//if configure said that we should follow the old version then return false or return true
	return ok;
}

static void get_grade_threshold(QsInfo *info, Ac_statistic& ac_sta) // expression and threshold
{
    int doc_num = info->bcinfo.get_doc_num();
    int qualified_num = 0;
	//threshold
	//grade 13: 100
	//grade 11: 100
    for ( int i = 0; i < ac_sta.grade_threshold.size(); ++i )// if grade_threshold.empty() do nothing !
    {
        qualified_num = 0;
		int degree = atoi(ac_sta.grade_threshold[i].first.c_str());
		int threshold = atoi(ac_sta.grade_threshold[i].second.c_str());
        for (int j = 0 ; j < doc_num; ++j )
        {
            SrmDocInfo *doc = info->bcinfo.get_doc_at(j);
            if( doc == NULL ) continue;

            if( expr_calc(info, ac_sta.exprs[degree], doc) )
            {
                qualified_num++;
            }
			if ( qualified_num >= threshold ) break;
        }
		ac_sta.grade_threshold_filter[degree] = (qualified_num >= threshold ? 1 : -1); 
		AC_WRITE_LOG(info, " [degree:%d, qualified_num:%d, filter:%d]",degree,qualified_num, ac_sta.grade_threshold_filter[degree]);
		
    }
}

static int get_degree_by_exprs(QsInfo *info, Ac_statistic& ac_sta, SrmDocInfo * doc, bool check_all) // get degree by expr and threshold
{
	int degree = 0;
	//int no_filter;
	int check_num = 100;
	if(!check_all)
	{
		//check_num = int_from_conf(CHECK_NUM,2);
		check_num = 2;
	}

    for ( expr_it it = ac_sta.exprs.begin(); it != ac_sta.exprs.end() && check_num--; ++it )
    {
		//检查某个doc的档次
		degree = it->first;
		//no_filter = ac_sta.grade_threshold_filter[degree]; 
		// 0/1表示不需要过滤或满足条件不用过滤，-1表示不满足阈值要过滤掉
		//no_filter = true;
		/*
		if(ac_sta.grade_threshold_filter.find(degree) != ac_sta.grade_threshold_filter.end())
		{
			if(ac_sta.grade_threshold_filter[degree] == 1) 
				no_filter = true;
			else
				no_filter = false;
		}
		*/

        if((ac_sta.need_inten_ploy_filter>=0 || ac_sta.need_category_filter>=0) && expr_calc(info, it->second, doc) )
        {
    		return degree;
        }
    }
	return DEGREE_7_NOACT_DUP ;
}

static bool ac_auto_reranking_base(QsInfo *info, const AcRerankData& data, Ac_statistic& ac_sta) 
{
	if (info == NULL) {
		return false;
	}
	QsInfoBC *bcinfo = &info->bcinfo;
	int ranktype = RankType(info);
	int doc_num = bcinfo->get_doc_num();
    int orig_num = doc_num;
	ac_sta.original_num = doc_num;
	ac_sta.total_num = doc_num;
    // F_PARAM get dup flag
    int resort_flag = 1;
    int dup_flag = 1;
    ac_set_flag(info, &resort_flag, &dup_flag);
    ac_sta.dup_flag = dup_flag;
    int result_keep_num = int_from_conf(RESULT_KEEP_NUM, 1000);
    ac_sta.bc_input_num = doc_num;
    if (ac_sta.dup_flag && doc_num <= ac_sta.min_num_skip_dup) {
        ac_sta.dup_flag = 0;
    }

	AC_WRITE_LOG(info, " [auto ranking]");
    // F_PARAM strategy set socialtime
    if (info->parameters.getXsort() == std::string("social") &&
        info->parameters.getUs() == 1 &&
        (info->parameters.gray == 1 || info->parameters.socialtime)) {
        ac_sta.is_socialtime = true;
    }
    std::string key;
    if (info->params.key) {
        key = info->params.key;
    } else {
        key = info->params.query;
    }
    // F_PARAM strategy set cluster_repost
    if (info->parameters.getXsort() == std::string("social") &&
        info->parameters.getUs() == 1 &&
        info->parameters.cluster_repost == 1) {
        ac_sta.is_cluster_repost = true;
        ac_sta.min_repost_num = 2;
    }
	// int top_n_degree = int_from_conf(TOP_N_DEGREE, DEGREE_9_GOOD);
    //AC_WRITE_LOG(info, " [top_n_degree:%d]", DEGREE_9_GOOD);
    AC_WRITE_LOG(info, " [top_n_degree:%d]", ac_sta.good_grade);
    bool check_sandbox = true;
    /*
	auto_reference<ACStringSet> sandbox_query("sandbox_query");
	if (*sandbox_query == NULL) {
        check_sandbox = false;
	} else {
        if (!(info->intention & INTENTION_STAR) && !sandbox_query->test(key)) {
            check_sandbox = false;
        }
    }
    */
    int bc_check_black = int_from_conf(BLACK_LIST_CHECK_SWITCH, 1);
    //{TODO:del
    auto_reference<SourceIDDict> black_dict("source_id.dict");
	auto_reference<ACLongSet> mask_mids("mask_mids");
	auto_reference<ACLongSet> manual_black_uids("uids_black_manual.txt");
	auto_reference<DASearchDictSet> black_uids("uids_black");
	auto_reference<DASearchDictSet> plat_black("plat_black");
	auto_reference<DASearchDict> suspect_uids("suspect_uids");

    //}
    time_t time_now = ac_sta.curtime;
	time_t delay_time = time_now - int_from_conf(SUSPECT_DELAY_TIME, 172800);
	time_t delay_time_qi20 = time_now - int_from_conf(QI20_DELAY_TIME, 604800);

	char* source_black = bitdup_get(g_sort.bitmap, g_sort.source_black_type);
	//auto_reference<DASearchDictSet> good_uids("good_uids");
    auto_reference<SourceIDDict> dict("old_source_id.dict");
	auto_reference<DASearchDict> uids_validfans_week("uids_validfans_week");
	auto_reference<DASearchDict> uids_validfans_hour("uids_validfans_hour");
	//auto_reference<DASearchDictSet> dup_black_uids("dup_uids_black");
	//auto_reference<ACLongSet> sandbox_uids("sandbox_uids");
    // for old imgcheat
    //auto_reference<DASearchDictSet> uid_source_mul_dict("uid_source_mul_dict");
    //auto_reference<DASearchDictSet> uid_source_suspect_dict("uid_source_suspect_dict");
	//auto_reference<ACLongSet> uid_source_black_source("uid_source_black_source");
	//auto_reference<ACLongSet> uid_source_white_source("uid_source_white_source.txt");
	auto_reference<ACStringSet> img_check_query("hot_query.txt");
	/*
    if (!(*good_uids)) {
        AC_WRITE_LOG(info, " [good_uids load error]");
    }
	*/
    int inten_user_type = intention_user_type(info->intention);
	QsInfoSocial *social = &info->socialinfo;
    // F_PARAM strategy sort_time
	//get_grade_threshold(info, ac_sta);

    if (!data.degrade) {
        int sort_time_num = doc_num;
        if (sort_time_num > data.max_act_rerank_doc) {
            sort_time_num = data.max_act_rerank_doc;
        }
        if (ac_sta.is_socialtime) {
            for (int i = 0; i < sort_time_num; ++i) {
                SrmDocInfo *doc = bcinfo->get_doc_at(i);
                if (doc == NULL) {
                    continue;
                }
                UpdateFamousConcernInfo(info, doc, inten_user_type);
                UpdataTimeOne(info, doc, inten_user_type, time_now);
            }
        }
        bcinfo->sort(0, sort_time_num, my_time_cmp);
        // get_strategy(info, ac_sta);
        get_strategy_inten(info, ac_sta);
		if ( ac_sta.need_inten_ploy_filter==1 || ac_sta.need_category_filter==1 )
		{
			AC_WRITE_LOG(info, " [film result >= 100, no recall qi low doc]");
			ac_sta.recall_qi_low_doc = 0;
			ac_sta.recall_qi_low_doc_week = 0;
			ac_sta.recall_qi_low_doc_month = 0;
		}
    }
    /* <abtest_social_switch>0</abtest_social_switch>
     * <abtest_social_0>9</abtest_social_0>
     * <abtest_csort_del>0</abtest_csort_del>
    if (info->parameters.abtest) {
        if ((info->intention & INTENTION_HOTSEARCH_WORD) ||
            (info->intention & INTENTION_PLACE) ||
            *img_check_query && img_check_query->test(key)) {
            ac_sta.check_pic_source = true;
            ac_sta.qualified_num = 0;
            ac_sta.original_pic_num = 0;
            ac_sta.orig_video_num = 0;
            ac_sta.all_pic_num = 0;
            ac_sta.all_video_num = 0;
            ac_sta.del_pic_num = 0;
            ac_sta.del_other_num = 0;
            ac_sta.del_video_num = 0;
        }
    } else {
    }
    */
    if (*img_check_query && img_check_query->test(key)) {
        if (ac_sta.check_pic_source_dict) {
            ac_sta.check_pic_source = true;
            ac_sta.check_pic_source_dict_query = true;
            ac_sta.qualified_num = 0;
            ac_sta.original_pic_num = 0;
            ac_sta.orig_video_num = 0;
            ac_sta.all_pic_num = 0;
            ac_sta.all_video_num = 0;
            ac_sta.del_pic_num = 0;
            ac_sta.del_other_num = 0;
            ac_sta.del_video_num = 0;
        }
    } else if (ac_sta.check_pic_source_normal) {
        ac_sta.check_pic_source = true;
    }
    int valid_forward_fwnum = int_from_conf(VALID_FORWARD_FWNUM, 1);
    // last 40, don't act cluster
	for (;
         ac_sta.act_index < (doc_num - ac_sta.too_few_limit_doc_num) &&
         ac_sta.act_index < data.max_act_rerank_doc;
         ac_sta.act_index++) {
		SrmDocInfo *doc = bcinfo->get_doc_at(ac_sta.act_index);
        if (doc == NULL) {
            ac_sta.doc_null++;
            continue;
        }
        AcExtDataParser attr(doc);
        uint64_t uid = GetDomain(doc);
        // F_PARAM filter
        //if (bc_check_black)
		//{
		//	if(check_invalid_doc_mid(info, doc, uid, attr, ac_sta, true)) continue;
		//}
		//else
		//{
		//	if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
		//}
		if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
        bool is_good_uids = false;
		int white_type = PassByWhite(attr);
        if (white_type) {
            is_good_uids = true;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [act:"PRIU64","PRIU64",%d,USER_TYPE_GOOD_UID_OR_WHITE_WEIBO]", doc->docid, uid, white_type);
        }
        uint64_t docattr = doc->docattr;
        int source = attr.get_digit_source();
		int is_social = getSocialRelation(info, uid, docattr, social);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [act:"PRIU64","PRIU64",%d:%d, good:%d,social:%d]",
            doc->docid, uid, doc->bcidx, doc->bsidx, is_good_uids, is_social);
        // F_PARAM filter
        if (is_good_uids || (is_social == 1)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [good_uid or social pass black:"PRIU64","PRIU64",%d:%d]",
                doc->docid, uid, doc->bcidx, doc->bsidx);
        } else {
			if(check_manual_black_uids(info, doc, attr, ac_sta, uid, manual_black_uids,delay_time_qi20, true)) {
				continue;
			}
            if (!bc_check_black && check_invalid_doc(info, doc, uid, attr, check_sandbox, delay_time, delay_time_qi20, ac_sta,
                                  mask_mids, manual_black_uids, black_uids, plat_black,
                                  suspect_uids, source, black_dict, true)) {
                continue;
            }
        }
        // F_PARAM strategy
        bool black_source = false;
        if (source && BITDUP_EXIST(source_black, source, MAX_SOURCE_SIZE)) { 
            black_source = true;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_BLACK_UID_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                            " [source black:"PRIU64"]", doc->docid);
        }
		doc->filter_flag = 0;
        update_validfans_doc(info, doc, uid, uids_validfans_week, uids_validfans_hour);
        bool is_dup = false;
        int degree = 0;
        uint64_t qi = attr.get_qi();
		int granularity = get_granularity_qi(qi);
		bool is_bad_vip_or_source = doc_is_bad_result(info, doc, attr, dict);
        int dup_result = 0;

		degree = get_degree_by_exprs(info, ac_sta, doc,true);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [init d:"PRIU64",%d]", doc->docid, degree);
        bool check_pic_source_failed = false;
        bool already_check_pic_source = false;
		if(degree >= ac_sta.super_grade)
		{
			set_degree(doc, degree);
			is_dup = is_dup_update_doc(info, doc, attr, ac_sta, false, dup_result);	
			ac_sta.degree_num[degree]++;
		}
		else if(degree > ac_sta.good_grade)
		{
			set_degree(doc, degree);
			ac_sta.degree_num[degree]++;
            if(ac_sta.check_pic_source &&
               ac_sta.cluster_data_update.size() < ac_sta.cluster_first_page_num_degree) {
                CheckClusterDocPicSource(info, doc, ac_sta, attr, already_check_pic_source,
                            check_pic_source_failed);
            }
            if (!check_pic_source_failed) {
                is_dup = is_dup_update_doc(info, doc, attr, ac_sta, false, dup_result);	
                if(is_dup)
                {
                    if(dup_result)
                    {
                        degree = DEGREE_3_DUP_DUP;
                    }
                    else
                    {
                        degree = DEGREE_4_DUP;
                    }
                }
            }
			//else ac_sta.degree_num[degree]++;
		}
		else if(degree < ac_sta.del_grade)
		{
			ac_sta.degree_0++;
            set_degree(doc,degree);
			DelDocAddDupPrintLog(info, doc, attr, "less than del_grade", true, ac_sta);
			continue;
		}
		else
		{
			if(degree_is_del(info, doc, granularity))
			{
                degree = DEGREE_0;
				set_degree(doc,degree);
				ac_sta.degree_0++;
				DelDocAddDupPrintLog(info, doc, attr, "degree_is_del", true, ac_sta);
				continue;
			}
			else if(black_source || is_bad_vip_or_source ||(granularity < 3))
			{
				degree = ac_sta.low_grade;
				set_degree(doc,degree);
				ac_sta.degree_num[degree]++;
			}
			else
			{
				degree = ac_sta.good_grade;
				set_degree(doc,degree);
                if(ac_sta.check_pic_source &&
                   ac_sta.cluster_data_update.size() < ac_sta.cluster_first_page_num_degree) {
                    CheckClusterDocPicSource(info, doc, ac_sta, attr, already_check_pic_source,
                                check_pic_source_failed);
                }
                if (!check_pic_source_failed) {
                    is_dup = is_dup_update_doc(info, doc, attr, ac_sta, false, dup_result);
                    if(is_dup) {
                        degree = dup_result != 0 ? DEGREE_3_DUP_DUP : DEGREE_4_DUP;
                    } else {
                        degree = ac_sta.good_grade;
                        ac_sta.degree_num[degree]++;
                    }
                }
			}
		}
        set_degree(doc, degree);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [top_n:%d,"PRI64", d:%d:%d]", ac_sta.act_index, doc->docid, degree, doc->filter_flag);
        if(degree < ac_sta.good_grade) {
            ac_sta.degree_small++;
            continue;
        }
        ac_sta.topN_top++;
        // F_PARAM strategy
        DupClusterActOneUpdate(info, doc, attr, ac_sta);
        if (data.degrade_state <= RERANK_UPDATE_DEGRADE_STATE_1) {
            if (ac_sta.cluster_data_update.size() > data.keep_top_n &&
                ac_sta.topN_top > data.cluster_top_buff * data.keep_top_n) {
                break;
            }
        } else {
            if (ac_sta.topN_top > data.cluster_top_buff * data.keep_top_n) {
				 AC_WRITE_LOG(info, "[topn_top]");
                break;
            }
        }
	}
    // for cluster
    AC_WRITE_LOG(info, " [all:%d, act:%d, top:%d, small:%d, del: black:%d, degree0:%d, act:%d=%d, "
                "recall:%d:%d:%d, is_dup_replace_del:%d]",
                doc_num, ac_sta.act_index, ac_sta.topN_top, ac_sta.degree_small, ac_sta.black_num,
                ac_sta.degree_0, ac_sta.act_index,
                (ac_sta.degree_small + ac_sta.topN_top + ac_sta.black_num + ac_sta.degree_0),
                ac_sta.recall_qi_low_doc, ac_sta.recall_qi_low_doc_week, ac_sta.recall_qi_low_doc_month,
                ac_sta.is_dup_replace_del);

	//print grading results
	__gnu_cxx::hash_map<int,int>::iterator dit = ac_sta.degree_num.begin();
	for(;dit != ac_sta.degree_num.end();++dit)
	{
		AC_WRITE_LOG(info, "[d:%d,n:%d]",dit->first,dit->second);
	}

    int real_top = 0;
    // F_PARAM strategy
    {
        AC_WRITE_LOG(info," [size:%d:%d]", ac_sta.cluster_data_update.size(),
                    ac_sta.cluster_for_picsource_check.size());
        std::set<uint64_t> multi_dup_del;
        // remove only root
        __gnu_cxx::hash_map<uint64_t, AcDupRepostClusterDataUpdate>::iterator it =
                    ac_sta.cluster_data_update.begin();
        for (; it != ac_sta.cluster_data_update.end(); ++it) {
            // remove  count <=1 && orig's weibo
            if (it->second.root_cluster ||
                it->second.count <= 1 && it->second.root_doc) {
                AC_WRITE_DOC_DEBUG_LOG(info, it->second.root_doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [cluster remove only root dup:"PRI64", root_cluster:%d, count:%d, "PRI64"]",
                    it->first, it->second.root_cluster, it->second.count, it->second.root_doc->docid);
                multi_dup_del.insert(it->first);
                std::map<uint64_t, ClusterDocid>::iterator it_cluster =
                    ac_sta.cluster_for_picsource_check.find(it->second.root_doc->docid);
                if (it_cluster != ac_sta.cluster_for_picsource_check.end()) {
                    it_cluster->second.act = 1;
                }
            }
        }
        std::set<uint64_t>::iterator it_dup = multi_dup_del.begin();
        for (; it_dup != multi_dup_del.end(); ++it_dup) {
            __gnu_cxx::hash_map<uint64_t, AcDupRepostClusterDataUpdate>::iterator it_del =
                    ac_sta.cluster_data_update.find(*it_dup);
            if (it_del != ac_sta.cluster_data_update.end()) {
                AC_WRITE_DEBUG_LOG(info, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                            " [cluster del: dup:"PRI64"]", *it_dup);
                ac_sta.cluster_data_update.erase(it_del);
            }
        }
        // F_PARAM strategy
        ClusterRepostUpdate(info, ac_sta);
        ClusterRepostDelUpdate(info, ac_sta);
        for (int i = 0; i < ac_sta.act_index && i < doc_num; ++i) {
            SrmDocInfo *doc = bcinfo->get_doc_at(i);
            if (doc == NULL) {
                continue;
            }
            if (doc->filter_flag & 0x1) {
                // & 0x1 must degree >= DEGREE_9_GOOD
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG del_cluster:"PRI64"]", doc->docid);
                info->bcinfo.del_doc(doc);
                ac_sta.cluster_del++;
                ac_sta.topN_top--;
                continue;
            }
            int degree = get_degree(doc);
            //if (degree < DEGREE_9_GOOD) 
            if (degree < ac_sta.good_grade) {
                continue;
            }
            real_top++;
            UpdateCheckRecallData(info, ac_sta, doc);
            ac_sta.top_time_num++;
            if (ac_sta.top_n_time > doc->rank) {
                ac_sta.top_n_time = doc->rank;
            }
            if (doc->cluster_info.time && ac_sta.top_cluster_time > doc->rank) {
                ac_sta.top_cluster_time = doc->rank;
            }
        }
    }
    int orig_top = ac_sta.topN_top;
    ac_sta.topN_top = real_top;
    if (ac_sta.topN_top < ac_sta.min_topN_num_for_recall) {
        ac_sta.need_recall_dup_num = ac_sta.min_topN_num_for_recall - ac_sta.topN_top;
    }
    if ((info->intention & INTENTION_HOTSEARCH_WORD) &&
        int_from_conf(HOT_QUERY_STOP_DUPRECALL, 1)) {
        ac_sta.need_recall_dup_num = 0;
    }
    AC_WRITE_LOG(info, " [act:%d, top:%d:%d, dup(need_recall:%d), DEL: black:%d, degree0:%d, cluster:%d"
                ", recall:%d:%d:%d, top_k:%d,"PRIU64","PRIU64", is_dup_replace_del:%d, doc_null:%d]",
                ac_sta.act_index, ac_sta.topN_top, orig_top, ac_sta.need_recall_dup_num,
                ac_sta.black_num, ac_sta.degree_0, ac_sta.cluster_del,
                ac_sta.recall_qi_low_doc, ac_sta.recall_qi_low_doc_week, ac_sta.recall_qi_low_doc_month,
                ac_sta.top_time_num, ac_sta.top_n_time, ac_sta.top_cluster_time,
                ac_sta.is_dup_replace_del, ac_sta.doc_null);
    // for 1000 
    ac_sta.now_del_doc = ac_sta.black_num + ac_sta.degree_0 + ac_sta.cluster_del;
    ac_sta.no_act_doc_num = doc_num - ac_sta.act_index;
    // for 1000 already degree
    if (ac_sta.need_recall_dup_num > 0) {
        // F_PARAM strategy
        //TopNRecallDup(info, data, doc_num, DEGREE_9_GOOD, ac_sta);
        TopNRecallDup(info, data, doc_num, ac_sta.good_grade, ac_sta);
    }
    TopNActDegreeNoactAndDelDup(info, data, doc_num, ac_sta.good_grade, ac_sta);
    AC_WRITE_LOG(info,
        " [t:"PRIU64", all:%d, no_act:%d, top:%d, dup(need_recall:%d,recall:%d), act_keep:%d"
        ", act_time_del:%d, del:%d, act:%d==%d, recall:%d:%d:%d, is_dup_replace_del:%d]",
        ac_sta.top_n_time, doc_num, ac_sta.no_act_doc_num, ac_sta.topN_top, ac_sta.need_recall_dup_num,
        ac_sta.recall_dup_num, ac_sta.act_keep_doc,
        ac_sta.act_del_time, ac_sta.now_del_doc, ac_sta.act_index,
        (ac_sta.topN_top + ac_sta.act_keep_doc + ac_sta.act_del_time + ac_sta.now_del_doc),
        ac_sta.recall_qi_low_doc, ac_sta.recall_qi_low_doc_week, ac_sta.recall_qi_low_doc_month,
        ac_sta.is_dup_replace_del);
    // F_PARAM strategy
    adjust_nick_split(info);
    adjust_nick_partial_result(info);
    // adjust_movie_result(info);
    // adjust category results
    adjust_category_result(info, int_from_conf(CATEGORY_RESULT_TIMELINE, 100), 0);
    // for 1000 other, no cluster
    int left_black = 0;
    ac_sta.top_n_left_better_time = ac_sta.top_n_time;
    // F_PARAM strategy
    for (int i = ac_sta.act_index; i < doc_num; ++i) {
        SrmDocInfo *doc = bcinfo->get_doc_at(i);
        if (doc == NULL) {
            continue;
        }
        // enought
        // F_PARAM strategy
        if (ac_sta.act_keep_doc + ac_sta.topN_top > result_keep_num + ac_sta.uid_social_num_buf) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN KEEP_NUM_OK :"PRI64", :%d]", doc->docid, i);
            info->bcinfo.del_doc(doc);
            continue;
        }
        // F_PARAM strategy
        // check time when no recall_qi_low_doc
        if (ac_sta.recall_qi_low_doc <= 0 &&
            ac_sta.recall_qi_low_doc_week <= 0 &&
            ac_sta.recall_qi_low_doc_month <= 0 &&
            doc->rank >= ac_sta.top_n_time) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN >top_n_time :"PRI64", :%d]", doc->docid, i);
            info->bcinfo.del_doc(doc);
            ac_sta.left_time_bad++;
            continue;
        }
        AcExtDataParser attr(doc);
        uint64_t uid = GetDomain(doc);
        // F_PARAM filter
        // check black
        //if (!bc_check_black && check_invalid_doc_mid(info, doc, uid, attr, ac_sta, true)) {
        //    continue;
        //}
		//else if (bc_check_black)
		//{
		//	if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
		//}

		if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
       // bool is_good_uids = false;
	   // int white_type = PassByWhite(attr);
       // if (white_type) {
       //     is_good_uids = true;
       //     AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
       //         " [act:"PRIU64","PRIU64",%d,USER_TYPE_GOOD_UID_OR_WHITE_WEIBO]", doc->docid, uid, white_type);
       // }

		int degree = get_degree_by_exprs(info, ac_sta, doc, false);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [>act_index act:"PRIU64","PRIU64",%d:%d, good/social:%d]",
            doc->docid, uid, doc->bcidx, doc->bsidx, degree);
		if(degree >= ac_sta.super_grade)  
		{
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [good_uid or social pass black:"PRIU64","PRIU64",%d:%d]",
                doc->docid, uid, doc->bcidx, doc->bsidx);
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [<1000 social or vip keep:"PRI64", index:%d,good/social:%d]",
                    doc->docid, i, degree);

            ac_sta.act_keep_doc++;
			set_degree(doc,degree);
			if (!update_repost_fake_cluster_struct(info, doc, attr, ac_sta))
			{
				AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
						" [DEL_LOG outTopN update_repost :"PRIU64",dup del:"PRIU64", :%d]",
						doc->docid, attr.get_dup(), i);
				info->bcinfo.del_doc(doc);
				ac_sta.update_repost++;
				continue;
			}
			int dup_result = 0;
			is_dup_update_doc(info, doc, attr, ac_sta, true, dup_result);
			continue;
		}
		else
		{
			int source = attr.get_digit_source();
			if(check_manual_black_uids(info, doc, attr, ac_sta, uid, manual_black_uids, delay_time_qi20, true)) {
				continue;
			}
			if (!bc_check_black && check_invalid_doc(info, doc, uid, attr, check_sandbox, delay_time, delay_time_qi20, ac_sta,
						mask_mids, manual_black_uids, black_uids, plat_black,suspect_uids, source, black_dict, true))
				continue;
		}
		
        // check bad
        // F_PARAM strategy
		int granularity = get_granularity_qi(attr.get_qi());
        if (degree_is_del(info, doc, granularity)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN degree_del :"PRI64", :%d]", doc->docid, i);
            info->bcinfo.del_doc(doc);
            ac_sta.left_bad++;
            continue;
        }
        if(ac_sta.check_pic_source &&
           (ac_sta.topN_top+ac_sta.act_keep_doc) < ac_sta.cluster_first_page_num_after_cluster) {
            ac_sta.qualified_num++;
            uint64_t digit_attr = attr.get_digit_attr();
            hot_cheat_check_addinfo(info, digit_attr,
                        (ac_sta.topN_top+ac_sta.act_keep_doc), ac_sta);
            if (CheckPicSource(info, doc, ac_sta, attr, digit_attr)) {
                ac_sta.degree_0++;
                DelDocAddDupPrintLog(info, doc, attr, "source_check_failed", false, ac_sta);
                continue;
            }
        }
        // for update repost cluster_struct
        // for repost must > last_cluster_time
        // F_PARAM strategy
        if (CheckForwardClusterDel(info, attr, ac_sta, doc)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN repost_cluster t:%u > cluster_t:"PRIU64",dup del:"PRI64", :%d]",
                    doc->rank, ac_sta.top_cluster_time, doc->docid, i);
            info->bcinfo.del_doc(doc);
            ac_sta.left_cluser_time_error++;
            continue;
        }
        // for update repost cluster_struct
        // F_PARAM strategy
        if (!update_repost_fake_cluster_struct(info, doc, attr, ac_sta)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN update_repost :"PRIU64",dup del:"PRIU64", :%d]",
                    doc->docid, attr.get_dup(), i);
            info->bcinfo.del_doc(doc);
            ac_sta.update_repost++;
            continue;
        }
        // check dup
		doc->filter_flag = 0;
        degree = 0;
        // validfans need in is_bad_blue_vip_doc <= doc_is_bad_result
        // update_validfans_doc(info, doc, uid, uids_validfans_week, uids_validfans_hour);
        // check low and recall
        // F_PARAM strategy
        int invalid = is_invalid_topN_left_doc(info, doc, attr, ac_sta);
        if (invalid) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [<1000 > top_n invalid:%d set flag:"PRI64", :%d]", invalid, doc->docid, i);
            degree = DEGREE_8_RECALL;
            doc->filter_flag = 0;
            doc->filter_flag |= degree << 20;
            ac_sta.left_recall++;
            continue;
        }
        //doc_num - i < ac_sta.too_few_limit_doc_num &&
        // F_PARAM strategy
        int dup_result = 0;
        if (is_dup_update_doc(info, doc, attr, ac_sta, true, dup_result)) {
            if (!dup_result && ac_sta.need_recall_dup_num > 0) {
                degree = DEGREE_6_RECALL_LOW_DUP;
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [RECALL_LOW_DUP_BETTER 3, d:%d :"PRI64", t:%u]",
                        degree, doc->docid, doc->rank);
            } else {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG outTopN dup:"PRI64", :%d]", doc->docid, i);
                info->bcinfo.del_doc(doc);
                ac_sta.left_dup++;
                continue;
            }
        }
        // F_PARAM strategy
        // for recall
        if (UpdateCheckRecallData(info, ac_sta, doc)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [<1000 recall_doc_d keep:"PRI64", :%d]", doc->docid, i);
            ac_sta.act_keep_doc++;
            ac_sta.recall_doc_num++;
            if (degree == DEGREE_6_RECALL_LOW_DUP) {
                ac_sta.need_recall_dup_num--;
                ac_sta.recall_dup_num++;
            }
            continue;
        }
        // F_PARAM strategy
        if (doc->rank >= ac_sta.top_n_time) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN >top_n_time :"PRI64", :%d]", doc->docid, i);
            info->bcinfo.del_doc(doc);
            ac_sta.left_time_bad++;
            continue;
        }
        if (degree == DEGREE_6_RECALL_LOW_DUP) {
            ac_sta.need_recall_dup_num--;
            ac_sta.recall_dup_num++;
        }
        if (doc->rank < ac_sta.top_n_left_better_time) {
            ac_sta.top_n_left_better_time = doc->rank;
        }
        // 1: < top_n_time
        // 2: >= top_n_time, but need recall, so all keep
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [<1000 > top_n keep:"PRI64", d:%d,:%d]", doc->docid, degree, i);
        ac_sta.act_keep_doc++;
    }
    AC_WRITE_LOG(info, " [act_keep_doc:%d, top:%d, result_keep_num:%d]",
                ac_sta.act_keep_doc, ac_sta.topN_top, result_keep_num);
    // for recall
    // F_PARAM strategy
    if (ac_sta.act_keep_doc + ac_sta.topN_top < result_keep_num) {
        for (int i = ac_sta.act_index; i < doc_num; ++i) {
            SrmDocInfo *doc = bcinfo->get_doc_at(i);
            if (doc == NULL) {
                continue;
            }
            int degree = get_degree(doc);
            if (degree != DEGREE_8_RECALL) {
                continue;
            }
            if (ac_sta.act_keep_doc + ac_sta.topN_top > result_keep_num + ac_sta.uid_social_num_buf) {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG recall KEEP_NUM_ok :"PRI64", :%d]", doc->docid, i);
                info->bcinfo.del_doc(doc);
                continue;
            }
            if ((ac_sta.bc_input_num > ac_sta.min_num_skip_dup) &&
                (doc->rank >= ac_sta.top_n_time || doc->rank >= ac_sta.top_n_left_better_time)) {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG outTopN last >top_n_time(%d>%d):"PRI64":%d,%u > "PRIU64","PRIU64"]",
                        ac_sta.bc_input_num, ac_sta.min_num_skip_dup,
                        doc->docid, i, doc->rank, ac_sta.top_n_time, ac_sta.top_n_left_better_time);
                info->bcinfo.del_doc(doc);
                ac_sta.left_time_bad++;
                continue;
            }
            doc->filter_flag = 0;
            AcExtDataParser attr(doc);
            if(ac_sta.check_pic_source &&
               (ac_sta.topN_top+ac_sta.act_keep_doc) < ac_sta.cluster_first_page_num_after_cluster) {
                ac_sta.qualified_num++;
                uint64_t digit_attr = attr.get_digit_attr();
                hot_cheat_check_addinfo(info, digit_attr,
                            (ac_sta.topN_top+ac_sta.act_keep_doc), ac_sta);
                if (CheckPicSource(info, doc, ac_sta, attr, digit_attr)) {
                    ac_sta.degree_0++;
                    DelDocAddDupPrintLog(info, doc, attr, "source_check_failed", false, ac_sta);
                    continue;
                }
            }
            int dup_result = 0;
            if (is_dup_update_doc(info, doc, attr, ac_sta, true, dup_result)) {
                if (!dup_result && ac_sta.need_recall_dup_num > 0) {
                    degree = DEGREE_6_RECALL_LOW_DUP;
                    ac_sta.need_recall_dup_num--;
                    ac_sta.recall_dup_num++;
                    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                            " [RECALL_LAST_LOW_DUP, d:%d :"PRI64", t:%u]",
                            degree, doc->docid, doc->rank);
                } else {
                    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                            " [DEL_LOG outTopN last dup:"PRI64", :%d]", doc->docid, i);
                    info->bcinfo.del_doc(doc);
                    ac_sta.left_dup++;
                    continue;
                }
            }
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [>1000 recall rank:%u cluster_time:"PRIU64","PRIU64" del:"PRI64", :%d]",
                    doc->rank, ac_sta.top_cluster_time, GET_FORWARD_FLAG(attr.get_digit_attr()),
                    doc->docid, i);
            ac_sta.recall_doc_num++;
            ac_sta.act_keep_doc++;
        }
    }
	info->bcinfo.refresh();
    doc_num = bcinfo->get_doc_num();
	bcinfo->sort(0, doc_num, my_time_cmp);
    // for uid dup, and socialtime limit
    // F_PARAM strategy
    ac_uid_dup_and_socialtime_limit(info, data, doc_num, ac_sta);
    AC_WRITE_LOG(info, " [> top_N: left_time_bad:%d, balck:%d, update_repost:%d, left_bad:%d"
                       ", left_dup:%d, left_recall:%d, uid_dup:%d, cluster_del:%d"
                       ", real recall:%d, repost root_mid_del:%d, is_dup_replace_del:%d] ",
                ac_sta.left_time_bad, ac_sta.black_num, ac_sta.update_repost, ac_sta.left_bad,
                ac_sta.left_dup, ac_sta.left_recall, ac_sta.del_uid_dup_num,
                ac_sta.repost_cluster_del_num, ac_sta.recall_doc_num, ac_sta.repost_root_mid_del_count,
                ac_sta.is_dup_replace_del);
	info->bcinfo.refresh();
    // last sort by time
	doc_num = bcinfo->get_doc_num();
    reset_rank_use_time(info,ranktype);
	bcinfo->sort(0, doc_num, my_time_cmp);
    int before_pic_num = doc_num;
/*    
    if (*uid_source_dict and *realtime_hotquery and realtime_hotquery->test(key))
    //if (*uid_source_dict) 
    {
        hot_cheat_check(info, ac_sta, doc_num, uid_source_dict);
        info->bcinfo.refresh();
    }
    */
    // F_PARAM strategy
    if (ac_sta.check_pic_source) {
        AC_WRITE_LOG(info, " [new_before_hot:pic:(%d,%d,%d) video:(%d,%d,%d) del:(%d,%d]",
                    ac_sta.original_pic_num, ac_sta.all_pic_num,
                    (ac_sta.all_pic_num - ac_sta.del_pic_num),
                    ac_sta.orig_video_num, ac_sta.all_video_num,
                    (ac_sta.all_video_num - ac_sta.del_video_num),
                    ac_sta.del_pic_num, ac_sta.del_other_num);
        AC_WRITE_LOG(info, " [hot_cheat_check]");
        int use_new_hot_pic_source = int_from_conf(USE_NEW_HOT_PIC_SOURCE, 1);
        if (use_new_hot_pic_source) {
            hot_cheat_check_v2(info, ac_sta, doc_num);
        } else {
            hot_cheat_check(info, ac_sta, doc_num);
        }
        info->bcinfo.refresh();
    }
	doc_num = bcinfo->get_doc_num();
    AC_WRITE_LOG(info, " [f_1:%d, f_2:%d, o_num:%d]", before_pic_num, doc_num, orig_num);
    
	//img_check_query == hot_query.txt
	if ((info->intention & INTENTION_HOTSEARCH_WORD) && *img_check_query && img_check_query->test(key))
	//if ( *img_check_query && img_check_query->test(key))
	{
		AC_WRITE_LOG(info, " [new source check duplication");
		drop_duplicated_new_source(info, ac_sta, doc_num);
		info->bcinfo.refresh();
	}
	int after_source_num = bcinfo->get_doc_num();
    AC_WRITE_LOG(info, " [before:%d, after:%d, o_num:%d]", doc_num, after_source_num, orig_num);
    return true;
}

//static bool ac_reranking_update_base(QsInfo *info, const AcRerankData& data) {
static bool ac_reranking_update_base(QsInfo *info, const AcRerankData& data, Ac_statistic& ac_sta) 
{
	if (info == NULL) {
		return false;
	}
	QsInfoBC *bcinfo = &info->bcinfo;
	int ranktype = RankType(info);
	int doc_num = bcinfo->get_doc_num();
    int orig_num = doc_num;
	ac_sta.original_num = doc_num;
	ac_sta.total_num = doc_num;
    // F_PARAM get dup flag
    int resort_flag = 1;
    int dup_flag = 1;
    ac_set_flag(info, &resort_flag, &dup_flag);
    ac_sta.dup_flag = dup_flag;
    int result_keep_num = int_from_conf(RESULT_KEEP_NUM, 1000);
    ac_sta.bc_input_num = doc_num;
    if (ac_sta.dup_flag && doc_num <= ac_sta.min_num_skip_dup) {
        ac_sta.dup_flag = 0;
    }

    // F_PARAM strategy set socialtime
    if (info->parameters.getXsort() == std::string("social") &&
        info->parameters.getUs() == 1 &&
        (info->parameters.gray == 1 || info->parameters.socialtime)) {
        ac_sta.is_socialtime = true;
    }
    std::string key;
    if (info->params.key) {
        key = info->params.key;
    } else {
        key = info->params.query;
    }
    // F_PARAM strategy set cluster_repost
    if (info->parameters.getXsort() == std::string("social") &&
        info->parameters.getUs() == 1 &&
        info->parameters.cluster_repost == 1) {
        ac_sta.is_cluster_repost = true;
        ac_sta.min_repost_num = 2;
    }
	// int top_n_degree = int_from_conf(TOP_N_DEGREE, DEGREE_9_GOOD);
    AC_WRITE_LOG(info, " [top_n_degree:%d]", DEGREE_9_GOOD);
    bool check_sandbox = true;
    /*
	auto_reference<ACStringSet> sandbox_query("sandbox_query");
	if (*sandbox_query == NULL) {
        check_sandbox = false;
	} else {
        if (!(info->intention & INTENTION_STAR) && !sandbox_query->test(key)) {
            check_sandbox = false;
        }
    }
    */
    int bc_check_black = int_from_conf(BLACK_LIST_CHECK_SWITCH, 1);
    //{TODO:del
    auto_reference<SourceIDDict> black_dict("source_id.dict");
	auto_reference<ACLongSet> mask_mids("mask_mids");
	auto_reference<ACLongSet> manual_black_uids("uids_black_manual.txt");
	auto_reference<DASearchDictSet> black_uids("uids_black");
	auto_reference<DASearchDictSet> plat_black("plat_black");
	auto_reference<DASearchDict> suspect_uids("suspect_uids");

    //}
    time_t time_now = ac_sta.curtime;
	time_t delay_time = time_now - int_from_conf(SUSPECT_DELAY_TIME, 172800);
	time_t delay_time_qi20 = time_now - int_from_conf(QI20_DELAY_TIME, 604800);

	char* source_black = bitdup_get(g_sort.bitmap, g_sort.source_black_type);
	//auto_reference<DASearchDictSet> good_uids("good_uids");
    auto_reference<SourceIDDict> dict("old_source_id.dict");
	auto_reference<DASearchDict> uids_validfans_week("uids_validfans_week");
	auto_reference<DASearchDict> uids_validfans_hour("uids_validfans_hour");
	//auto_reference<DASearchDictSet> dup_black_uids("dup_uids_black");
	//auto_reference<ACLongSet> sandbox_uids("sandbox_uids");
    // for old imgcheat
    //auto_reference<DASearchDictSet> uid_source_mul_dict("uid_source_mul_dict");
    //auto_reference<DASearchDictSet> uid_source_suspect_dict("uid_source_suspect_dict");
	//auto_reference<ACLongSet> uid_source_black_source("uid_source_black_source");
	//auto_reference<ACLongSet> uid_source_white_source("uid_source_white_source.txt");
	auto_reference<ACStringSet> img_check_query("hot_query.txt");
	/*
    if (!(*good_uids)) {
        AC_WRITE_LOG(info, " [good_uids load error]");
    }
	*/
    int inten_user_type = intention_user_type(info->intention);
	QsInfoSocial *social = &info->socialinfo;
    // F_PARAM strategy sort_time
    if (!data.degrade) {
        int sort_time_num = doc_num;
        if (sort_time_num > data.max_act_rerank_doc) {
            sort_time_num = data.max_act_rerank_doc;
        }
        if (ac_sta.is_socialtime) {
            for (int i = 0; i < sort_time_num; ++i) {
                SrmDocInfo *doc = bcinfo->get_doc_at(i);
                if (doc == NULL) {
                    continue;
                }
                UpdateFamousConcernInfo(info, doc, inten_user_type);
                UpdataTimeOne(info, doc, inten_user_type, time_now);
            }
        }
        bcinfo->sort(0, sort_time_num, my_time_cmp);
        // get_strategy(info, ac_sta);
        get_strategy_inten(info, ac_sta);
		if ( ac_sta.need_inten_ploy_filter==1 || ac_sta.need_category_filter==1 )
		{
			AC_WRITE_LOG(info, " [film result >= 100, no recall qi low doc]");
			ac_sta.recall_qi_low_doc = 0;
			ac_sta.recall_qi_low_doc_week = 0;
			ac_sta.recall_qi_low_doc_month = 0;
		}
    }
    /*
    if (info->parameters.abtest) {
        if ((info->intention & INTENTION_HOTSEARCH_WORD) ||
            (info->intention & INTENTION_PLACE) ||
            *img_check_query && img_check_query->test(key)) {
            ac_sta.check_pic_source = true;
            ac_sta.qualified_num = 0;
            ac_sta.original_pic_num = 0;
            ac_sta.orig_video_num = 0;
            ac_sta.all_pic_num = 0;
            ac_sta.all_video_num = 0;
            ac_sta.del_pic_num = 0;
            ac_sta.del_other_num = 0;
            ac_sta.del_video_num = 0;
        }
    } else {
    }
    */
    if (*img_check_query && img_check_query->test(key)) {
        if (ac_sta.check_pic_source_dict) {
            ac_sta.check_pic_source = true;
            ac_sta.check_pic_source_dict_query = true;
            ac_sta.qualified_num = 0;
            ac_sta.original_pic_num = 0;
            ac_sta.orig_video_num = 0;
            ac_sta.all_pic_num = 0;
            ac_sta.all_video_num = 0;
            ac_sta.del_pic_num = 0;
            ac_sta.del_other_num = 0;
            ac_sta.del_video_num = 0;
        }
    } else if (ac_sta.check_pic_source_normal) {
        ac_sta.check_pic_source = true;
    }
    int valid_forward_fwnum = int_from_conf(VALID_FORWARD_FWNUM, 1);
    // last 40, don't act cluster
	for (;
         ac_sta.act_index < (doc_num - ac_sta.too_few_limit_doc_num) &&
         ac_sta.act_index < data.max_act_rerank_doc;
         ac_sta.act_index++) {
		SrmDocInfo *doc = bcinfo->get_doc_at(ac_sta.act_index);
        if (doc == NULL) {
            ac_sta.doc_null++;
            continue;
        }
        AcExtDataParser attr(doc);
        uint64_t uid = GetDomain(doc);
		/*
        if (*good_uids && good_uids->Search(uid) != -1) {
            is_good_uids = true;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [act:"PRIU64","PRIU64",good_uids_files_good]", doc->docid, uid);
        }
		*/
        // F_PARAM filter
        //if (bc_check_black)
		//{
		//	if(check_invalid_doc_mid(info, doc, uid, attr, ac_sta, true)) continue;
		//}
		//else
		//{
		//	if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
		//}
		if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
        bool is_good_uids = false;
		int white_type = PassByWhite(attr);
        if (white_type) {
            is_good_uids = true;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [act:"PRIU64","PRIU64",%d,USER_TYPE_GOOD_UID_OR_WHITE_WEIBO]", doc->docid, uid, white_type);
        }
        uint64_t docattr = doc->docattr;
        int source = attr.get_digit_source();
		int is_social = getSocialRelation(info, uid, docattr, social);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [act:"PRIU64","PRIU64",%d:%d, good:%d,social:%d]",
            doc->docid, uid, doc->bcidx, doc->bsidx, is_good_uids, is_social);
        // F_PARAM filter
        if (is_good_uids || (is_social == 1)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [good_uid or social pass black:"PRIU64","PRIU64",%d:%d]",
                doc->docid, uid, doc->bcidx, doc->bsidx);
        } else {
			if(check_manual_black_uids(info, doc, attr, ac_sta, uid, manual_black_uids, delay_time_qi20, true)) {
				continue;
			}
            if ( !bc_check_black && check_invalid_doc(info, doc, uid, attr, check_sandbox, delay_time, delay_time_qi20, ac_sta,
                                  mask_mids, manual_black_uids, black_uids, plat_black,
                                  suspect_uids, source, black_dict, true)) {
                continue;
            }
        }
        // F_PARAM strategy
        bool black_source = false;
        if (source && BITDUP_EXIST(source_black, source, MAX_SOURCE_SIZE)) { 
            black_source = true;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_BLACK_UID_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                            " [source black:"PRIU64"]", doc->docid);
        }
		doc->filter_flag = 0;
        update_validfans_doc(info, doc, uid, uids_validfans_week, uids_validfans_hour);
        bool is_dup = false;
        int degree = 0;
        uint64_t qi = attr.get_qi();
		int granularity = get_granularity_qi(qi);
		bool is_bad_vip_or_source = doc_is_bad_result(info, doc, attr, dict);
		bool has_valid_fwnum = is_valid_forward(doc, attr, valid_forward_fwnum);
        bool is_valid_inten_ploy = false;
        bool check_pic_source_failed = false;
        bool already_check_pic_source = false;
        if (ac_sta.need_inten_ploy_filter == 1) {
            is_valid_inten_ploy = is_valid_inten_ploy_doc(info, doc, attr, ac_sta);
        }
        bool is_category_filter = true;
        if (ac_sta.need_category_filter == 1) {
            is_category_filter = is_category_filter_doc(info, doc, attr, ac_sta);
        }
        int dup_result = 0;
		if (is_social == 1) {
			degree = DEGREE_200_SOCIAL;
            doc->filter_flag |= degree<<20;
            is_dup = is_dup_update_doc(info, doc, attr, ac_sta, false, dup_result);
			ac_sta.degree_17++;
        } else if (is_good_uids) {
			degree = DEGREE_100_VIP;
            doc->filter_flag |= degree<<20;
            is_dup = is_dup_update_doc(info, doc, attr, ac_sta, false, dup_result);
			ac_sta.degree_16++;
		} else if (is_social == 2) {
            degree = DEGREE_14_SOCIAL_AT;
            doc->filter_flag |= degree<<20;
            if(ac_sta.check_pic_source &&
               ac_sta.cluster_data_update.size() < ac_sta.cluster_first_page_num_degree) {
                CheckClusterDocPicSource(info, doc, ac_sta, attr, already_check_pic_source,
                            check_pic_source_failed);
            }
            if (!check_pic_source_failed) {
                is_dup = is_dup_update_doc(info, doc, attr, ac_sta, false, dup_result);
                if(is_dup) {
                    if (dup_result) {
                        degree = DEGREE_3_DUP_DUP;
                    } else {
                        degree = DEGREE_4_DUP;
                    }
                } else {
                    degree = DEGREE_14_SOCIAL_AT;
                    ac_sta.degree_14++;
                }
            }
		} else if (is_valid_inten_ploy) {
            degree = DEGREE_13_VALID_INTEN_PLOY;
            doc->filter_flag |= degree<<20;
            if(ac_sta.check_pic_source &&
               ac_sta.cluster_data_update.size() < ac_sta.cluster_first_page_num_degree) {
                CheckClusterDocPicSource(info, doc, ac_sta, attr, already_check_pic_source,
                            check_pic_source_failed);
            }
            if (!check_pic_source_failed) {
                is_dup = is_dup_update_doc(info, doc, attr, ac_sta, false, dup_result);
                if(is_dup) {
                    if (dup_result) {
                        degree = DEGREE_3_DUP_DUP;
                    } else {
                        degree = DEGREE_4_DUP;
                    }
                } else {
                    degree = DEGREE_13_VALID_INTEN_PLOY;
                    ac_sta.degree_13++;
                }
			}
		} else if (has_valid_fwnum) {
            degree = DEGREE_12_VALID_FWNUM;
            doc->filter_flag |= degree<<20;
            if(ac_sta.check_pic_source &&
               ac_sta.cluster_data_update.size() < ac_sta.cluster_first_page_num_degree) {
                CheckClusterDocPicSource(info, doc, ac_sta, attr, already_check_pic_source,
                            check_pic_source_failed);
            }
            if (!check_pic_source_failed) {
                is_dup = is_dup_update_doc(info, doc, attr, ac_sta, false, dup_result);
                if(is_dup) {
                    if (dup_result) {
                        degree = DEGREE_3_DUP_DUP;
                    } else {
                        degree = DEGREE_4_DUP;
                    }
                } else {
                    degree = DEGREE_12_VALID_FWNUM;
                    ac_sta.degree_12++;
                }
            }
		} else if (!is_category_filter) {
            degree = DEGREE_11_CATAGORY_FILTER;
            doc->filter_flag |= degree<<20;
            if(ac_sta.check_pic_source &&
               ac_sta.cluster_data_update.size() < ac_sta.cluster_first_page_num_degree) {
                CheckClusterDocPicSource(info, doc, ac_sta, attr, already_check_pic_source,
                            check_pic_source_failed);
            }
            if (!check_pic_source_failed) {
                is_dup = is_dup_update_doc(info, doc, attr, ac_sta, false, dup_result);
                if(is_dup) {
                    if (dup_result) {
                        degree = DEGREE_3_DUP_DUP;
                    } else {
                        degree = DEGREE_4_DUP;
                    }
                } else {
                    degree = DEGREE_11_CATAGORY_FILTER;
                    ac_sta.degree_11++;
                }
            }
		} else {
            if (degree_is_del(info, doc, granularity)) {
                ac_sta.degree_0++;
                DelDocAddDupPrintLog(info, doc, attr, "degree_is_del", true, ac_sta);
                continue;
			} else if(black_source || is_bad_vip_or_source || (granularity < 3)) {
                degree = DEGREE_7_NOACT_DUP;
			} else {
                degree = DEGREE_9_GOOD;
                doc->filter_flag |= degree<<20;
                if(ac_sta.check_pic_source &&
                   ac_sta.cluster_data_update.size() < ac_sta.cluster_first_page_num_degree) {
                    CheckClusterDocPicSource(info, doc, ac_sta, attr, already_check_pic_source,
                                check_pic_source_failed);
                }
                if (!check_pic_source_failed) {
                    is_dup = is_dup_update_doc(info, doc, attr, ac_sta, false, dup_result);
                    if(is_dup) {
                        if (dup_result) {
                            degree = DEGREE_3_DUP_DUP;
                        } else {
                            degree = DEGREE_4_DUP;
                        }
                    } else {
                        degree = DEGREE_9_GOOD;
                    }
                }
			}
		}
		doc->filter_flag = 0;
		doc->filter_flag |= degree<<20;
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [top_n:%d,"PRI64", d:%d:%d]", ac_sta.act_index, doc->docid, degree, doc->filter_flag);
        if(degree < DEGREE_9_GOOD) {
            ac_sta.degree_small++;
            continue;
        }
        ac_sta.topN_top++;
        // F_PARAM strategy
        DupClusterActOneUpdate(info, doc, attr, ac_sta);
        if (data.degrade_state <= RERANK_UPDATE_DEGRADE_STATE_1) {
            if (ac_sta.cluster_data_update.size() > data.keep_top_n &&
                ac_sta.topN_top > data.cluster_top_buff * data.keep_top_n) {
                break;
            }
        } else {
            if (ac_sta.topN_top > data.cluster_top_buff * data.keep_top_n) {
                break;
            }
        }
	}
    // for cluster
    AC_WRITE_LOG(info, " [all:%d, act:%d, top:%d, small:%d, del: black:%d, degree0:%d, act:%d=%d, "
                "recall:%d:%d:%d, is_dup_replace_del:%d]",
                doc_num, ac_sta.act_index, ac_sta.topN_top, ac_sta.degree_small, ac_sta.black_num,
                ac_sta.degree_0, ac_sta.act_index,
                (ac_sta.degree_small + ac_sta.topN_top + ac_sta.black_num + ac_sta.degree_0),
                ac_sta.recall_qi_low_doc, ac_sta.recall_qi_low_doc_week, ac_sta.recall_qi_low_doc_month,
                ac_sta.is_dup_replace_del);
    int real_top = 0;
    // F_PARAM strategy
    {
        AC_WRITE_LOG(info," [size:%d:%d]", ac_sta.cluster_data_update.size(),
                    ac_sta.cluster_for_picsource_check.size());
        std::set<uint64_t> multi_dup_del;
        // remove only root
        __gnu_cxx::hash_map<uint64_t, AcDupRepostClusterDataUpdate>::iterator it =
                    ac_sta.cluster_data_update.begin();
        for (; it != ac_sta.cluster_data_update.end(); ++it) {
            // remove  count <=1 && orig's weibo
            if (it->second.root_cluster ||
                it->second.count <= 1 && it->second.root_doc) {
                AC_WRITE_DOC_DEBUG_LOG(info, it->second.root_doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [cluster remove only root dup:"PRI64", root_cluster:%d, count:%d, "PRI64"]",
                    it->first, it->second.root_cluster, it->second.count, it->second.root_doc->docid);
                multi_dup_del.insert(it->first);
                std::map<uint64_t, ClusterDocid>::iterator it_cluster =
                    ac_sta.cluster_for_picsource_check.find(it->second.root_doc->docid);
                if (it_cluster != ac_sta.cluster_for_picsource_check.end()) {
                    it_cluster->second.act = 1;
                }
            }
        }
        std::set<uint64_t>::iterator it_dup = multi_dup_del.begin();
        for (; it_dup != multi_dup_del.end(); ++it_dup) {
            __gnu_cxx::hash_map<uint64_t, AcDupRepostClusterDataUpdate>::iterator it_del =
                    ac_sta.cluster_data_update.find(*it_dup);
            if (it_del != ac_sta.cluster_data_update.end()) {
                AC_WRITE_DEBUG_LOG(info, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                            " [cluster del: dup:"PRI64"]", *it_dup);
                ac_sta.cluster_data_update.erase(it_del);
            }
        }
        // F_PARAM strategy
        ClusterRepostUpdate(info, ac_sta);
        ClusterRepostDelUpdate(info, ac_sta);
        for (int i = 0; i < ac_sta.act_index && i < doc_num; ++i) {
            SrmDocInfo *doc = bcinfo->get_doc_at(i);
            if (doc == NULL) {
                continue;
            }
            if (doc->filter_flag & 0x1) {
                // & 0x1 must degree >= DEGREE_9_GOOD
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG del_cluster:"PRI64"]", doc->docid);
                info->bcinfo.del_doc(doc);
                ac_sta.cluster_del++;
                ac_sta.topN_top--;
                continue;
            }
            int degree = get_degree(doc);
            if (degree < DEGREE_9_GOOD) {
                continue;
            }
            real_top++;
            UpdateCheckRecallData(info, ac_sta, doc);
            ac_sta.top_time_num++;
            if (ac_sta.top_n_time > doc->rank) {
                ac_sta.top_n_time = doc->rank;
            }
            if (doc->cluster_info.time && ac_sta.top_cluster_time > doc->rank) {
                ac_sta.top_cluster_time = doc->rank;
            }
        }
    }
    int orig_top = ac_sta.topN_top;
    ac_sta.topN_top = real_top;
    if (ac_sta.topN_top < ac_sta.min_topN_num_for_recall) {
        ac_sta.need_recall_dup_num = ac_sta.min_topN_num_for_recall - ac_sta.topN_top;
    }
    if ((info->intention & INTENTION_HOTSEARCH_WORD) &&
        int_from_conf(HOT_QUERY_STOP_DUPRECALL, 1)) {
        ac_sta.need_recall_dup_num = 0;
    }
    AC_WRITE_LOG(info, " [act:%d, top:%d:%d, dup(need_recall:%d), DEL: black:%d, degree0:%d, cluster:%d"
                ", recall:%d:%d:%d, top_k:%d,"PRIU64","PRIU64", is_dup_replace_del:%d, doc_null:%d]",
                ac_sta.act_index, ac_sta.topN_top, orig_top, ac_sta.need_recall_dup_num,
                ac_sta.black_num, ac_sta.degree_0, ac_sta.cluster_del,
                ac_sta.recall_qi_low_doc, ac_sta.recall_qi_low_doc_week, ac_sta.recall_qi_low_doc_month,
                ac_sta.top_time_num, ac_sta.top_n_time, ac_sta.top_cluster_time,
                ac_sta.is_dup_replace_del, ac_sta.doc_null);
    // for 1000 
    ac_sta.now_del_doc = ac_sta.black_num + ac_sta.degree_0 + ac_sta.cluster_del;
    ac_sta.no_act_doc_num = doc_num - ac_sta.act_index;
    // for 1000 already degree
    if (ac_sta.need_recall_dup_num > 0) {
        // F_PARAM strategy
        TopNRecallDup(info, data, doc_num, DEGREE_9_GOOD, ac_sta);
    }
    TopNActDegreeNoactAndDelDup(info, data, doc_num, DEGREE_9_GOOD, ac_sta);
    AC_WRITE_LOG(info,
        " [t:"PRIU64", all:%d, no_act:%d, top:%d, dup(need_recall:%d,recall:%d), act_keep:%d"
        ", act_time_del:%d, del:%d, act:%d==%d, recall:%d:%d:%d, is_dup_replace_del:%d]",
        ac_sta.top_n_time, doc_num, ac_sta.no_act_doc_num, ac_sta.topN_top, ac_sta.need_recall_dup_num,
        ac_sta.recall_dup_num, ac_sta.act_keep_doc,
        ac_sta.act_del_time, ac_sta.now_del_doc, ac_sta.act_index,
        (ac_sta.topN_top + ac_sta.act_keep_doc + ac_sta.act_del_time + ac_sta.now_del_doc),
        ac_sta.recall_qi_low_doc, ac_sta.recall_qi_low_doc_week, ac_sta.recall_qi_low_doc_month,
        ac_sta.is_dup_replace_del);
    // F_PARAM strategy
    adjust_nick_split(info);
    adjust_nick_partial_result(info);
    // adjust_movie_result(info);
    // adjust category results
    adjust_category_result(info, int_from_conf(CATEGORY_RESULT_TIMELINE, 100), 0);
    // for 1000 other, no cluster
    int left_black = 0;
    ac_sta.top_n_left_better_time = ac_sta.top_n_time;
    // F_PARAM strategy
    for (int i = ac_sta.act_index; i < doc_num; ++i) {
        SrmDocInfo *doc = bcinfo->get_doc_at(i);
        if (doc == NULL) {
            continue;
        }
        // enought
        // F_PARAM strategy
        if (ac_sta.act_keep_doc + ac_sta.topN_top > result_keep_num + ac_sta.uid_social_num_buf) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN KEEP_NUM_OK :"PRI64", :%d]", doc->docid, i);
            info->bcinfo.del_doc(doc);
            continue;
        }
        // F_PARAM strategy
        // check time when no recall_qi_low_doc
        if (ac_sta.recall_qi_low_doc <= 0 &&
            ac_sta.recall_qi_low_doc_week <= 0 &&
            ac_sta.recall_qi_low_doc_month <= 0 &&
            doc->rank >= ac_sta.top_n_time) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN >top_n_time :"PRI64", :%d]", doc->docid, i);
            info->bcinfo.del_doc(doc);
            ac_sta.left_time_bad++;
            continue;
        }
        AcExtDataParser attr(doc);
        uint64_t uid = GetDomain(doc);
        // F_PARAM filter
        // check black
        //if (!bc_check_black && check_invalid_doc_mid(info, doc, uid, attr, ac_sta, true)) {
        //    continue;
        //}
		//else if (bc_check_black)
		//{
		//	if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
		//}
		if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
        bool is_good_uids = false;
		int white_type = PassByWhite(attr);
        if (white_type) {
            is_good_uids = true;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [act:"PRIU64","PRIU64",%d,USER_TYPE_GOOD_UID_OR_WHITE_WEIBO]", doc->docid, uid, white_type);
        }
		/*
        if (*good_uids && good_uids->Search(uid) != -1) {
            is_good_uids = true;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [act:"PRIU64","PRIU64",good_uids_files_good]", doc->docid, uid);
        }
		*/
        uint64_t docattr = doc->docattr;
		int is_social = getSocialRelation(info, uid, docattr, social);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [>act_index act:"PRIU64","PRIU64",%d:%d, good:%d,social:%d]",
            doc->docid, uid, doc->bcidx, doc->bsidx, is_good_uids, is_social);
        // F_PARAM filter
        if (is_good_uids || (is_social == 1)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [good_uid or social pass black:"PRIU64","PRIU64",%d:%d]",
                doc->docid, uid, doc->bcidx, doc->bsidx);
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [<1000 social or vip keep:"PRI64", index:%d,vip:%d,social:%d]",
                    doc->docid, i, is_good_uids, is_social);
            ac_sta.act_keep_doc++;
            int degree = DEGREE_15_GOOD_PASS_FILTER;
            if (is_good_uids) {
                degree = DEGREE_100_VIP;
            }
            if (is_social) {
                degree = DEGREE_200_SOCIAL;
            }
			doc->filter_flag = 0;
            doc->filter_flag |= degree<<20;
            //update_repost_fake_cluster_struct(info, doc, attr, ac_sta);
            if (!update_repost_fake_cluster_struct(info, doc, attr, ac_sta)) {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG outTopN update_repost :"PRIU64",dup del:"PRIU64", :%d]",
                        doc->docid, attr.get_dup(), i);
                info->bcinfo.del_doc(doc);
                ac_sta.update_repost++;
                continue;
            }
            int dup_result = 0;
            is_dup_update_doc(info, doc, attr, ac_sta, true, dup_result);
            continue;
        } else {
            int source = attr.get_digit_source();
			if(check_manual_black_uids(info, doc, attr, ac_sta, uid, manual_black_uids, delay_time_qi20, true)) {
				continue;
			}
            if (!bc_check_black && check_invalid_doc(info, doc, uid, attr, check_sandbox, delay_time, delay_time_qi20, ac_sta,
                                  mask_mids, manual_black_uids, black_uids, plat_black,
                                  suspect_uids, source, black_dict, true)) {
                continue;
            }
        }
        // check bad
        // F_PARAM strategy
		int granularity = get_granularity_qi(attr.get_qi());
        if (degree_is_del(info, doc, granularity)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN degree_del :"PRI64", :%d]", doc->docid, i);
            info->bcinfo.del_doc(doc);
            ac_sta.left_bad++;
            continue;
        }
        // for repost must > last_cluster_time
        // F_PARAM strategy
        if (CheckForwardClusterDel(info, attr, ac_sta, doc)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN repost_cluster t:%u > cluster_t:"PRIU64",dup del:"PRI64", :%d]",
                    doc->rank, ac_sta.top_cluster_time, doc->docid, i);
            info->bcinfo.del_doc(doc);
            ac_sta.left_cluser_time_error++;
            continue;
        }
        if(ac_sta.check_pic_source &&
           (ac_sta.topN_top+ac_sta.act_keep_doc) < ac_sta.cluster_first_page_num_after_cluster) {
            ac_sta.qualified_num++;
            uint64_t digit_attr = attr.get_digit_attr();
            hot_cheat_check_addinfo(info, digit_attr,
                        (ac_sta.topN_top+ac_sta.act_keep_doc), ac_sta);
            if (CheckPicSource(info, doc, ac_sta, attr, digit_attr)) {
                ac_sta.degree_0++;
                DelDocAddDupPrintLog(info, doc, attr, "source_check_failed", false, ac_sta);
                continue;
            }
        }
        // for update repost cluster_struct
        // F_PARAM strategy
        if (!update_repost_fake_cluster_struct(info, doc, attr, ac_sta)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN update_repost :"PRIU64",dup del:"PRIU64", :%d]",
                    doc->docid, attr.get_dup(), i);
            info->bcinfo.del_doc(doc);
            ac_sta.update_repost++;
            continue;
        }
        // check dup
		doc->filter_flag = 0;
        int degree = 0;
        // validfans need in is_bad_blue_vip_doc <= doc_is_bad_result
        // update_validfans_doc(info, doc, uid, uids_validfans_week, uids_validfans_hour);
        // check low and recall
        // F_PARAM strategy
        int invalid = is_invalid_topN_left_doc(info, doc, attr, ac_sta);
        if (invalid) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [<1000 > top_n invalid:%d set flag:"PRI64", :%d]", invalid, doc->docid, i);
            degree = DEGREE_8_RECALL;
            doc->filter_flag = 0;
            doc->filter_flag |= degree << 20;
            ac_sta.left_recall++;
            continue;
        }
        //doc_num - i < ac_sta.too_few_limit_doc_num &&
        // F_PARAM strategy
        int dup_result = 0;
        if (is_dup_update_doc(info, doc, attr, ac_sta, true, dup_result)) {
            if (!dup_result && ac_sta.need_recall_dup_num > 0) {
                degree = DEGREE_6_RECALL_LOW_DUP;
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [RECALL_LOW_DUP_BETTER 3, d:%d :"PRI64", t:%u]",
                        degree, doc->docid, doc->rank);
            } else {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG outTopN dup:"PRI64", :%d]", doc->docid, i);
                info->bcinfo.del_doc(doc);
                ac_sta.left_dup++;
                continue;
            }
        }
        // F_PARAM strategy
        // for recall
        if (UpdateCheckRecallData(info, ac_sta, doc)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [<1000 recall_doc_d keep:"PRI64", :%d]", doc->docid, i);
            ac_sta.act_keep_doc++;
            ac_sta.recall_doc_num++;
            if (degree == DEGREE_6_RECALL_LOW_DUP) {
                ac_sta.need_recall_dup_num--;
                ac_sta.recall_dup_num++;
            }
            continue;
        }
        // F_PARAM strategy
        if (doc->rank >= ac_sta.top_n_time) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN >top_n_time :"PRI64", :%d]", doc->docid, i);
            info->bcinfo.del_doc(doc);
            ac_sta.left_time_bad++;
            continue;
        }
        if (degree == DEGREE_6_RECALL_LOW_DUP) {
            ac_sta.need_recall_dup_num--;
            ac_sta.recall_dup_num++;
        }
        if (doc->rank < ac_sta.top_n_left_better_time) {
            ac_sta.top_n_left_better_time = doc->rank;
        }
        // 1: < top_n_time
        // 2: >= top_n_time, but need recall, so all keep
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [<1000 > top_n keep:"PRI64", d:%d,:%d]", doc->docid, degree, i);
        ac_sta.act_keep_doc++;
    }
    AC_WRITE_LOG(info, " [act_keep_doc:%d, top:%d, result_keep_num:%d]",
                ac_sta.act_keep_doc, ac_sta.topN_top, result_keep_num);
    // for recall
    // F_PARAM strategy
    if (ac_sta.act_keep_doc + ac_sta.topN_top < result_keep_num) {
        for (int i = ac_sta.act_index; i < doc_num; ++i) {
            SrmDocInfo *doc = bcinfo->get_doc_at(i);
            if (doc == NULL) {
                continue;
            }
            int degree = get_degree(doc);
            if (degree != DEGREE_8_RECALL) {
                continue;
            }
            if (ac_sta.act_keep_doc + ac_sta.topN_top > result_keep_num + ac_sta.uid_social_num_buf) {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG recall KEEP_NUM_ok :"PRI64", :%d]", doc->docid, i);
                info->bcinfo.del_doc(doc);
                continue;
            }
            if ((ac_sta.bc_input_num > ac_sta.min_num_skip_dup) &&
                (doc->rank >= ac_sta.top_n_time || doc->rank >= ac_sta.top_n_left_better_time)) {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG outTopN last >top_n_time(%d>%d):"PRI64":%d,%u > "PRIU64","PRIU64"]",
                        ac_sta.bc_input_num, ac_sta.min_num_skip_dup,
                        doc->docid, i, doc->rank, ac_sta.top_n_time, ac_sta.top_n_left_better_time);
                info->bcinfo.del_doc(doc);
                ac_sta.left_time_bad++;
                continue;
            }
            doc->filter_flag = 0;
            AcExtDataParser attr(doc);
            if(ac_sta.check_pic_source &&
               (ac_sta.topN_top+ac_sta.act_keep_doc) < ac_sta.cluster_first_page_num_after_cluster) {
                ac_sta.qualified_num++;
                uint64_t digit_attr = attr.get_digit_attr();
                hot_cheat_check_addinfo(info, digit_attr,
                            (ac_sta.topN_top+ac_sta.act_keep_doc), ac_sta);
                if (CheckPicSource(info, doc, ac_sta, attr, digit_attr)) {
                    ac_sta.degree_0++;
                    DelDocAddDupPrintLog(info, doc, attr, "source_check_failed", false, ac_sta);
                    continue;
                }
            }
            int dup_result = 0;
            if (is_dup_update_doc(info, doc, attr, ac_sta, true, dup_result)) {
                if (!dup_result && ac_sta.need_recall_dup_num > 0) {
                    degree = DEGREE_6_RECALL_LOW_DUP;
                    ac_sta.need_recall_dup_num--;
                    ac_sta.recall_dup_num++;
                    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                            " [RECALL_LAST_LOW_DUP, d:%d :"PRI64", t:%u]",
                            degree, doc->docid, doc->rank);
                } else {
                    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                            " [DEL_LOG outTopN last dup:"PRI64", :%d]", doc->docid, i);
                    info->bcinfo.del_doc(doc);
                    ac_sta.left_dup++;
                    continue;
                }
            }
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [>1000 recall rank:%u cluster_time:"PRIU64","PRIU64" del:"PRI64", :%d]",
                    doc->rank, ac_sta.top_cluster_time, GET_FORWARD_FLAG(attr.get_digit_attr()),
                    doc->docid, i);
            ac_sta.recall_doc_num++;
            ac_sta.act_keep_doc++;
        }
    }
	info->bcinfo.refresh();
    doc_num = bcinfo->get_doc_num();
	bcinfo->sort(0, doc_num, my_time_cmp);
    // for uid dup, and socialtime limit
    // F_PARAM strategy
    ac_uid_dup_and_socialtime_limit(info, data, doc_num, ac_sta);
    AC_WRITE_LOG(info, " [> top_N: left_time_bad:%d, balck:%d, update_repost:%d, left_bad:%d"
                       ", left_dup:%d, left_recall:%d, uid_dup:%d, cluster_del:%d"
                       ", real recall:%d, repost root_mid_del:%d, is_dup_replace_del:%d] ",
                ac_sta.left_time_bad, ac_sta.black_num, ac_sta.update_repost, ac_sta.left_bad,
                ac_sta.left_dup, ac_sta.left_recall, ac_sta.del_uid_dup_num,
                ac_sta.repost_cluster_del_num, ac_sta.recall_doc_num, ac_sta.repost_root_mid_del_count,
                ac_sta.is_dup_replace_del);
	info->bcinfo.refresh();
    // last sort by time
	doc_num = bcinfo->get_doc_num();
    reset_rank_use_time(info,ranktype);
	bcinfo->sort(0, doc_num, my_time_cmp);
    int before_pic_num = doc_num;
/*    
    if (*uid_source_dict and *realtime_hotquery and realtime_hotquery->test(key))
    //if (*uid_source_dict) 
    {
        hot_cheat_check(info, ac_sta, doc_num, uid_source_dict);
        info->bcinfo.refresh();
    }
    */
    // F_PARAM strategy
    if (ac_sta.check_pic_source) {
        AC_WRITE_LOG(info, " [old_before_hot:pic:(%d,%d,%d) video:(%d,%d,%d) del:(%d,%d]",
                    ac_sta.original_pic_num, ac_sta.all_pic_num,
                    (ac_sta.all_pic_num - ac_sta.del_pic_num),
                    ac_sta.orig_video_num, ac_sta.all_video_num,
                    (ac_sta.all_video_num - ac_sta.del_video_num),
                    ac_sta.del_pic_num, ac_sta.del_other_num);
        AC_WRITE_LOG(info, " [hot_cheat_check]");
        int use_new_hot_pic_source = int_from_conf(USE_NEW_HOT_PIC_SOURCE, 1);
        if (use_new_hot_pic_source) {
            hot_cheat_check_v2(info, ac_sta, doc_num);
        } else {
            hot_cheat_check(info, ac_sta, doc_num);
        }
        info->bcinfo.refresh();
    }
	doc_num = bcinfo->get_doc_num();
    AC_WRITE_LOG(info, " [f_1:%d, f_2:%d, o_num:%d]", before_pic_num, doc_num, orig_num);
    
	//img_check_query == hot_query.txt
	if ((info->intention & INTENTION_HOTSEARCH_WORD) && *img_check_query && img_check_query->test(key))
	//if ( *img_check_query && img_check_query->test(key))
	{
		AC_WRITE_LOG(info, " [new source check duplication");
		drop_duplicated_new_source(info, ac_sta, doc_num);
		info->bcinfo.refresh();
	}
	int after_source_num = bcinfo->get_doc_num();
    AC_WRITE_LOG(info, " [before:%d, after:%d, o_num:%d]", doc_num, after_source_num, orig_num);
    return true;
}

bool ac_reranking_time(QsInfo *info) {
    if (info == NULL) {
        return false;
    }
    // xsort=time nofilter=0 us=1
    if (info->parameters.getXsort() != std::string("time") || info->parameters.getUs() != 1) {
        return false;
    }
    int dup = info->parameters.dup;
    Ac_statistic ac_sta;
    init_stat_nod(info, ac_sta);
    int resort_flag = 1;
    int dup_flag = 1;
    ac_set_flag(info, &resort_flag, &dup_flag);
    ac_sta.dup_flag = dup_flag;
    AC_WRITE_LOG(info, " [ac_reranking_time,dup:%d,%d]", ac_sta.dup_flag, dup);
    int bc_check_black = int_from_conf(BLACK_LIST_CHECK_SWITCH, 1);
    //{TODO:del
	auto_reference<SourceIDDict> black_dict("source_id.dict");
	auto_reference<ACLongSet> mask_mids("mask_mids");
	auto_reference<ACLongSet> manual_black_uids("uids_black_manual.txt");
	auto_reference<DASearchDictSet> black_uids("uids_black");
	auto_reference<DASearchDictSet> plat_black("plat_black");
	auto_reference<DASearchDict> suspect_uids("suspect_uids");
    //}
    QsInfoBC *bcinfo = &info->bcinfo;
    int doc_num = bcinfo->get_doc_num();
    bool check_sandbox = true;
    time_t time_now = ac_sta.curtime;
    time_t delay_time = time_now - int_from_conf(SUSPECT_DELAY_TIME, 172800);
    time_t delay_time_qi20 = time_now - int_from_conf(QI20_DELAY_TIME, 604800);
    for (int i = 0; i < doc_num; i++) {
        SrmDocInfo *doc = bcinfo->get_doc_at(i);
        if(NULL == doc) {
            continue ;
        }
        AcExtDataParser attr(doc);
        uint64_t uid = GetDomain(doc);
        int source = attr.get_digit_source();
        // F_PARAM filter
        if (FilterDocattr(doc)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [DEL_LOG filter_docattr:"PRIU64","PRIU64"]", doc->docid, doc->docattr);
            info->bcinfo.del_doc(doc);
            continue;
        }
        // F_PARAM filter
		//if (bc_check_black)
		//{
		//	if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, true)) continue;
		//}
		//else
		//{
		//	if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
		//}
		// F_PARAM filter
		if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
		if(check_manual_black_uids(info, doc, attr, ac_sta, uid, manual_black_uids, delay_time_qi20, true)) {
			continue;
		}
		if (!bc_check_black && check_invalid_doc(info, doc, uid, attr, check_sandbox, delay_time,
					delay_time_qi20, ac_sta, mask_mids, manual_black_uids, black_uids,
					plat_black, suspect_uids, source, black_dict, false)) {
			continue;
		}
        int dup_result = 0;
        if (dup && is_dup_update_doc(info, doc, attr, ac_sta, true, dup_result)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                " [DEL_LOG dup:"PRIU64"]", doc->docid);
            info->bcinfo.del_doc(doc);
            continue;
        }
    }
    release_stat_nod(info, ac_sta);
    return true;
}


bool ac_reranking_update(QsInfo *info) {
	if (info == NULL) {
		return false;
	}
    // only social and us=1
	if (info->parameters.getXsort() != std::string("social") || info->parameters.getUs() != 1) {
        if (info->parameters.getXsort() == "social") {
            int ranktype = RankType(info);
            reset_rank_use_time(info,ranktype);
            info->bcinfo.sort(0, info->bcinfo.get_doc_num(), my_time_cmp);
            AC_WRITE_LOG(info, " [resort my_time_cmp] ");
        }
		return 0;
	}
    int keep_top_n = int_from_conf(KEEP_TOP_N, 100);
    int max_act_rerank_doc = int_from_conf(MAX_ACT_RERANK_DOC_DEGRADE, 1500);
    int cluster_top_buff = int_from_conf(CLUSTER_TOP_BUFF, 5);
    int degrade_state = RERANK_UPDATE_DEGRADE_STATE_NONE;
    int rerank_update_degrade_state2_per = int_from_conf(RERANK_UPDATE_DEGRADE_STATE2_PER, 30);
    int rerank_update_degrade_state1_per = int_from_conf(RERANK_UPDATE_DEGRADE_STATE1_PER, 20);
	if (ac_reranking_latency_ctrl.judge(rerank_update_degrade_state2_per)) {
        degrade_state = RERANK_UPDATE_DEGRADE_STATE_2;
        max_act_rerank_doc = int_from_conf(MAX_ACT_RERANK_DOC_DEGRADE, 1200);
        cluster_top_buff = int_from_conf(CLUSTER_TOP_BUFF_2, 2);
    } else if (ac_reranking_latency_ctrl.judge(rerank_update_degrade_state1_per)) {
        degrade_state = RERANK_UPDATE_DEGRADE_STATE_1;
        max_act_rerank_doc = int_from_conf(MAX_ACT_RERANK_DOC_DEGRADE, 1350);
        cluster_top_buff = int_from_conf(CLUSTER_TOP_BUFF_1, 3);
    }
    if (max_act_rerank_doc < 1000) {
        max_act_rerank_doc = 1000;
    }
	AC_WRITE_LOG(info, " [ac_reranking_update] [degrade_state:%d] ", degrade_state);
    AcRerankData data;
    data.keep_top_n = keep_top_n;
    data.max_act_rerank_doc = max_act_rerank_doc;
    data.cluster_top_buff = cluster_top_buff;
    data.degrade_state = degrade_state;
    data.degrade = false;
    //ac_reranking_update_base(info, data);
	
	Ac_statistic ac_sta;
	init_stat_nod(info, ac_sta);
	bool da_rank = get_auto_grades(info, ac_sta);
	int use_auto_ranking = int_from_conf(USE_AUTO_RANKING,1);

	if(use_auto_ranking && da_rank)
		ac_auto_reranking_base(info,data,ac_sta);
	else
    	ac_reranking_update_base(info,data,ac_sta);
    release_stat_nod(info, ac_sta);
    return true;
}

bool ac_reranking_degrade_update(QsInfo *info) {
	if (info == NULL) {
		return false;
	}
	if (info->parameters.getXsort() != std::string("social") || info->parameters.getUs() != 1) {
        if (info->parameters.getXsort() == "social") {
            int ranktype = RankType(info);
            reset_rank_use_time(info,ranktype);
            info->bcinfo.sort(0, info->bcinfo.get_doc_num(), my_time_cmp);
            AC_WRITE_LOG(info, " [resort my_time_cmp] ");
        }
		return 0;
	}
    int keep_top_n = int_from_conf(KEEP_TOP_N, 60);
    int max_act_rerank_doc = int_from_conf(MAX_ACT_RERANK_DOC_DEGRADE, 1000);
    int cluster_top_buff = int_from_conf(CLUSTER_TOP_BUFF, 1);
    int degrade_state = RERANK_UPDATE_DEGRADE_STATE_DEGRADE;
	AC_WRITE_LOG(info, " [ac_reranking_degrade_update] [degrade_state:%d] ", degrade_state);
    AcRerankData data;
    data.keep_top_n = keep_top_n;
    data.max_act_rerank_doc = max_act_rerank_doc;
    data.cluster_top_buff = cluster_top_buff;
    data.degrade_state = degrade_state;
    data.degrade = true;
    //ac_reranking_update_base(info, data);
	Ac_statistic ac_sta;
	init_stat_nod(info, ac_sta);
	bool da_rank = get_auto_grades(info, ac_sta);

	if(da_rank)
		ac_auto_reranking_base(info,data,ac_sta);
	else
    	ac_reranking_update_base(info, data,ac_sta);
    release_stat_nod(info, ac_sta);
    return true;
}

static void init_stat_nod(QsInfo *info, Ac_statistic &node)
{
	node.degree_0  = 0;//垃圾
	node.degree_11 = 0;//
	node.degree_14 = 0;
	node.degree_16 = 0;
	node.degree_17 = 0;
	node.black_num = 0;
	node.curtime = time(NULL);
	node.recall_qi_low_doc = int_from_conf(RECALL_QI_LOW_DOC_TIME_INTERVAL_NUM, 5);
	node.recall_time = node.curtime - int_from_conf(RECALL_QI_LOW_DOC_TIME_INTERVAL, (3600*24));
	node.recall_qi_low_doc_week = int_from_conf(RECALL_QI_LOW_DOC_TIME_INTERVAL_NUM_WEEK, 15);
	node.recall_time_week = node.curtime - int_from_conf(RECALL_QI_LOW_DOC_TIME_INTERVAL_WEEK, (3600*24*7));
	node.recall_qi_low_doc_month = int_from_conf(RECALL_QI_LOW_DOC_TIME_INTERVAL_NUM_MONTH, 35);
	node.recall_time_month =
        node.curtime - int_from_conf(RECALL_QI_LOW_DOC_TIME_INTERVAL_MONTH, (3600*24*30));
	node.dup_flag = 0;
	node.cluster_del_degree1 = 0;
	node.cluster_del = 0;
    node.is_cluster_repost = false;
    node.is_socialtime = false;
    node.min_repost_num = 1;
    node.repost_root_mid_del_count = 0;
    node.cluster_fwnum = int_from_conf(CLUSTER_FWNUM_VALID, 5);
    if ((info->intention & INTENTION_HOTSEARCH_WORD)) {
        node.cluster_root_valid_num = int_from_conf(CLUSTER_ROOT_VALID_NUM_HOT, 1);
        node.cluster_repost_valid_num = int_from_conf(CLUSTER_REPOST_VALID_NUM_HOT, 2);
    } else {
        node.cluster_root_valid_num = int_from_conf(CLUSTER_ROOT_VALID_NUM, 1);
        node.cluster_repost_valid_num = int_from_conf(CLUSTER_REPOST_VALID_NUM, 2);
    }
    node.is_dup_replace_del = 0;
    node.act_index = 0;
    node.degree_small = 0;
    node.doc_null = 0;
    node.top_n_time = (uint64_t)-1;
    node.top_n_left_better_time = (uint64_t)-1;
    node.too_few_limit_doc_num = int_from_conf(TOO_FEW_LIMIT_DOC_NUM, 100);
    node.top_time_num = 0;
    node.top_cluster_time = (uint64_t)-1;
    node.now_del_doc = 0;
    node.no_act_doc_num = 0;
    node.act_keep_doc = 0;
    node.act_del_time = 0;
    node.recall_doc_num = 0;
    node.uid_social_num_buf = int_from_conf(UID_SOCIAL_NUM_BUF, 60);
    node.left_time_bad = 0;
    node.left_bad = 0;
    node.left_dup = 0;
    node.left_cluser_time_error = 0;
    node.left_recall = 0;
    node.update_repost = 0;
    node.del_uid_dup_num = 0;
    node.uid_dup_actindex = 0;
    node.repost_cluster_del_num = 0;
    node.min_topN_num_for_recall = int_from_conf(MIN_TOPN_NUM_FOR_RECALL, 40);
    node.topN_top = 0;
    node.need_recall_dup_num = 0;
    node.recall_dup_num = 0;
    node.bc_input_num = 0;
    node.min_num_skip_dup = int_from_conf(MIN_NUM_SKIP_DUP, 60);
    node.cluster_validfans_num = int_from_conf(CLUSTER_VALIDFANS_NUM, 10000);
    node.fwnum_update_cluster_time = int_from_conf(FWNUM_UPDATE_CLUSTER_TIME, 1);
    node.uid_source_count = int_from_conf(UID_SOURCE_COUNT, 2);
    if (node.uid_source_count > MAX_UID_SOURCE_COUNT) {
        node.uid_source_count = MAX_UID_SOURCE_COUNT;
    }
    node.uid_source_time_pass = int_from_conf(UID_SOURCE_TIME_PASS, (3600*24*60));
    node.ac_use_source_dict = int_from_conf(AC_USE_SOURCE_DICT, 1);
    for (int i = 0; i < MAX_UID_SOURCE_COUNT; ++i) {
        node.uid_source_set[i] = NULL;
    }
    node.uid_source_suspect_dict = NULL;
    node.uid_source_mul_dict = NULL;
    node.uid_source_black_source = NULL;
    node.uid_source_white_source = NULL;
    node.pic_source = Resource::Reference<DASearchDictSet>("pic_source_normal");;
    node.zone_data = Resource::Reference<DASearchDictSet>("zone_source_normal");;
    // qi & (0x1 << 2), qi & (0x1 << 3)
    node.hot_cheat_check_qi_bits = int_from_conf(HOT_CHEAT_CHECK_QI_BITS, 12);
    for (int i = 0; i < MAX_INTENTION_STRATEGY_NUM; ++i) {
        node.inten_ploy_fwnum[i] = 0;
        node.inten_ploy_likenum[i] = 0;
    }
    node.inten_ploy_docnum_min = int_from_conf(INTENTION_STRATEGY_NUM, 100);
    node.inten_ploy_fwnum_filter = 0;
    node.inten_ploy_likenum_filter = 0;
    node.need_inten_ploy_filter = 0;
    node.need_category_filter = 0;

	node.expr_init_ok = true;
	node.super_grade = DEGREE_15_GOOD_PASS_FILTER;
	node.good_grade = DEGREE_9_GOOD;
	node.recall_good_grade = node.good_grade + 1;
	node.del_grade = DEGREE_0;
	node.low_grade = DEGREE_7_NOACT_DUP;

	node.default_exprs = "{\"info\":{\"200\":\"[social]==1\",\"100\":\"[good_uid]>0\",\"14\":\"[social]==2\",\"13\":\"([intention]&0x04) && (([fwnum]>=1) || ([likenum]>=10))\",\"12\":\"[valid_fwnum]==1\",\"11\":\"([intention]&0x04)&&(([docattr]&(0x1<<46))||([docattr]&(0x1<<47)))\"}}";
	node.default_thresholds =  "{\"threshold\":{\"13\":\"100\",\"11\":\"100\"}}";

	node.qualified_num = 0;
	node.original_pic_num = 0;
	node.orig_video_num = 0;
	node.all_pic_num = 0;
	node.all_video_num = 0;
	node.del_pic_num = 0;
	node.del_other_num = 0;
	node.del_video_num = 0;

	node.min_act_pic_cheat_num = int_from_conf(REALTIME_HOTQUERY_CHEAT_CHECK, 30);
	node.is_check_open = int_from_conf(REALTIME_HOTQUERY_CHEAT_CHECK, 1);
	node.first_page_num = int_from_conf(FIRST_PAGE_KEEP_NUM, 40);
	node.cluster_first_page_num_after_cluster = int_from_conf(CLUSTER_FIRST_PAGE_KEEP_NUM_AFTER_CLUSTER, 0);
	node.cluster_first_page_num_actcluster = int_from_conf(CLUSTER_FIRST_PAGE_KEEP_NUM_ACTCLUSTER, 40);
	node.cluster_first_page_num_degree = int_from_conf(CLUSTER_FIRST_PAGE_KEEP_NUM_DEGREE, 0);
    node.pic_cheat_delay_time = int_from_conf(PIC_CHEAT_DELAY_TIME, 172800);
    node.pic_cheat_act_weibo_count = int_from_conf(PIC_CHEAT_ACT_WEIBO_COUNT, 40);
    node.delay_time = node.curtime - node.pic_cheat_delay_time;
    // check 1 pic[0x1], 2 short_url[0x1<<1], 3 video[0x1<<4]
    node.check_digitattr = int_from_conf(HOT_PIC_SOURCE_CHECK_DIGITATTR, 147);
    node.new_hot_pic_source_pic_as_video = int_from_conf(NEW_HOT_PIC_SOURCE_PIC_AS_VIDEO, 1);
    node.pass_by_validfans = int_from_conf(HOT_PIC_SOURCE_PASS_BY_VALIDFANS, 5000);
	node.check_pic_source = 0;
	node.check_pic_source_dict_query = false;
	node.check_pic_source_dict = int_from_conf(CHECK_PIC_SOURCE_DICT, 1);
	node.check_pic_source_normal = int_from_conf(CHECK_PIC_SOURCE_NORMAL, 1);
	node.check_source_zone_normal_type = int_from_conf(CHECK_SOURCE_ZONE_NORMAL_TYPE, 0);
	node.check_source_zone_dict_type = int_from_conf(CHECK_SOURCE_ZONE_DICT_TYPE, 0);
	node.check_source_zone_normal_digit_type = int_from_conf(CHECK_SOURCE_ZONE_NORMAL_DIGIT_TYPE, -1);
	node.check_source_zone_dict_digit_type = int_from_conf(CHECK_SOURCE_ZONE_DICT_DIGIT_TYPE, -1);
    node.check_source_zone_qi_source = int_from_conf(CHECK_SOURCE_ZONE_QI_SOURCE, 4);
    node.check_source_zone_qi_or = int_from_conf(CHECK_SOURCE_ZONE_QI_OR, 8);
    node.check_source_zone_qi_zone = int_from_conf(CHECK_SOURCE_ZONE_QI_ZONE, 131072);
    node.check_source_zone_normal_use_acdict = int_from_conf(CHECK_SOURCE_ZONE_NORMAL_USE_ACDICT, 0);
    node.check_source_zone_dict_use_acdict = int_from_conf(CHECK_SOURCE_ZONE_DICT_USE_ACDICT, 1);
}

static void release_stat_nod(QsInfo *info, Ac_statistic &node) {
    if (node.pic_source) {
        node.pic_source->UnReference();
        AC_WRITE_LOG(info," [UnReference pic_source]");
    }
    if (node.zone_data) {
        node.zone_data->UnReference();
        AC_WRITE_LOG(info," [UnReference zone_data]");
    }
}

/*end*/
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
	ac_reranking_latency_ctrl.init("func.ac.reranking.cpu.sort", 0,
                int_from_conf(RERANKING_LATENCY_CTRL_TIMEOUT, 80),
                200, 10, 6);
	ac_reranking_degrade_latency_ctrl.init("func.ac.reranking.cpu.sort", 0,
                int_from_conf(RERANKING_LATENCY_DEGRADE_CTRL_TIMEOUT, 160),
                200, 10, 6);
	return;
}

static void __attribute__ ((destructor)) ac_sort_fini(void) {
	slog_write(LL_NOTICE, "ac_sort so destroyed");
	return;
}

static void make_atresult_weibo(QsInfo *info)
{
	if (info->params.key == NULL || strchr(info->params.key, ' ')) {
		return ;
	}
 
	int num = info->bcinfo.get_doc_num( );
	if (num <= 20) {
		return ;
	}

	const uint64_t UserTextFullMatchFlag = (0x4ll << (2 * 5));
	int count = 0;
	int num_at_result = 0;
	int num_atresult_exact_match = 0;
	int exact_match_result_top2page = 0;

	for (int i = 0; (i < 500) && (i < num); i++) {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if(NULL == doc) {
			continue ;
		}
		if (doc->docattr & (0x1ll << 43)) {
			num_atresult_exact_match++;
			if (i < 40)
			{
				exact_match_result_top2page++;
			}
		}
		if (doc->docattr & UserTextFullMatchFlag) {
			num_at_result++;
		}
		count++;
	}
	float pop_rate = 0;
	if (count != 0) {
	    pop_rate = (float)num_at_result/count;
	}

	float pop_rate1 = 0;
	if (num_at_result != 0) {
		pop_rate1 = (float)num_atresult_exact_match/num_at_result;
	}

	AC_WRITE_LOG(info, " {at_user[conunt:%d at:%d exactat:%d exactatp2:%d pop:%f pop1:%f]} ",
			count, num_at_result, num_atresult_exact_match,
			exact_match_result_top2page, pop_rate, pop_rate1);

	if ((pop_rate < 0.399) || (pop_rate1 > 0.50) || 
			(num_atresult_exact_match <= 3) || (exact_match_result_top2page <=2)) {
		return ;
	}

	int num_at_hint = 0;
	for (int j = 0; (j < 500) && (j < num); j++) {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(j);
		if (NULL == doc) {
			continue ;
		}

		if (doc->docattr & (0x1ll << 43)) {
			num_at_hint++;
			info->bcinfo.atresult_weibo.push_back(doc);
			if(num_at_hint >= 3) {
				break ;
			}
		}
	}
}

static void adjust_nick_partial_result(QsInfo *info)
{
	int num = info->bcinfo.get_doc_num( );
	if (num <= 20) {
		return ;
	}

	const uint64_t UserTextExactMatchFlag = (0x1ll << 43);
	const uint64_t UserTextMatchFlag = (0x1ll << (2*5));
	const uint64_t ContentMatchFlag = (0x1ll << 5);
	int count = 0;
	int num_at_result = 0;

	for (int i = 0; (i < 500) && (i < num); i++) {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if(NULL == doc) {
			continue ;
		}
		if (doc->docattr & UserTextMatchFlag) {
			num_at_result++;
		}
		count++;
	}
	float pop_rate = 0;
	if (count != 0) {
	    pop_rate = (float)num_at_result/count;
	}

	if(pop_rate >= 0.15)
	{
	    return ;
	}
    	//std::string query = info->parameters.getKey();

	for (int j = 0; (j < 500) && (j < num); j++) {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(j);
		if (NULL == doc) {
			continue ;
		}
		long nickHitFlag = (doc->docattr & UserTextMatchFlag);
		long nickExactHitFlag = (doc->docattr & UserTextExactMatchFlag);
		long contentHitFlag = (doc->docattr & ContentMatchFlag);
		if( nickHitFlag && !nickExactHitFlag && !contentHitFlag)
		{
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG nick_partial:"PRI64","PRI64","PRI64","PRI64","PRI64"]",
                    doc->docid, doc->docattr, nickHitFlag, nickExactHitFlag, contentHitFlag);
		    info->bcinfo.del_doc(doc);
		}
	}
}
static int adjust_nick_split(QsInfo *info) 
{
	int num = info->bcinfo.get_doc_num( );
	if (num < 100) {
		//too little result,skip
		return 0;
	}
	const uint64_t NickSplitMatchFlag = (0x1ll << 45);
	const uint64_t NickMatchFlag = (0x1ll << 10);
	const uint64_t ContentMatchFlag = (0x1ll << 5);
	SrmDocInfo *doc = NULL;
	int nickMatchNum = 0;
	int nickSplitMatchNum = 0;
	for (int i = 0; i < num; i++ ) {
		doc = info->bcinfo.get_doc_at(i);
		if (doc && (doc->docattr & NickMatchFlag)) {
		    nickMatchNum++;
			if (doc->docattr & NickSplitMatchFlag)
			{
				nickSplitMatchNum++;
			}
		}
	}
	int nickOneSentMtachNum = nickMatchNum - nickSplitMatchNum;
	float oneSentPop = 0;
	if (nickMatchNum != 0) {
	    oneSentPop = (float)nickOneSentMtachNum/nickMatchNum;
	}
    AC_WRITE_LOG(info," [nmn:%d nsn:%d osp:%f]",nickMatchNum, nickSplitMatchNum, oneSentPop);
	if(nickOneSentMtachNum < 5 || oneSentPop < 0.05 || ((nickMatchNum >= 100) && (oneSentPop < 0.70)))
	{
		return 0;
	} 
	int delete_num = 0;
	for (int i = 0; i < num; i++) {
		if( num - delete_num <= 100) {
			break ;
		}
		doc = info->bcinfo.get_doc_at(i);
		if(doc && (doc->docattr & NickSplitMatchFlag)) {
			if( doc->docattr & ContentMatchFlag )
			{
				continue ;
			}
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG nick_split:"PRI64","PRI64"]",
                    doc->docid, doc->docattr );
			info->bcinfo.del_doc(doc);
			delete_num++;
		}			
	}
    return 0;
}

static int adjust_movie_result(QsInfo *info)
{
    if(info->isStrongMovieQuery == 0)
    {
        return 0;
    }
    int  num = info->bcinfo.get_doc_num( );
    if(num < 10)
    {
        //too lirrle result,skip;
	return 0;         
    }
    const uint64_t MovieMatchFlag = (0x1ll << 46);//靠靠query
    const uint64_t MoviePartMatchFlag = (0x1ll << 47);//靠靠query
    SrmDocInfo *doc = NULL;
    int movieResultNum = 0;
    for(int i = 0; i < num; i++)
    {
        doc = info->bcinfo.get_doc_at(i);
	if( doc && (doc->bcidx == 0) && (doc->docattr & MovieMatchFlag))
	{
	    movieResultNum++;
	}
    }
    AC_WRITE_LOG(info," [movieNum:%d]", movieResultNum);
    if(movieResultNum < 5)
    {
        return 0;	
    }
    std::string query = info->parameters.getKey();
    for(int i = 0; i < num; i++)
    {
	doc = info->bcinfo.get_doc_at(i);
	if(doc && (doc->bcidx == 0) && ((doc->docattr & MovieMatchFlag) == 0) && ((doc->docattr & MoviePartMatchFlag) == 0))
	{
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [DEL_LOG adjust_movie :"PRI64","PRI64"]", doc->docid, doc->docattr);
	    info->bcinfo.del_doc(doc);
	}
    }
}
#endif

