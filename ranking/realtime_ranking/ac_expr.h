#pragma once
#include <stdint.h>

#define MAX_IND_NUM	1000

class SrmDocInfo;
class QsInfo;
enum{AN_ATTR, AN_RANK, AN_DOMAIN};
enum{XT_OPRT, XT_DIGIT, XT_ATTR};

//操作符
enum xop_type{
	XOP_PAR_L = 0,
	XOP_PAR_R = 1,

    XOP_ADD,
    XOP_SUB,
    XOP_MUTI,
    XOP_DIV,
    XOP_MOD,

    XOP_BIT_LSH,
    XOP_BIT_RSH,
    XOP_BIT_AND,
    XOP_BIT_OR,
    XOP_BIT_XOR,
    XOP_BIT_NOT,

    XOP_BOOL_LT,
    XOP_BOOL_LE,
    XOP_BOOL_GT,  // 15
    XOP_BOOL_GE,
    XOP_BOOL_EQ,
    XOP_BOOL_NE,

    XOP_BOOL_AND,
    XOP_BOOL_OR, // 20
    XOP_BOOL_NOT,

    XOP_COND,
    XOP_COMMA
};


#define GET_FUNC_TYPE(name) DOC_FUNC_TYPE__##name;

enum {
    DOC_FUNC_TYPE__NONE,
    DOC_FUNC_TYPE__digit_attr,
    DOC_FUNC_TYPE__status,
    DOC_FUNC_TYPE__level,
    DOC_FUNC_TYPE__digit_source,
    DOC_FUNC_TYPE__qi,
    DOC_FUNC_TYPE__user_type,
    DOC_FUNC_TYPE__verified,
    DOC_FUNC_TYPE__verified_type,
    DOC_FUNC_TYPE__dup,
    DOC_FUNC_TYPE__dup_cont,
    DOC_FUNC_TYPE__simhash2,
    DOC_FUNC_TYPE__fwnum,
    DOC_FUNC_TYPE__cmtnum,
    DOC_FUNC_TYPE__validfwnm,
    DOC_FUNC_TYPE__unvifwnm,
    DOC_FUNC_TYPE__idxlen,
    DOC_FUNC_TYPE__deflen,
    DOC_FUNC_TYPE__unrnum,
    DOC_FUNC_TYPE__likenum,
    DOC_FUNC_TYPE__whitelikenum,
    DOC_FUNC_TYPE__blacklikenum,
    DOC_FUNC_TYPE__identity_type,
    DOC_FUNC_TYPE__vip_user_type,
    DOC_FUNC_TYPE__special_state,
    DOC_FUNC_TYPE__valid_fans_level,
    DOC_FUNC_TYPE__audit,
    DOC_FUNC_TYPE__privacy,
    DOC_FUNC_TYPE__dup_cluster,
    DOC_FUNC_TYPE__white_weibo,
    DOC_FUNC_TYPE__reserved,
    DOC_FUNC_TYPE__uid,
    DOC_FUNC_TYPE__adv,
    DOC_FUNC_TYPE__reserved1,
	DOC_FUNC_TYPE__intention,
	DOC_FUNC_TYPE__docattr,
	DOC_FUNC_TYPE__good_uid,
	DOC_FUNC_TYPE__social,
	DOC_FUNC_TYPE__valid_fwnum
};

typedef struct{
	char *symbol;	//符号
	int num;	//操作数的个数
	int priority;	//运算符的优先级
}Op;

//运算符
static Op s_symbol[] = {
	{"(", 1, 0},
	{")", 0, 0},

	{"+", 2, 11},
	{"-", 2, 11},
	{"*", 2, 12},
	{"/", 2, 12},
	{"%", 2, 12},

	{"<<", 2, 10},
	{">>", 2, 10},
	{"&", 2, 7},
	{"|", 2, 5},
	{"^", 2, 6},
	{"~", 1, 13},

	{"<", 2, 9},
	{"<=", 2, 9},
	{">", 2, 9},
	{">=", 2, 9},
	{"==", 2, 8},
	{"!=", 2, 8},

	{"&&", 2, 4},
	{"||", 2, 3},
	{"!", 1, 13},

	{"?:", 3, 2},
	{",", 2, 1}
};


typedef struct{
	uint32_t type:2; //0-运算符，1-数字，2-变量
	uint64_t attr_type:2; //0-attr, 1-rank, 2-domain
    // for ac func_type
	uint32_t off:6;	//64内的起始bit
	uint32_t len:6;	//bit的长度
	uint32_t n:16;
}ind_t;

//逆波兰式
typedef struct{
	int ninds;
	int nargs;
}expr_t;

typedef struct{
    std::vector<ind_t> inds;
    std::vector<uint64_t> args;
} vec_expr_t;
//str为表达式，初始化表达式到buf里面，返回值为长度
//int expr_init(char *str, int str_len, void *buf, int maxlen);
int expr_init(QsInfo* info, char *str, int str_len, vec_expr_t& e);

//根据str表达式创建一个逆波兰式，使用malloc分配内存，最后需要调用free或者expr_destroy
int expr_create(QsInfo* info, char *str, int str_len, vec_expr_t& e);

//释放h内存，使用expr_init创建的不需要
void expr_destroy(void *h);

//对逆波兰式，进行计算，根据attr、rank、site
uint64_t expr_calc(QsInfo* info, const vec_expr_t& e, SrmDocInfo* doc);

//得到expr的大小
int expr_size(void *h);

//得到rank_start和rank_end的值
int expr_rank(void *h, uint32_t *rank_start, uint32_t *rank_end);
