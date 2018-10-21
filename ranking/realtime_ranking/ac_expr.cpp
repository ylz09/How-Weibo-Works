#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include <vector>
#include "slog.h"
#include "ac_so.h"
#include "ac_expr.h"
#include "ac_utils.h"
#include "ac_hot_ext_data_parser.h"
#include "ac_sort.h"


#define PRI64 "%ld"
#define PRINT_RERANK_UPDATE_LOG 2
#define SHOW_DEBUG_NEWLINE 2


static uint64_t get_val_by_func(int type, QsInfo* info, SrmDocInfo* doc, const AcExtDataParser& attr) {
    switch (type) {
		case DOC_FUNC_TYPE__docattr:
			return doc->docattr;
		case DOC_FUNC_TYPE__intention:
			return info->intention;
		case DOC_FUNC_TYPE__valid_fwnum:
			return is_valid_forward(doc,attr,1);
		case DOC_FUNC_TYPE__good_uid:
	    	return PassByWhite(attr);
		case DOC_FUNC_TYPE__social:
	    	return getSocialRelation(info, doc);
        case DOC_FUNC_TYPE__uid:
            return GetDomain(doc);
        case DOC_FUNC_TYPE__fwnum:
            return attr.get_fwnum();
        case DOC_FUNC_TYPE__likenum:
            return attr.get_likenum();
        case DOC_FUNC_TYPE__verified:
            return attr.get_verified();
        case DOC_FUNC_TYPE__verified_type:
            return attr.get_verified_type();
        case DOC_FUNC_TYPE__digit_attr:
            return attr.get_digit_attr();
        case DOC_FUNC_TYPE__status:
            return attr.get_status();
        case DOC_FUNC_TYPE__level:
            return attr.get_level();
        case DOC_FUNC_TYPE__digit_source:
            return attr.get_digit_source();
        case DOC_FUNC_TYPE__qi:
            return attr.get_qi();
        case DOC_FUNC_TYPE__user_type:
            return attr.get_user_type();
        case DOC_FUNC_TYPE__dup:
            return attr.get_dup();
        case DOC_FUNC_TYPE__dup_cont:
            return attr.get_dup_cont();
        case DOC_FUNC_TYPE__simhash2:
            return attr.get_simhash2();
        case DOC_FUNC_TYPE__cmtnum:
            return attr.get_cmtnum();
        case DOC_FUNC_TYPE__validfwnm:
            return attr.get_validfwnm();
        case DOC_FUNC_TYPE__unvifwnm:
            return attr.get_unvifwnm();
        case DOC_FUNC_TYPE__idxlen:
            return attr.get_idxlen();
        case DOC_FUNC_TYPE__deflen:
            return attr.get_deflen();
        case DOC_FUNC_TYPE__unrnum:
            return 0; //attr.get_unrnum();
        case DOC_FUNC_TYPE__whitelikenum:
            return attr.get_whitelikenum();
        case DOC_FUNC_TYPE__blacklikenum:
            return attr.get_blacklikenum();
        case DOC_FUNC_TYPE__identity_type:
            return attr.get_identity_type();
        case DOC_FUNC_TYPE__vip_user_type:
            return 0;//attr.get_vip_user_type
        case DOC_FUNC_TYPE__special_state:
            return 0;//attr.get_special_state();
        case DOC_FUNC_TYPE__valid_fans_level:
            return attr.get_valid_fans_level();
        case DOC_FUNC_TYPE__audit:
            return attr.get_audit();
        case DOC_FUNC_TYPE__privacy:
            return attr.get_privacy();
        case DOC_FUNC_TYPE__dup_cluster:
            return 0;//attr.get_dup_cluster();
        case DOC_FUNC_TYPE__white_weibo:
            return attr.get_white_weibo();
        case DOC_FUNC_TYPE__reserved:
            return attr.get_reserved();
        case DOC_FUNC_TYPE__adv:
            return 0;//attr.get_adv();
        case DOC_FUNC_TYPE__reserved1:
            return 0;//attr.get_reserved1();
    }
    return 0;
}

static uint64_t get_func_type(char* name) {
    if (strcmp(name, "fwnum") == 0) {
        return GET_FUNC_TYPE(fwnum);
    } else if (strcmp(name, "likenum") == 0) {
        return GET_FUNC_TYPE(likenum);
    } else if (strcmp(name, "social") == 0) {
        return GET_FUNC_TYPE(social);
    } else if (strcmp(name, "good_uid") == 0) {
        return GET_FUNC_TYPE(good_uid);
    } else if (strcmp(name, "valid_fwnum") == 0) {
        return GET_FUNC_TYPE(valid_fwnum);
    } else if (strcmp(name, "docattr") == 0) {
        return GET_FUNC_TYPE(docattr);
    } else if (strcmp(name, "intention") == 0) {
        return GET_FUNC_TYPE(intention);
    } else if (strcmp(name, "verified") == 0) {
        return GET_FUNC_TYPE(verified);
    } else if (strcmp(name, "verified_type") == 0) {
        return GET_FUNC_TYPE(verified_type);
    } else if (strcmp(name, "white_weibo") == 0) {
        return GET_FUNC_TYPE(white_weibo);
    } else if (strcmp(name, "reserved") == 0) {
        return GET_FUNC_TYPE(reserved);
    } else if (strcmp(name, "adv") == 0) {
        return GET_FUNC_TYPE(adv);
    } else if (strcmp(name, "reserved1") == 0) {
        return GET_FUNC_TYPE(reserved1);
    } else if (strcmp(name, "digit_attr") == 0) {
        return GET_FUNC_TYPE(digit_attr);
    } else if (strcmp(name, "status") == 0) {
        return GET_FUNC_TYPE(status);
    } else if (strcmp(name, "level") == 0) {
        return GET_FUNC_TYPE(level);
    } else if (strcmp(name, "digit_source") == 0) {
        return GET_FUNC_TYPE(digit_source);
    } else if (strcmp(name, "qi") == 0) {
        return GET_FUNC_TYPE(qi);
    } else if (strcmp(name, "user_type") == 0) {
        return GET_FUNC_TYPE(user_type);
    } else if (strcmp(name, "dup") == 0) {
        return GET_FUNC_TYPE(dup);
    } else if (strcmp(name, "dup_cont") == 0) {
        return GET_FUNC_TYPE(dup_cont);
    } else if (strcmp(name, "simhash2") == 0) {
        return GET_FUNC_TYPE(simhash2);
    } else if (strcmp(name, "cmtnum") == 0) {
        return GET_FUNC_TYPE(cmtnum);
    } else if (strcmp(name, "validfwnm") == 0) {
        return GET_FUNC_TYPE(validfwnm);
    } else if (strcmp(name, "unvifwnm") == 0) {
        return GET_FUNC_TYPE(unvifwnm);
    } else if (strcmp(name, "idxlen") == 0) {
        return GET_FUNC_TYPE(idxlen);
    } else if (strcmp(name, "deflen") == 0) {
        return GET_FUNC_TYPE(deflen);
    } else if (strcmp(name, "unrnum") == 0) {
        return GET_FUNC_TYPE(unrnum);
    } else if (strcmp(name, "whitelikenum") == 0) {
        return GET_FUNC_TYPE(whitelikenum);
    } else if (strcmp(name, "blacklikenum") == 0) {
        return GET_FUNC_TYPE(blacklikenum);
    } else if (strcmp(name, "identity_type") == 0) {
        return GET_FUNC_TYPE(identity_type);
    } else if (strcmp(name, "vip_user_type") == 0) {
        return GET_FUNC_TYPE(vip_user_type);
    } else if (strcmp(name, "special_state") == 0) {
        return GET_FUNC_TYPE(special_state);
    } else if (strcmp(name, "valid_fans_level") == 0) {
        return GET_FUNC_TYPE(valid_fans_level);
    } else if (strcmp(name, "audit") == 0) {
        return GET_FUNC_TYPE(audit);
    } else if (strcmp(name, "privacy") == 0) {
        return GET_FUNC_TYPE(privacy);
    } else if (strcmp(name, "dup_cluster") == 0) {
        return GET_FUNC_TYPE(dup_cluster);
    } else if (strcmp(name, "uid") == 0) {
        return GET_FUNC_TYPE(uid);
    }
    return DOC_FUNC_TYPE__NONE;
}

#define INIT_BUF_SIZE	10000
//创建逆波兰式
int expr_create(QsInfo* info, char *str, int str_len, vec_expr_t& e){
	int len = expr_init(info, str, str_len, e);
    for (int i = 0; i < e.inds.size();++i) {
        if (e.inds[i].type == XT_DIGIT) {
            //printf("digit:%d\n", e.args[e.inds[i].n]);
        } else if (e.inds[i].type == XT_OPRT) {
            //printf("opt:%s\n", s_symbol[e.inds[i].n].symbol);
        } else if (e.inds[i].type == XT_ATTR) {
            //printf("attr:%d\n", e.args[e.inds[i].n]);
        }
    }
	if(len < 0){
		//printf("len:%d < 0\n", len);
        AC_WRITE_LOG(info, " [exp create failed:%d]", len);
		return 0;
	}
	return 1;
}

int expr_size(void *h){
	expr_t *expr = (expr_t*)h;
	return sizeof(*expr) + expr->ninds * sizeof(ind_t) + expr->nargs * sizeof(uint64_t);
}

//释放内存
void expr_destroy(void *h){
	free(h);
}

//运算符优先级
//(	0
//,	1
//?:	2
//||	3
//&&	4
//|	5
//^	6
//&	7
//== != 8
//> >= < <= 9
//>> <<	10
//+ -	11
//* / %	12
//! ~	13

#define EXPR_POP(e){\
	int n = s_symbol[e].priority;\
	while(op_num && (op_stack[op_num - 1] >> 8) >= n){\
		int _e = op_stack[op_num - 1] & 0xff;\
		if(_e){\
			ind_t ind = {0, 0, 0, 0, _e};\
			exp.inds.push_back(ind);\
            show_str += " " + std::string(s_symbol[_e].symbol); \
		} else if (n == 0) {\
            op_num --;\
            break; \
        } \
		op_num --;\
	}\
	if(e){\
		op_stack[op_num ++] = e | (n << 8);\
	}\
}

int expr_init(QsInfo* info, char *str, int str_len, vec_expr_t& exp) {
    // AC_WRITE_LOG(info, " [exp :%s]", str);
    std::string show_str;
	uint16_t op_stack[MAX_IND_NUM]; //操作符栈
	int op_num = 0;
	char *p = (char*)str, *end = p + str_len;
	while(p < end){
		switch(*p){
			case ' ':
			case '\t':
			case '\r':
			case '\n':
				break;
			case '(':
				op_stack[op_num ++] = XOP_PAR_L;
				break;
			case ')':
				EXPR_POP(XOP_PAR_L);
				break;
			case ',':
				EXPR_POP(XOP_COMMA);
				break;
			case '?':
				EXPR_POP(XOP_COND);
				break;
			case ':':
				EXPR_POP(XOP_COND);
				if(exp.inds[exp.inds.size()-2].n != XOP_COND){ //验证语法是否正确
					return -2;
				}
				break;
			case '|':
				if(p + 1 < end && p[1] == '|'){
					EXPR_POP(XOP_BOOL_OR);
					p ++;
				}else{
					EXPR_POP(XOP_BIT_OR);
				}
				break;
			case '&':
				if(p + 1 < end && p[1] == '&'){
					EXPR_POP(XOP_BOOL_AND);
					p ++;
				}else{
					EXPR_POP(XOP_BIT_AND);
				}
				break;
			case '^':
				EXPR_POP(XOP_BIT_XOR);
				break;
			case '=':
				if(p + 1 < end && p[1] == '='){
					EXPR_POP(XOP_BOOL_EQ);
					p ++;
				}else{
					return -3;
				}
				break;
			case '!':
				if(p + 1 < end && p[1] == '='){
					EXPR_POP(XOP_BOOL_NE);
					p ++;
				}else{
					EXPR_POP(XOP_BOOL_NOT);
				}
				break;
			case '>':
				if(p + 1 < end && p[1] == '='){
					EXPR_POP(XOP_BOOL_GE);
					p ++;
				}else if(p + 1 < end && p[1] == '>'){
					EXPR_POP(XOP_BIT_RSH);
					p ++;
				}else{
					EXPR_POP(XOP_BOOL_GT);
				}
				break;
			case '<':
				if(p + 1 < end && p[1] == '='){
					EXPR_POP(XOP_BOOL_LE);
					p ++;
				}else if(p + 1 < end && p[1] == '<'){
					EXPR_POP(XOP_BIT_LSH);
					p ++;
				}else{
					EXPR_POP(XOP_BOOL_LT);
				}
				break;
			case '+':
				EXPR_POP(XOP_ADD);
				break;
			case '-':
				EXPR_POP(XOP_SUB);
				break;
			case '*':
				EXPR_POP(XOP_MUTI);
				break;
			case '/':
				EXPR_POP(XOP_DIV);
				break;
			case '%':
				EXPR_POP(XOP_MOD);
				break;
			case '~':
				EXPR_POP(XOP_BIT_NOT);
				break;
			default :
				if(isdigit(*p)){
                    uint64_t val = strtoull(p, &p, 0);
					ind_t ind = {XT_DIGIT, 0, 0, 0, exp.args.size()};
                    exp.inds.push_back(ind);
                    exp.args.push_back(val);
                    char digit_str[64];
                    int len = snprintf(digit_str, 63, "%lld", val);
                    digit_str[len] = '\0';
                    show_str += " " + std::string(digit_str);
					continue;
				}else if(*p == '['){
                    p ++;
                    char name[64] = {0};
                    int namelen = 0;
                    bool get_type_ok = false;
                    while(p < end){
                        if (*p != ']') {
                            name[namelen] = *p;
                            namelen++;
                            p ++;
                        } else {
                            uint64_t func_type = get_func_type(name);
                            ind_t ind = {XT_ATTR, 0, func_type, 0, exp.args.size()};
                            exp.inds.push_back(ind);
                            show_str += " " + std::string(name);
                            exp.args.push_back(func_type);
                            get_type_ok = true;
                            break;
                        }
                    }
                    if (!get_type_ok) {
                        return -21;
                    }
				}else{
					return -12;
				}
		}
		p ++;
	}
	int i;
	for(i = op_num - 1; i >= 0; i --){
		ind_t ind = {0, 0, 0, 0, op_stack[i] & 0xff};
        exp.inds.push_back(ind);
        int e = op_stack[i] & 0xff;
        int n = op_stack[i] >> 8;
        show_str += " " + std::string(s_symbol[e].symbol);
	}
    AC_WRITE_LOG(info, " [exp :%s]", show_str.c_str());
	return exp.inds.size();
}


#define POP_STACK(o){\
	int n = s_symbol[o].num;\
	if(sl < n){\
		return 0;\
	}\
	if(n == 3){\
		arg1 = stack[--sl];\
		arg2 = stack[--sl];\
		arg3 = stack[--sl];\
	}else if(n == 2){\
		arg1 = stack[--sl];\
		arg2 = stack[--sl];\
	}else if(n == 1){\
		arg1 = stack[--sl];\
	}\
}

//逆波兰式的计算
uint64_t expr_calc(QsInfo* info, const vec_expr_t& e, SrmDocInfo* doc) {
    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
        " [filter exp :"PRI64"]", doc->docid);
	uint64_t stack[MAX_IND_NUM];
	int sl = 0;
	int num = e.inds.size();
	int i;
    AcExtDataParser attr(doc);
	for(i = 0; i < num; i ++){
		ind_t ind = e.inds[i];
		int xv = ind.n;
		uint64_t res;
		uint64_t arg1, arg2, arg3;
		switch(ind.type){
		case XT_ATTR:
            {
                int type = e.args[xv];
                uint64_t val = get_val_by_func(type, info, doc, attr);
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                            " [filter exp :"PRI64",attr:%d %lld]", doc->docid, type, val);
                stack[sl++] = val;
                break;
            }
		case XT_DIGIT:
            {
                uint64_t val = e.args[xv];
                // printf("digit:%lld\n", val);
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [filter exp :"PRI64",digit:%lld]", doc->docid, val);
                stack[sl++] = val;
                break;
            }
		case XT_OPRT:{
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [filter exp :"PRI64",opt:%d]", doc->docid, xv);
			switch(xv){
			//逻辑运算符
			case XOP_BOOL_LT: //<
				POP_STACK(XOP_BOOL_LT);
				res = (arg2 < arg1);
				break;
			case XOP_BOOL_LE: //<=
				POP_STACK(XOP_BOOL_LE);
				res = (arg2 <= arg1);
				break;
			case XOP_BOOL_GT: //>
				POP_STACK(XOP_BOOL_GT);
				res = (arg2 > arg1);
				break;
			case XOP_BOOL_GE: //>=
				POP_STACK(XOP_BOOL_GE);
				res = (arg2 >= arg1);
				break;
			case XOP_BOOL_EQ: //==
				POP_STACK(XOP_BOOL_EQ);
				res = (arg2 == arg1);
				break;
			case XOP_BOOL_NE: //!=
				POP_STACK(XOP_BOOL_NE);
				res = (arg2 != arg1);
				break;
			case XOP_BOOL_AND: //&&
				POP_STACK(XOP_BOOL_AND);
				res = (arg2 && arg1);
				break;
			case XOP_BOOL_OR: //||
				POP_STACK(XOP_BOOL_OR);
				res = (arg2 || arg1);
				break;
			case XOP_BOOL_NOT: //!
				POP_STACK(XOP_BOOL_NOT);
				res = !arg1;
				break;

			//算术运算符
			case XOP_ADD: //+
				POP_STACK(XOP_ADD);
				res = (arg2 + arg1);
				break;
			case XOP_SUB:	//-
				POP_STACK(XOP_SUB);
				res = (arg2 - arg1);
				break;
			case XOP_MUTI:	//*
				POP_STACK(XOP_MUTI);
				res = (arg2 * arg1);
				break;
			case XOP_DIV:	// /
				POP_STACK(XOP_DIV);
				res = (arg1 ? arg2 / arg1 : 0);
				break;
			case XOP_MOD:	// %
				POP_STACK(XOP_MOD);
				res = (arg1 ? arg2 % arg1 : 0);
				break;

			//位运算符
			case XOP_BIT_LSH: //<<
				POP_STACK(XOP_BIT_LSH);
				res = (arg2 << arg1);
				break;
			case XOP_BIT_RSH: //>>
				POP_STACK(XOP_BIT_RSH);
				res = (arg2 >> arg1);
				break;
			case XOP_BIT_AND: // &
				POP_STACK(XOP_BIT_AND);
				res = (arg2 & arg1);
				break;
			case XOP_BIT_OR:	// |
				POP_STACK(XOP_BIT_OR);
				res = (arg2 | arg1);
				break;
			case XOP_BIT_XOR:	// ^
				POP_STACK(XOP_BIT_XOR);
				res = (arg2 ^ arg1);
				break;
			case XOP_BIT_NOT:	//~
				POP_STACK(XOP_BIT_NOT);
				res = ~arg1;
				break;

			//其他运算符
			case XOP_COND:	//? :
				POP_STACK(XOP_COND);
				res = arg3 ? arg2 : arg1;
				break;
			case XOP_COMMA:	//,
				POP_STACK(XOP_COMMA);
				res = arg1;
				break;
			default:
				return 0;
			}
			stack[sl++] = res;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [filter exp :"PRI64",val:%d,i:%d]", doc->docid, res, sl-1);
		}
		}
	}
	return sl == 1 ? stack[0] : 0;
}

#ifdef SELF_TEST
int main(int argc, char **argv){
	if(argc < 2){
		printf("usage:%s expr\n", *argv);
		return -1;
	}
    vec_expr_t e;
	void *h = expr_create(argv[1], strlen(argv[1]), e);
	if(h == NULL){
		printf("h==NULL\n");
		return -1;
	}
	uint64_t attr = 0xffffffffffffffff;
    printf("\n\n\n");
	uint64_t val = expr_calc(e);
	printf("expr_calc:%lu\n", val);
	free(h);
	return 0;
}
#endif
