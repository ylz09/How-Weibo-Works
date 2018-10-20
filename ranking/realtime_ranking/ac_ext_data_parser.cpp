#include "ac_g_conf.h"
#include "ac_ext_data_parser.h"

//获取attr中的属性，非扩展字段
//仅当Attr和Attr_disk中均有此字段的时候可以使用
#define GET_ATTR_VALUE(field) do\
{\
	if (_doc->attr_version) \
    {\
        return ((Attr*)(&_doc->attr))->field;\
    } else { \
        return ((Attr_history*)(&_doc->attr))->field;\
    }\
}while(0);

//获取内存库attr中的属性
#define GET_MEM_ATTR(field) return ((Attr*)(&_doc->attr))->field;

//获取硬盘库attr中的属性
//#define GET_DISK_ATTR(field) return ((Attr_disk*)(&_doc->attr))->field;


//获取扩展字段中的属性
//需要考虑后端兼容问题，若后端传递的属性长度小于ac数据结构的长度
//则缺少的属性置0
//注意：这种情况下会多一次memset和memcpy操作，需要关注性能
#define GET_MEM_ATTR_EXTRA(field) do\
{\
        if (_attr_extra == NULL || _attr_extra_len <= 0)\
        {\
            return 0;\
        }\
        else if (_attr_extra_len >= sizeof(AttrExtra))\
        {\
            return ((AttrExtra*)_attr_extra)->field;\
        }\
        else\
        {\
            AttrExtra tmp;\
            memset(&tmp, 0, sizeof(AttrExtra));\
            memcpy(&tmp, _attr_extra, _attr_extra_len);\
            return tmp.field;\
        }\
}while(0)

//获取一层库结果的扩展属性字段
#define GET_SMALL_ATTR_EXTRA(field) do\
{\
        if (_attr_extra == NULL || _attr_extra_len <= 0)\
        {\
            return 0;\
        }\
        else if (_attr_extra_len >= sizeof(AttrExtraSmall))\
        {\
            return ((AttrExtraSmall*)_attr_extra)->field;\
        }\
        else\
        {\
            AttrExtraSmall tmp;\
            memset(&tmp, 0, sizeof(AttrExtraSmall));\
            memcpy(&tmp, _attr_extra, _attr_extra_len);\
            return tmp.field;\
        }\
}while(0)


//get history extra attr
#define GET_HISTORY_ATTR_EXTRA(field) do\
{\
        if (_attr_extra == NULL || _attr_extra_len <= 0)\
        {\
            return 0;\
        }\
        else if (_attr_extra_len >= sizeof(AttrExtraHistory))\
        {\
            return ((AttrExtraHistory*)_attr_extra)->field;\
        }\
        else\
        {\
            AttrExtraHistory tmp;\
            memset(&tmp, 0, sizeof(AttrExtraHistory));\
            memcpy(&tmp, _attr_extra, _attr_extra_len);\
            return tmp.field;\
        }\
}while(0)

AcExtDataParser::AcExtDataParser(const SrmDocInfo* doc)
{
    set_doc(doc);
}
void AcExtDataParser::set_doc(const SrmDocInfo* doc)
{
    if (doc == NULL)
    {
        _attr_extra_len = 0;
        _attr_extra = NULL;
        _doc = NULL;
        return;
    }
    _doc = doc;
    if (doc->extra == NULL)
    {
        _attr_extra = NULL;
        _attr_extra_len = 0;
        return;
    }
    {
        _attr_extra_len = *(uint16_t*)(doc->extra);
        _attr_extra = (char*)doc->extra + sizeof(uint16_t);
    }
}

int AcExtDataParser::get_debug_info(QsInfoBC *bcinfo, SrmDocInfo *doc)
{
    if (doc == NULL || doc->extra == NULL)
    {
        return 0;
    }
    if (bcinfo->bc[doc->bcidx] == NULL)
    {
        return 0;
    }
    //扩展字段排列格式：
    //扩展属性长度，扩展属性，flagdata长度，flagdata，debug信息长度，debug信息
    int flag = bcinfo->bc[doc->bcidx]->flag;
    int extra_attr_len = *(uint16_t*)doc->extra;
    int head_len = sizeof(uint16_t) + extra_attr_len;
    if (flag & BS_FLAG_DATA)
    {
        //int flag_data_len = *(uint16_t*)((char*)doc->extra + head_len);
        head_len += sizeof(uint16_t);
        doc->docattr = *(uint64_t*) ((char*) doc->extra + head_len);
        head_len += sizeof(uint64_t);
    }
    else
    {
        doc->docattr = 0;
    }

    if ((flag & BS_FLAG_DEBUG) == 0)
    {
        return 0;
    }
    int dbg_len = *(uint16_t*) ((char*) doc->extra + head_len);
    if (dbg_len == 0 || dbg_len > doc->extra_len)
    {
        return 0;
    }
    //下面与旧代码保持一致，但似乎不太合理，为什么要覆盖掉debug_len
    char *p = (char *) doc->extra + head_len;
    memmove(p, p + 2, dbg_len);
    p[dbg_len] = 0;
    doc->debug = p;
    return 0;
}

uint64_t AcExtDataParser::get_digit_attr() const
{
    GET_ATTR_VALUE(digit_attr);
}

uint64_t AcExtDataParser::get_status() const
{
    GET_ATTR_VALUE(status);
}

uint64_t AcExtDataParser::get_level() const
{
    GET_ATTR_VALUE(level);
}

uint64_t AcExtDataParser::get_digit_source() const
{
    GET_ATTR_VALUE(digit_source);
}

uint64_t AcExtDataParser::get_qi() const
{
    GET_ATTR_VALUE(qi);
}

//用户类型
uint64_t AcExtDataParser::get_user_type() const
{
    GET_ATTR_VALUE(user_type);
}

//是否认证用户
uint64_t AcExtDataParser::get_verified() const
{
    GET_ATTR_VALUE(verified);
}

//认证用户类型
uint64_t AcExtDataParser::get_verified_type() const
{
    GET_ATTR_VALUE(verified_type);
}

uint64_t AcExtDataParser::get_audit() const
{
    GET_ATTR_VALUE(audit);
}

uint64_t AcExtDataParser::get_privacy() const
{
    GET_ATTR_VALUE(privacy);
}

//用于去重的签名
uint64_t AcExtDataParser::get_dup() const
{
    GET_ATTR_VALUE(dup);
}

//da_eva计算的simhash签名高32位
uint64_t AcExtDataParser::get_dup_cont() const
{
    //只有内存库有此属性
	if (_doc->attr_version)
    {
        GET_MEM_ATTR_EXTRA(dup_cont);
    }
    else
    {
        return 0;
    }
}

uint64_t AcExtDataParser::get_dup_url() const
{
    //只有内存库有此属性
	if (_doc->attr_version)
    {
        GET_MEM_ATTR_EXTRA(dup_url);
    }
    else
    {
        return 0;
    }
}

//获取转发数
uint64_t AcExtDataParser::get_fwnum_internal() const {
    //只有内存库和一层库有此属性
	if (_doc->attr_version)
    {
        GET_MEM_ATTR_EXTRA(fwnum);
    }
	else
	{
        GET_HISTORY_ATTR_EXTRA(fwnum);
    }
}

uint64_t AcExtDataParser::get_fwnum() const
{
    int fwnum = get_fwnum_internal();
    if (fwnum > 1024) {
        fwnum = (fwnum - 1024) * (fwnum - 1024);
    }
    return fwnum;
}

//获取评论数
uint64_t AcExtDataParser::get_cmtnum_internal() const {
    //只有内存库和一层库有此属性
	if (_doc->attr_version)
    {
        GET_MEM_ATTR_EXTRA(cmtnum);
    }
	else
	{
        GET_HISTORY_ATTR_EXTRA(cmtnum);
    }
}

uint64_t AcExtDataParser::get_cmtnum() const
{
    int cmtnum = get_cmtnum_internal();
    if (cmtnum > 1024) {
        cmtnum = (cmtnum - 1024) * (cmtnum - 1024);
    }
    return cmtnum;
}

//有效转发
uint64_t AcExtDataParser::get_validfwnm() const
{
    //只有内存库有此属性
	if (_doc->attr_version)
    {
        GET_MEM_ATTR_EXTRA(validfwnm);
    }
	else
	{
        GET_HISTORY_ATTR_EXTRA(validfwnm);
    }
}

//无效转发
uint64_t AcExtDataParser::get_unvifwnm() const
{
    //只有内存库有此属性
	if (_doc->attr_version)
    {
        GET_MEM_ATTR_EXTRA(unvifwnm);
    }
    else
    {
        return 0;
    }
}

uint64_t AcExtDataParser::get_idxlen() const
{
	if (_doc->attr_version)
    {
        GET_MEM_ATTR_EXTRA(idxlen);
	}
	else
	{
        GET_HISTORY_ATTR_EXTRA(idxlen);
    }
}

uint64_t AcExtDataParser::get_deflen() const
{
    //只有内存库有此属性
	if (_doc->attr_version)
    {
        GET_MEM_ATTR_EXTRA(deflen);
    }
    else
    {
        return 0;
    }
}

uint64_t AcExtDataParser::get_pad1() const
{
	return 0;
}

//内存库扩展属性的保留字段
uint64_t AcExtDataParser::get_reserved() const
{
    //只有内存库有此属性
	if (_doc->attr_version)
    {
        GET_MEM_ATTR_EXTRA(reserved);
	}
	else
	{
        GET_HISTORY_ATTR_EXTRA(reserved);
    }
}

uint64_t AcExtDataParser::get_cont_sign() const
{
    return _doc->domain;
}
uint64_t AcExtDataParser::get_identity_type() const
{
	if (_doc->attr_version)
    {
        GET_MEM_ATTR_EXTRA(identity_type);
    }
    else
    {
        return 0;
    }
}

uint64_t AcExtDataParser::get_likenum_internal() const {
    //只有内存库和一层库有此属性
	if (_doc->attr_version)
    {
        GET_MEM_ATTR_EXTRA(likenum);
	}
	else
	{
        GET_HISTORY_ATTR_EXTRA(likenum);
    }
}

uint64_t AcExtDataParser::get_likenum() const
{
    int likenum = get_likenum_internal();
    if (likenum > 1024) {
        likenum = (likenum - 1024) * (likenum - 1024);
    }
    return likenum;
}

//有效转发
uint64_t AcExtDataParser::get_whitelikenum() const
{
	if (_doc->attr_version)
    {
        //GET_MEM_ATTR_EXTRA(whitelikenum);
	}
	else
    {
        GET_HISTORY_ATTR_EXTRA(whitelikenum);
    }
}

uint64_t AcExtDataParser::get_blacklikenum() const
{
	if (_doc->attr_version)
    {
        //GET_MEM_ATTR_EXTRA(blacklikenum);
	}
	else
    {
        GET_HISTORY_ATTR_EXTRA(blacklikenum);
    }
}

uint64_t AcExtDataParser::get_domain() const
{
	if (_doc->attr_version)
    {
        GET_MEM_ATTR_EXTRA(uid_digit);
	}
	else
	{
        GET_HISTORY_ATTR_EXTRA(uid_digit);
    }
}

uint64_t GetDomain(const SrmDocInfo* doc) {
    AcExtDataParser attr(doc);
    return attr.get_domain();
} 
