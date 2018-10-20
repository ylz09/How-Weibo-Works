#include "ac_g_conf.h"
#include "ac_global_conf.h"
#include "ac_hot_ext_data_parser.h"
const int CONCERN_NUM_THR = 5;

//获取attr中的属性，非扩展字段
//仅当Attr和Attr_disk中均有此字段的时候可以使用

#define MEM_ATTR_IDX_MAX 8

#define GET_ATTR_VALUE(field) do\
{\
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) { \
        return ((Attr*)(&_doc->attr))->field;\
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) { \
        return ((Attr_history_2*)(&_doc->attr))->field;\
    } else {\
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


#define GET_HISTORY_2_ATTR_EXTRA(field) do\
{\
        if (_attr_extra == NULL || _attr_extra_len <= 0)\
        {\
            return 0;\
        }\
        else if (_attr_extra_len >= sizeof(AttrExtraHistory_2))\
        {\
            return ((AttrExtraHistory_2*)_attr_extra)->field;\
        }\
        else\
        {\
            AttrExtraHistory_2 tmp;\
            memset(&tmp, 0, sizeof(AttrExtraHistory_2));\
            memcpy(&tmp, _attr_extra, _attr_extra_len);\
            return tmp.field;\
        }\
}while(0)


#define PERINT_SOCIAL_TIME_LOG 2
#define SHOW_DEBUG_NEWLINE 1
#define WHITE_VIP_TAG 0x20

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

int AcExtDataParser::get_debug_info(QsInfo *info, QsInfoBC *bcinfo, SrmDocInfo *doc)
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

    AC_WRITE_DOC_DEBUG_LOG(info, doc, PERINT_SOCIAL_TIME_LOG, SHOW_DEBUG_NEWLINE,
                    " [reserved1:%d,%d,%lu,%lu,:%d]",
                    doc->reserved1, flag, doc->docid, GetDomain(doc), extra_attr_len);
    // so reserved1 need >= 2
    if (doc->reserved1 == 2) {
        uint16_t dynamic_ext_len = *(uint16_t*) ((char*) doc->extra + head_len);
        head_len += sizeof(uint16_t);
        int data_len = dynamic_ext_len;
        if (data_len != sizeof(SocialInfo)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PERINT_SOCIAL_TIME_LOG, SHOW_DEBUG_NEWLINE,
                            " [error: data_len:%d > sizeof(SocialInfo):%lu]",
                            data_len, sizeof(SocialInfo));
        }
        if (unsigned(data_len) > sizeof(SocialInfo)) {
            data_len = sizeof(SocialInfo);
        }
        doc->has_social_head = 1;
        memcpy(&doc->social_head, (char *) doc->extra + head_len, data_len);
        head_len += dynamic_ext_len;
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PERINT_SOCIAL_TIME_LOG, SHOW_DEBUG_NEWLINE,
                        " [reserved1:2: size:%u, data:%u,%u, %u:%lu, %u:%lu, %u:%lu, %lu:%lu,%lu:%lu,%lu:%lu,%lu:%lu,%lu:%lu]",
                        dynamic_ext_len, doc->social_head.colonialinfo.colonial_time,
                        doc->social_head.colonialinfo.colonial_num,
                        doc->social_head.person[0].time, doc->social_head.person[0].uid,
                        doc->social_head.person[1].time, doc->social_head.person[1].uid,
                        doc->social_head.person[2].time, doc->social_head.person[2].uid,
						doc->social_head.comment[0].comment_id, doc->social_head.comment[0].uid,
						doc->social_head.comment[1].comment_id, doc->social_head.comment[1].uid,
						doc->social_head.comment[2].comment_id, doc->social_head.comment[2].uid,
						doc->social_head.comment[3].comment_id, doc->social_head.comment[3].uid,
						doc->social_head.comment[4].comment_id, doc->social_head.comment[4].uid
						);
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
    return 0;
    // GET_ATTR_VALUE(audit);
}

uint64_t AcExtDataParser::get_privacy() const
{
    return 0;
    /*
    // GET_ATTR_VALUE(privacy);
	if (_doc->attr_version)
    {
        GET_MEM_ATTR_EXTRA(privacy);
	}
	else
	{
        GET_ATTR_VALUE(privacy);
        // GET_HISTORY_ATTR_EXTRA(privacy);
    }
    */
}

//用于去重的签名
uint64_t AcExtDataParser::get_dup() const
{
    GET_ATTR_VALUE(dup);
}

//da_eva计算的simhash签名高32位
uint64_t AcExtDataParser::get_dup_cont() const
{
	// 只有内存库有此属性
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(dup_cont);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        return 0; //GET_HISTORY_2_ATTR_EXTRA(dup_cont);
    } else {
		return 0; //GET_HISTORY_ATTR_EXTRA(dup_cont);
    }
}

//da_eva计算的simhash签名低32位
uint64_t AcExtDataParser::get_simhash2() const
{
	// 只有内存库有此属性
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(simhash2);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        return 0; //GET_HISTORY_2_ATTR_EXTRA(simhash2);
    } else {
        return 0; //GET_HISTORY_ATTR_EXTRA(simhash2);
    }
}

//获取转发数
uint64_t AcExtDataParser::get_fwnum_internal() const {
    //只有内存库和一层库有此属性
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(fwnum);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        GET_HISTORY_2_ATTR_EXTRA(fwnum);
    } else {
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
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(cmtnum);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        GET_HISTORY_2_ATTR_EXTRA(cmtnum);
    } else {
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
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(validfwnm);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        GET_HISTORY_2_ATTR_EXTRA(validfwnm);
	} else {
        GET_HISTORY_ATTR_EXTRA(validfwnm);
    }
}

//无效转发
uint64_t AcExtDataParser::get_unvifwnm() const
{
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(unvifwnm);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        GET_HISTORY_2_ATTR_EXTRA(unvifwnm);
	} else {
        GET_HISTORY_ATTR_EXTRA(unvifwnm);
    }
}

uint64_t AcExtDataParser::get_idxlen() const
{
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(idxlen);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        GET_HISTORY_2_ATTR_EXTRA(idxlen);
	} else {
        GET_HISTORY_ATTR_EXTRA(idxlen);
    }
}

uint64_t AcExtDataParser::get_deflen() const
{
	// 只有内存库有此属性
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(deflen);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        return 0; //GET_HISTORY_2_ATTR_EXTRA(deflen);
	} else {
        return 0; //GET_HISTORY_ATTR_EXTRA(deflen);
    }
}

uint64_t AcExtDataParser::get_pad1() const
{
    return 0;
}

//内存库扩展属性的保留字段
uint64_t AcExtDataParser::get_reserved() const
{
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(reserved);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
       return 0;  //GET_HISTORY_2_ATTR_EXTRA(reserved);
	} else {
		return 0;//GET_HISTORY_ATTR_EXTRA(reserved);
    }
}

uint64_t AcExtDataParser::get_cont_sign() const
{

	return _doc->domain;
	/*
	if (_doc->attr_version == 2)
    {
        return 0;
    } else {
        return _doc->domain;
    }
	*/
}

uint64_t AcExtDataParser::get_identity_type() const
{
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(identity_type);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        return 0; // GET_HISTORY_2_ATTR_EXTRA(identity_type);
	} else {
        GET_HISTORY_ATTR_EXTRA(identity_type);
    }
}

uint64_t AcExtDataParser::get_likenum_internal() const {
    //只有内存库和一层库有此属性
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(likenum);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        GET_HISTORY_2_ATTR_EXTRA(likenum);
	} else {
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
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(whitelikenum);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        GET_HISTORY_2_ATTR_EXTRA(whitelikenum);
	} else {
        GET_HISTORY_ATTR_EXTRA(whitelikenum);
    }
}

uint64_t AcExtDataParser::get_blacklikenum() const
{
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(blacklikenum);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        GET_HISTORY_2_ATTR_EXTRA(blacklikenum);
	} else {
        GET_HISTORY_ATTR_EXTRA(blacklikenum);
    }
}

uint64_t AcExtDataParser::get_domain() const
{
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(uid_digit);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        GET_HISTORY_2_ATTR_EXTRA(uid_digit);
	} else {
	/*
	   else if (_doc->attr_version == 2) {
        return _doc->domain;
		
    } 
	*/
        GET_HISTORY_ATTR_EXTRA(uid_digit);
    }
}


uint64_t GetDomain(const SrmDocInfo* doc) {
    AcExtDataParser attr(doc);
    return attr.get_domain();
} 

uint64_t AcExtDataParser::get_valid_fans_level() const
{
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(valid_fans_level);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        return 0; //GET_HISTORY_2_ATTR_EXTRA(valid_fans_level);
	} else {
		return 0; //GET_HISTORY_ATTR_EXTRA(valid_fans_level);
    }
}
uint64_t AcExtDataParser::get_white_weibo() const
{
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(white_weibo);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        return 0; //GET_HISTORY_2_ATTR_EXTRA(white_weibo);
	} else {
        return 0; //GET_HISTORY_ATTR_EXTRA(white_weibo);
    }
}

uint64_t AcExtDataParser::get_group_cheat() const
{
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(group_cheat);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        return 0; //GET_HISTORY_2_ATTR_EXTRA(group_cheat);
	} else {
        return 0; //GET_HISTORY_ATTR_EXTRA(group_cheat);
    }
}

uint64_t AcExtDataParser::get_province() const
{
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(province);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        return 0;
	} else {
        return 0;
    }
}

uint64_t AcExtDataParser::get_city() const
{
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        GET_MEM_ATTR_EXTRA(city);
	} else if (_doc->attr_version && _doc->attr_version == MEM_ATTR_IDX_MAX) {
        return 0;
	} else {
        return 0;
    }
}


static bool UidDupJudge(std::set<uint64_t> & uid_set, uint64_t uid)
{
	std::set<uint64_t>::iterator it = uid_set.find(uid);
	if (it == uid_set.end())
	{
		uid_set.insert(uid);
		return false;
	}
	else
		return true;
}

inline SocialDocValue * findSocialDocValue(const uint64_t docid, SocialDocValue * social_follows_info, int num)         
{                                                                          
	register SocialDocValue *_p;                                           
	register SocialDocValue * start = social_follows_info;                 
	while(num){                                                            
		_p = start + (num >> 1);                                           
		if(docid == _p->docid){                                            
			return _p;                                                     
		}else if(docid < _p->docid){                                       
			start = _p + 1;                                                
			num = (num - 1) >> 1;                                          
		}else{                                                             
			num >>= 1;                                                     
		}                                                                  
	}                                                                      
	return NULL;                                                           
}                                                                          

int UpdateFamousConcernInfo(QsInfo* info, SrmDocInfo* doc, int inten_user_type)
{
//    __gnu_cxx::hash_map<uint64_t, SocialDocValue>::iterator it;
//	it = info->social_follows_info.find(doc->docid);
	SocialDocValue * sdv = findSocialDocValue(doc->docid, info->social_follows_info, info->social_doc_cnt);
	int uid_num_count = 0;
	std::set<uint64_t> dup_uids;
//    if (it != info->social_follows_info.end())
    if (sdv != NULL)
	{
		if (uid_num_count < CONCERN_NUM_THR)
		{
//			uint64_t uid = GET_SOCIAL_DOC_UID(it->second);
			uint64_t uid = GET_SOCIAL_DOC_UID(*sdv);
//			uint32_t time = (it->second).time;
			uint32_t time = sdv->time;
//			int type = (it->second).action_type;
			int type = sdv->action_type;
			if (uid && UidDupJudge(dup_uids, uid))
			{
				if (doc->docid == info->parameters.debugid) 
					AC_WRITE_LOG(info, "[uid dup, docid:%lu, time:%u, uid:%lu, type:%d, cat:%d]", doc->docid, time, uid, type, 3);
			}
			else
			{
				doc->famous_concern_info.person_info[uid_num_count].time = time;
				doc->famous_concern_info.person_info[uid_num_count].uid = uid;
				doc->famous_concern_info.person_info[uid_num_count].type = type;
				/* 2是名人   3是关注人 */
				doc->famous_concern_info.person_info[uid_num_count].cat = 3;
				if (doc->docid == info->parameters.debugid) 
					AC_WRITE_LOG(info, "[docid:%lu social,time:%u, uid:%lu, type:%d, cat:%d]", doc->docid, time, uid, type, 3);
				uid_num_count++;
			}
		}
    }
	if (doc->has_social_head)
	{
		for (int i = 0; i < 3; i++)
		{
			uint64_t uid = doc->social_head.person[i].uid;
			uint32_t time = doc->social_head.person[i].time;
			int type = doc->social_head.person[i].action_state;
			if (uid && UidDupJudge(dup_uids, uid))
			{
				if (doc->docid == info->parameters.debugid) 
					AC_WRITE_LOG(info, "[uid dup, docid:%lu, time:%u, uid:%lu, type:%d, cat:%d]", doc->docid, time, uid, type, 2);
				continue;
			}
			if (time && uid && uid_num_count < CONCERN_NUM_THR)
			{
				if (doc->social_head.person[i].social_field && !(inten_user_type & (0x1 << doc->social_head.person[i].social_field))) 
				{
					if (doc->docid == info->parameters.debugid) 
						AC_WRITE_LOG(info, "[special famous intention no match, docid:%lu, time:%u, uid:%lu, type:%d, cat:%d]", doc->docid, time, uid, type, 2);
					continue;
				}
				doc->famous_concern_info.person_info[uid_num_count].time = time;
				doc->famous_concern_info.person_info[uid_num_count].uid  = uid;
				doc->famous_concern_info.person_info[uid_num_count].type = type;
				doc->famous_concern_info.person_info[uid_num_count].cat  = 2;
				if (doc->docid == info->parameters.debugid) 
					AC_WRITE_LOG(info, "[docid:%lu social,time:%u, uid:%lu, type:%d, field:%lu, cat:%d]", doc->docid, time, uid, type, doc->social_head.person[i].social_field, 2);
				uid_num_count++;
			}
		}
	}
	return 0;
}



int UpdataTimeOne(QsInfo* info, SrmDocInfo* doc, int inten_user_type, int curtime) {
    // for follows
//    __gnu_cxx::hash_map<uint64_t, SocialDocValue>::iterator it;
//    it = info->social_follows_info.find(doc->docid);
	/* 时效性为4的不更新发布时间一小时外微博的社会化时间 */
	int timelevel_4_thr = int_from_conf(TIMELEVEL_4_THR, 1);
	if (info->fit_timeliness == 4 && curtime - doc->rank > timelevel_4_thr * 3600) {
		return 0;
	}
	SocialDocValue * sdv = findSocialDocValue(doc->docid, info->social_follows_info, info->social_doc_cnt);
    uint32_t follow_time = 0;
//    if (it != info->social_follows_info.end()) 
    if (sdv != NULL)
	{
//		follow_time = it->second.time;
		follow_time = sdv->time;
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PERINT_SOCIAL_TIME_LOG, SHOW_DEBUG_NEWLINE,
                        " [s_follow FIND:%lu,%lu,%d]",
                        doc->docid, GET_SOCIAL_DOC_UID(*sdv), follow_time);
    }
    // for time and vip from bs
    if (doc->has_social_head) {
        doc->time_old = doc->rank;
        if (doc->social_head.person[0].time || doc->social_head.colonialinfo.colonial_time) {
            // for famous
            if (doc->social_head.person[0].time) {
                doc->rank = doc->social_head.person[0].time;
                doc->social_head_output = SOCIAL_ACTION_HEAD_PERSON;
                if (inten_user_type) {
                    if (doc->social_head.person[0].social_field &&
                        !(inten_user_type & (0x1 << doc->social_head.person[0].social_field))) {
                        doc->rank = doc->time_old;
                        doc->social_head_output = SOCIAL_ACTION_HEAD_NONE;
                    }
                    int i = 1;
                    for (; i < 3; ++i) {
                        if (doc->social_head.person[i].social_field &&
                            !(inten_user_type & (0x1 << doc->social_head.person[i].social_field))) {
                            continue;
                        }
                        if (doc->rank < doc->social_head.person[i].time) {
                            doc->rank = doc->social_head.person[i].time;
                            doc->social_head_output = SOCIAL_ACTION_HEAD_PERSON;
                            doc->social_head.person[0].time = doc->social_head.person[i].time;
                            doc->social_head.person[0].uid = doc->social_head.person[i].uid;
                            doc->social_head.person[0].action_state =
                                doc->social_head.person[i].action_state;
                        }
                    }
                } else {
                    if (doc->social_head.person[0].social_field) {
                        doc->rank = doc->time_old;
                        doc->social_head_output = SOCIAL_ACTION_HEAD_NONE;
                    }
                }
            }
            if (doc->rank < doc->social_head.colonialinfo.colonial_time) {
                doc->rank = doc->social_head.colonialinfo.colonial_time;
                doc->social_head_output = SOCIAL_ACTION_HEAD_COLONIAL;
            }
        }
        if (doc->docid == info->parameters.debugid) {
            if (doc->rank != doc->time_old) {
                AC_WRITE_LOG(info, " [social buff:%lu, old:%u, now:%u, %d]",
                            doc->docid, doc->time_old, doc->rank, doc->social_head_output);
            } else {
                AC_WRITE_LOG(info, " [social have head,but not buff:%lu, old:%u, now:%u, %d]",
                            doc->docid, doc->time_old, doc->rank, doc->social_head_output);
            }
        }
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PERINT_SOCIAL_TIME_LOG, SHOW_DEBUG_NEWLINE,
                        " [social buff:%lu, old:%u, now:%u, %d]",
                        doc->docid, doc->time_old, doc->rank, doc->social_head_output);
    }
    if (doc->social_head_output || follow_time) {
        if (follow_time > doc->rank ||
            (follow_time == doc->rank &&
             SOCIAL_ACTION_HEAD_PERSON != doc->social_head_output)) {
            if (doc->time_old == 0) {
                doc->time_old = doc->rank;
            }
            // for follow set
            doc->has_social_head = 1;
            doc->rank = follow_time;
            doc->social_head_output = SOCIAL_ACTION_HEAD_FOLLOWS;
            doc->social_head.person[0].time = follow_time;
//            doc->social_head.person[0].uid = GET_SOCIAL_DOC_UID(it->second);
//            doc->social_head.person[0].action_state = (it->second).action_type;
            doc->social_head.person[0].uid = GET_SOCIAL_DOC_UID(*sdv);
            doc->social_head.person[0].action_state = (*sdv).action_type;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PERINT_SOCIAL_TIME_LOG, SHOW_DEBUG_NEWLINE,
                            " [social buff follow:%lu, old:%u, now:%u, %d]",
                            doc->docid, doc->time_old, doc->rank, doc->social_head_output);
        }
    }

    if ((info->parameters.gray_second || info->parameters.resocialtime) &&
        doc->time_old) {
        int resocialtime = 0;
        if (info->parameters.gray_second) {
            resocialtime = info->parameters.gray_second;
        } else {
            resocialtime = info->parameters.resocialtime;
        }
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PERINT_SOCIAL_TIME_LOG, SHOW_DEBUG_NEWLINE,
                        "[resocialtime check:%lu:old:%u,rank:%u,now:%u, diff:%d]",
                        doc->docid, doc->time_old, doc->rank, curtime,
                        curtime - doc->rank);
        if (doc->rank + resocialtime < curtime) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PERINT_SOCIAL_TIME_LOG, SHOW_DEBUG_NEWLINE,
                            "[resocialtime failed:%lu:old:%u,rank:%u,now:%u, diff:%d]",
                            doc->docid, doc->time_old, doc->rank, curtime,
                            curtime - doc->rank);
            doc->rank = doc->time_old;
            doc->social_head_output = 0;
            doc->has_social_head = 0;
        }
    }
	return 0;
}

uint64_t AcExtDataParser::get_uint64_1() const
{
    if (_attr_extra == NULL || _attr_extra_len <= 0) {
        return 0;
    }
    return *((uint64_t*)_attr_extra);
}

uint64_t AcExtDataParser::get_uint64_2() const
{
    if (_attr_extra == NULL || _attr_extra_len <= 0) {
        return 0;
    }
    return *((uint64_t*)_attr_extra + 1);
}

uint64_t AcExtDataParser::get_uint64_3() const
{
    if (_attr_extra == NULL || _attr_extra_len <= 0) {
        return 0;
    }
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        return *((uint64_t*)_attr_extra + 2);
    }
    return 0;
}

uint64_t AcExtDataParser::get_uint64_4() const
{
    if (_attr_extra == NULL || _attr_extra_len <= 0) {
        return 0;
    }
	if (_doc->attr_version && _doc->attr_version < MEM_ATTR_IDX_MAX) {
        return *((uint64_t*)_attr_extra + 3);
    }
    return 0;
}

int PassByWhite(const AcExtDataParser & attr) {
    uint64_t user_type = attr.get_user_type();
	uint64_t white_weibo = attr.get_white_weibo();
	if (user_type & WHITE_VIP_TAG) {
		return 1;
	} else if (white_weibo) {
		return 2;
	}
	return 0;
}

static inline int delDocs(QsInfo *info, std::vector<SrmDocInfo*> &docvec, int start, int end)
{
    int cnt = 0;
    for(int i = start; i < end; ++i)
    {
        AcExtDataParser attr(docvec[i]);
        if (PassByWhite(attr) == 0)
        {
            ++cnt;
            info->bcinfo.del_doc(docvec[i]);
            //borrow to force delete to test, will be removed before check in
            //AC_WRITE_LOG(info,"[DEL_LOG adjust_category :%ld, %ld]", docvec[i]->docid, docvec[i]->docattr);
            AC_WRITE_DOC_DEBUG_LOG(info, docvec[i], 2, SHOW_DEBUG_NEWLINE,
            " [DEL_LOG adjust_category :%ld, %ld]", docvec[i]->docid, docvec[i]->docattr);
        }
    }
    return  cnt;
}

/*
static inline bool DocComp(const SrmDocInfo *di, const SrmDocInfo *dj)
{
    return di->value > dj->value;
}
*/
std::vector<SrmDocInfo*> cat_second_cand;
std::vector<SrmDocInfo*> cat_non_matched;

int adjust_category_result(QsInfo *info, int nThreshold, int isHot)
{
    int num = info->bcinfo.get_doc_num();
    if (num < nThreshold || info->intention == 0) //no intention data
    {
        //too little result, simply return;
        return 0;         
    }
    const uint64_t FirstMatchFlag = (0x1ll << 34);// match first category
    const uint64_t SecondMatchFlag = (0x1ll << 35);//match second category
    
    int nFirstMatchedNum = 0;

    cat_second_cand.clear();
    cat_non_matched.clear();
    for (int i = 0; i < num; i++)
    {
        SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
        if (doc && ((doc->filter_flag & 0x01) == 0 && doc->value != 0))
        {
            if (doc->docattr & FirstMatchFlag)
                ++nFirstMatchedNum;
            else if (doc->docattr & SecondMatchFlag)
                cat_second_cand.push_back(doc);
            else
                cat_non_matched.push_back(doc);
        }
    }

    int nMatched = nFirstMatchedNum + cat_second_cand.size();
    //required number larger than total effective docs
    //simply return
    if (nMatched == 0 || nThreshold > nMatched + cat_non_matched.size())
        return 0;

    AC_WRITE_LOG(info," [Category: First Match %d ]", nFirstMatchedNum);
    AC_WRITE_LOG(info," [Category: Second Match %d ]", cat_second_cand.size());
    AC_WRITE_LOG(info," [Category: Threshold %d ]", nThreshold);
    int delcnt = 0;
    if (nFirstMatchedNum >= nThreshold) //good result larger than threshold 
    {
        delcnt += delDocs(info, cat_second_cand, 0, cat_second_cand.size());
        delcnt += delDocs(info, cat_non_matched, 0, cat_non_matched.size());
    }
    else if (nMatched >= nThreshold)//keep partial second matched 
    {
        int nKeeped = nThreshold - nFirstMatchedNum;
        //the docs are already sorted descently
        //std::nth_element(secondCandVec.begin(), secondCandVec.begin() + nKeeped, secondCandVec.end(), DocComp);
        delcnt += delDocs(info, cat_second_cand, nKeeped, cat_second_cand.size());
        delcnt += delDocs(info, cat_non_matched, 0, cat_non_matched.size());
    }
    else// filter no info, return
    {
        int nKeeped = nThreshold - nMatched;
        if (nKeeped < static_cast<int>(cat_non_matched.size()))
        {
            //std::nth_element(nonMatchedVec.begin(), nonMatchedVec.begin() + nKeeped, nonMatchedVec.end(), DocComp);
            delcnt += delDocs(info, cat_non_matched, nKeeped, cat_non_matched.size());
        }
    }
    AC_WRITE_LOG(info," [Category: %d docs deleted]", delcnt);

    if (isHot && delcnt > 0)
	    info->bcinfo.refresh();

    return 0;
}
