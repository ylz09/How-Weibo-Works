#ifndef  __AC_HOT_EXT_DATA_PARSER_H_
#define  __AC_HOT_EXT_DATA_PARSER_H_

#include "ac_srm_doc_info.h"
#include "ac_miniblog.h"
#include "ac_miniblog_hot_ext.h"
#include "ac_qs_info_bc.h"

/* --------------------------------------------------------------------------*/
/**
 * @brief 解析bc传递给ac的扩展数据字段
 *        新增扩展字段需要在此类中添加读取和解析接口，新增字段只能在AttrExtra或AttrExtraDisk
 *        的末尾追加
 */
/* ----------------------------------------------------------------------------*/
class AcExtDataParser
{
public:
    //构造函数
    AcExtDataParser(): _doc(NULL),_attr_extra(NULL), _attr_extra_len(0)
    {
    
    }
    AcExtDataParser(const SrmDocInfo* doc);
    void set_doc(const SrmDocInfo* doc);

    /* --------------------------------------------------------------------------*/
    /**
     * @brief 从扩展数据中解析出debug信息
     *
     * @Param bcinfo  
     *
     * @Returns   
     */
    /* ----------------------------------------------------------------------------*/
    static int get_debug_info(QsInfo *info, QsInfoBC *bcinfo, SrmDocInfo* doc);

    uint64_t get_digit_attr() const;

    uint64_t get_status() const;

    uint64_t get_level() const;

    uint64_t get_digit_source() const;

    uint64_t get_qi() const;

    //用户类型
    uint64_t get_user_type() const;
    
    //是否认证用户
    uint64_t get_verified() const;

    //认证用户类型
    uint64_t get_verified_type() const;

    uint64_t get_audit() const;

    uint64_t get_privacy() const;

    //用于去重的签名
    uint64_t get_dup() const;
    
    //da_eva计算的simhash签名高32位
    uint64_t get_dup_cont() const;
    uint64_t get_simhash2() const;


    //获取转发数
    uint64_t get_fwnum() const;
    uint64_t get_fwnum_internal() const;

    //获取评论数
    uint64_t get_cmtnum() const;
    uint64_t get_cmtnum_internal() const;

    //有效转发
    uint64_t get_validfwnm() const;

    //无效转发
    uint64_t get_unvifwnm() const;

    uint64_t get_idxlen() const;

    uint64_t get_deflen() const;

    uint64_t get_pad1() const;

    uint64_t get_cont_sign() const;

    //内存库扩展属性的保留字段
    uint64_t get_reserved() const;

    //获取赞个数
    uint64_t get_likenum() const;
    uint64_t get_likenum_internal() const;
    uint64_t get_whitelikenum() const;
    uint64_t get_blacklikenum() const;
    //微博分类
    uint64_t get_identity_type() const;
    uint64_t get_domain() const;
    uint64_t get_valid_fans_level() const;
	uint64_t get_white_weibo() const;
	uint64_t get_group_cheat() const;
	uint64_t get_province() const;
	uint64_t get_city() const;
    uint64_t get_uint64_1() const;
    uint64_t get_uint64_2() const;
    uint64_t get_uint64_3() const;
    uint64_t get_uint64_4() const;

private:
    const SrmDocInfo* _doc; //待处理的结果
    void* _attr_extra; //属性扩展字段的起始位置
    uint16_t _attr_extra_len; //属性扩展字段的长度

};
uint64_t GetDomain(const SrmDocInfo* doc);
int UpdataTimeOne(QsInfo* info, SrmDocInfo* doc, int inten_user_type, int curtime);
int UpdateFamousConcernInfo(QsInfo* info, SrmDocInfo* doc, int inten_user_type);
int PassByWhite(const AcExtDataParser & attr);
//adjust category info for ac
int adjust_category_result(QsInfo *info, int docnum, int isHot);

#endif
