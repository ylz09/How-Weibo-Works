/***************************************************************************
 * 
 * Copyright (c) 2011 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file ac_minibloguser.h
 * @author wangjian6(wangjian6@staff.sina.com.cn)
 * @date 2011/09/27 10:08:17
 * @version 1.0
 * @brief 
 * 搜人的宏定义
 *  
 **/

#ifndef _AC_MINIBLOGUSER_HOT_EXP_H
#define _AC_MINIBLOGUSER_HOT_EXP_H

//内存库结果扩展属性
typedef struct{
	uint64_t dup_cont:32;		//128:32 simhash签名
	uint64_t simhash2:32;		//160:32 simhash 2

	uint64_t fwnum:11;		//192:11      转发数
	uint64_t cmtnum:11;		//203:11      评论数
	uint64_t validfwnm:10;		//214:10  有效转发比例  取值范围[0,1000)
	uint64_t unvifwnm:10;		//224:10  无效转发比例
	uint64_t idxlen:8;		//234:8       微博正文索引长度
	uint64_t deflen:8;		//242:8
	uint64_t unrnum:6;		//250:6       

    uint64_t likenum : 11;   //256:11    赞数目
    uint64_t whitelikenum : 10;   //267:10
    uint64_t blacklikenum : 10;   //277:10
    uint64_t identity_type : 3; //287:3   微博分类
    uint64_t vip_user_type: 4;   //290:4
    uint64_t special_state : 8;   //294:8
	uint64_t valid_fans_level: 2;		//302:2
	uint64_t audit:2;		//304:2
	uint64_t privacy:2;		//306:2
	uint64_t dup_cluster: 1;		//308:1
	uint64_t white_weibo: 1;		//309:1
	uint64_t group_cheat: 1;	    //310:1
	uint64_t reserved: 9;		//311:9

    uint64_t uid_digit : 34;   //320:34
    uint64_t adv : 3;   //354:3
    uint64_t province : 6;   //357:6
    uint64_t city : 5;   //363:5
    uint64_t reserved1 : 16;   //368:16 for check have social_data must >= 2
}AttrExtra;

typedef struct{
        uint64_t digit_attr:8;          //0:8
        uint64_t status:5;              //8:5
        uint64_t level:3;               //13:3
        uint64_t digit_source:24;       //16:24
        uint64_t qi:24;                 //40:24

        uint64_t user_type:8;           //64:8
        uint64_t verified:1;            //72:1
        uint64_t verified_type:3;       //73:3
        uint64_t audit:2;               //76:2
        uint64_t privacy:2;             //78:2
        uint64_t dup:48;                //80:48

	uint64_t fwnum:11;		//128:11      转发数
	uint64_t cmtnum:11;		//139:11      评论数
	uint64_t validfwnm:10;		//150:10  有效转发比例  取值范围[0,1000)
	uint64_t unvifwnm:10;		//160:10  无效转发比例
	uint64_t idxlen:8;		//170:8       微博正文索引长度
    uint64_t identity_type : 3; //178:3   微博分类
    uint64_t blacklikenum : 10;   //181:10
	uint64_t reserved: 1;		//191:1

    uint64_t uid_digit : 34; //192:34    内容签名
    uint64_t likenum : 11;   //226:11    赞数目
    uint64_t whitelikenum : 10;   //237:10
	uint64_t unrnum:6;		//247:6       
    uint64_t special_state: 2;   //253:2
    uint64_t reserved1 : 1;   //255:1 // for check have social_data must >= 2

} Attr_history;

typedef struct{
	uint64_t fwnum:11;		//128:11      转发数
	uint64_t cmtnum:11;		//139:11      评论数
	uint64_t validfwnm:10;		//150:10  有效转发比例  取值范围[0,1000)
	uint64_t unvifwnm:10;		//160:10  无效转发比例
	uint64_t idxlen:8;		//170:8       微博正文索引长度
    uint64_t identity_type : 3; //178:3   微博分类
    uint64_t blacklikenum : 10;   //181:10
	uint64_t reserved: 1;		//191:1

    uint64_t uid_digit : 34; //192:34    内容签名
    uint64_t likenum : 11;   //226:11    赞数目
    uint64_t whitelikenum : 10;   //237:10
	uint64_t unrnum:6;		//247:6       
    uint64_t special_state: 2;   //253:2
    uint64_t reserved1 : 1;   //255:1 for check have social_data must >= 2

} AttrExtraHistory;

typedef struct{
        uint64_t digit_attr:8;          //0:8
        uint64_t status:5;              //8:5
        uint64_t level:3;               //13:3
        uint64_t digit_source:24;       //16:24
        uint64_t qi:24;                 //40:24

        uint64_t user_type:8;           //64:8
        uint64_t verified:1;		//72:1
        uint64_t verified_type:3;	//73:3
        uint64_t dup:52;		//76:52

	uint64_t fwnum:11;		//128:11      转发数
	uint64_t cmtnum:11;		//139:11      评论数
	uint64_t validfwnm:10;		//150:10  有效转发比例  取值范围[0,1000)
	uint64_t unvifwnm:10;		//160:10  无效转发比例
	uint64_t idxlen:8;		//170:8       微博正文索引长度
    uint64_t audit:2;       //178:2
    uint64_t blacklikenum : 10;   //180:10
    uint64_t privacy:2;            //190:2

    uint64_t uid_digit : 34; //192:34    内容签名
    uint64_t likenum : 11;   //226:11    赞数目
    uint64_t whitelikenum : 10;   //237:10
	uint64_t unrnum:3;		//247:3
	uint64_t adv:3;		//250:3
    uint64_t special_state: 2;   //253:2
    uint64_t reserved1 : 1;   //255:1 for check have social_data must >= 2

} Attr_history_2;
typedef struct{
	uint64_t fwnum:11;		//128:11      转发数
	uint64_t cmtnum:11;		//139:11      评论数
	uint64_t validfwnm:10;		//150:10  有效转发比例  取值范围[0,1000)
	uint64_t unvifwnm:10;		//160:10  无效转发比例
	uint64_t idxlen:8;		//170:8       微博正文索引长度
    uint64_t audit:2;       //178:2
    uint64_t blacklikenum : 10;   //180:10
    uint64_t privacy:2;            //190:2

    uint64_t uid_digit : 34; //192:34    内容签名
    uint64_t likenum : 11;   //226:11    赞数目
    uint64_t whitelikenum : 10;   //237:10
	uint64_t unrnum:3;		//247:3
	uint64_t adv:3;		//250:3
    uint64_t special_state: 2;   //253:2
    uint64_t reserved1 : 1;   //255:1 for check have social_data must >= 2
} AttrExtraHistory_2;

//rank
typedef struct {
	uint32_t digit_time; //64-95
} Rank;

#endif  // _AC_MINIBLOGUSER_HOT_EXP_H
