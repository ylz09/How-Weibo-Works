
#ifndef  __GS_ITERFACE_H_
#define  __GS_ITERFACE_H_

#pragma pack(push)
#pragma pack(4) //设置结构体4字节对齐

#include <ext_parse.h>
#include <ac_bc.h>

typedef int32_t gstime;

namespace gs
{
	/*
	 * @see 赞微博，其他赞，签到，文档下载并分享，听，评价，发布, 协同过滤
     *      INTEREST_MBLOG : 基于用户关系的推荐
     *      TAG_RECOM : 基于标签的推荐
     *      REL_WEIBO : 基于微博在用户历史行为中共现关系的推荐
     *      SIM_WEIBO : 基于微博内容相似度的推荐
     *      TAG_EDIT  : 增加或修改某用户的兴趣标签
	 */
	typedef enum {COMMEND = 0, LIKE, SIGN, SHARE, LISTEN, COMMENT, PUBLISH, COLLABORATION, COLLABOUSC,
		COLLABOLAV, INTEREST_MBLOG, TAG_RECOM, REL_WEIBO, SIM_WEBO, TAG_EDIT, TAG_SEARCH, NACTION} action_kinds;
    

	/*
	 * @brief 非关系的协同过滤
	 */
	inline static bool is_collaborate(int type)
	{
		return (type == COLLABORATION || type == COLLABOUSC || type == COLLABOLAV);
	}

	/*
	 * @brief 基于公司学校协同过滤
	 */
	inline static bool is_collabousc(const char * query)
	{
		if (query == NULL)
		{
			return false;
		}

		return (strncmp(query, "us_", 3) == 0 || strncmp(query, "uc_", 3) == 0);
	}

	/*
	 * @brief 文章、长微博、视频 等热门推荐榜
	 */
	inline static bool is_collabolav(const char * query)
	{
		if (query == NULL)
		{
			return false;
		}

		if (strcmp(query, "oc_publish_longmblog") == 0 || strcmp(query, "oc_forward_longmblog") == 0 || strcmp(query, "oc_recommend_longmblog") == 0)
		{
			return true;
		}

		if (strcmp(query, "oc_publish_article") == 0 || strcmp(query, "oc_forward_article") == 0 || strcmp(query, "oc_recommend_article") == 0)
		{
			return true;
		}

		if (strcmp(query, "oc_publish_video") == 0 || strcmp(query, "oc_forward_video") == 0 || strcmp(query, "oc_recommend_video") == 0)
		{
			return true;
		}

		return false;
	}

	inline static bool end_with(const char * key, const char * pattern)
	{
		if (key == NULL || pattern == NULL)
		{
			return false;
		}

		int klen = strlen(key);
		int plen = strlen(pattern);

		if (klen < plen || strcmp(key + klen - plen, pattern) != 0)
		{
			return false;
		}

		return true;
	}

	inline static bool is_long_objectid(const char * query)
	{
		if (strcmp(query, "oc_publish_video") == 0 || strcmp(query, "oc_forward_video") == 0 || strcmp(query, "oc_recommend_video") == 0)
		{
			return false;
		}

		if (end_with(query, "_image") || end_with(query, "_video") || end_with(query, "_docs"))
		{
			return true;
		}

		return false;
	}

	inline static bool is_need_abstract(const char * query)
	{
		if (strcmp(query, "oc_publish_article") == 0 || strcmp(query, "oc_forward_article") == 0 || strcmp(query, "oc_recommend_article") == 0)
		{
			return true;
		}

		return false;
	}

	inline static const char * id_to_pic(__uint128_t id, char * object_id, size_t size)
	{
		if (object_id == NULL || size == 0)
		{
			return "";
		}

		uint64_t high = id >> 64;
		uint64_t low = (uint64_t) id;
		snprintf(object_id, size, "1042018:%lx%lx", high, low);
		return object_id;
	}

	inline static __uint128_t pic_to_id(const char * pic)
	{
		if (pic == NULL)
		{
			return 0;
		}

		if (strncmp(pic, "1042018:", 8) == 0)
		{
			pic += 8;
		}

		const int PIC_LEN = 32;
		char normalpic[PIC_LEN+1] = { '0' };
		int piclen = strlen(pic);
		if (piclen > PIC_LEN)
		{
			return 0;
		}
		else
		{
			strcpy(normalpic+PIC_LEN-piclen, pic);
		}

		char sep = normalpic[PIC_LEN/2];
		normalpic[PIC_LEN/2] = '\0';
		uint64_t hight = strtoul(normalpic, NULL, 16);
		normalpic[PIC_LEN/2] = sep;
		uint64_t low = strtoul(normalpic+PIC_LEN/2, NULL, 16);
		__uint128_t temp = hight;
		temp <<= 64;
		temp |= low;
		return temp;
	}

	inline static __uint128_t byte_to_id(uint8_t * byte)
	{
		if (byte == NULL)
		{
			return 0;
		}

		__uint128_t id = *((uint64_t *) (byte));
		id <<= 64;
		id |= *((uint64_t *) (byte + 8));
		return id;
	}

	static const char * const gKindsName[] = {
		"commend",
		"like",
		"sign",
		"share",
		"listen",
		"comment",
		"publish",
		"collaboration",
		"collabousc",
		"collabolav",
		"interest_mblog",
        "tag_recom",
        "related_weibo",
        "similar_weibo",
        "tag_edit",
		NULL
	};

	struct general_param
	{
		int action;	/// 动作，对应action_kinds
		int cate;	/// page类型
		int type;	/// 对象类别
		int time;	/// 动作时间
		int place;	/// 地点

		int page;	/// 翻页参数
		int pagesize;

		int nuid;		/// 人物个数
		int nobject;	/// 目标对象个数
		int nreserve;	/// 保留个数

		int ext_size;	/// 扩展字段长度, warning，必须在设置好 uids,objects,reserves之后再设置，不可逆

		long data[0];	/// param list; uid list; object list; other list; ext field

		const long * get_uids() const
		{
			return data;
		}

		long * get_uids()
		{
			return data;
		}

		const long * get_objects() const
		{
			return data + nuid;
		}

		long * get_objects()
		{
			return data + nuid;
		}

		const long * get_reserve() const
		{
			return data + nuid + nobject;
		}

		long * get_reserve()
		{
			return data + nuid + nobject;
		}

		const char * get_ext() const
		{
			return (const char *) (data + nuid + nobject + nreserve);
		}

		char * get_ext()
		{
			return (char *) (data + nuid + nobject + nreserve);
		}

		size_t size() const
		{
			return offsetof(general_param,data) + sizeof(long)*(nuid+nobject+nreserve) + ext_size;
		}

		int ext_init()
		{
			return ::ext_init(get_ext());
		}

		int ext_set(const char * key, const void * value, int len)
		{
			::ext_set(get_ext(), key, value, len);
			ext_size = ::ext_size(get_ext());
			return ext_size;
		}

		int ext_get(const char * key, void * value, size_t vsize) const
		{
			if (ext_size == 0 || ext_size != ::ext_size((void *) get_ext()) || vsize == 0)
			{
				return 0;
			}

			int ret = ::ext_get_val((void *) get_ext(), key, value, vsize);
			if (ret >= int (vsize))
			{
				ret = vsize -1;
			}
			
			((char *) value)[ret] = '\0';

			return ret;
		}
	};

	struct attitude
	{
		uint64_t action_type:MAX_SR_AT_BITS;
		uint64_t mid:MAX_SR_MID_BITS;
		gstime t;
	};

	struct likedby
	{
		__uint128_t id;
		gstime t;
	};

	enum {TOPK = 1000};

    enum TAG_EDIT_TYPE
    {
        TYPE_INVALID = 0,
        TYPE_ADD_TAG = 1,
        TYPE_DEL_TAG = 2,
        TYPE_SEARCH_TAG = 3,
        TAG_EDIT_TYPE_NUM
    };
    enum GS_COMMAND
    {
        CMD_INVALID = 0,
        CMD_RELATED_USER_RECOM = 10,
        CMD_RELATED_WEIBO_RECOM = 11,
        CMD_TAG_RECOM = 12,
        CMD_USER_HISTORY = 13,
        CMD_WEIBO_INDEX = 14,
    };
}

#pragma pack(pop)//恢复对齐状态

#endif  //__GS_ITERFACE_H_

