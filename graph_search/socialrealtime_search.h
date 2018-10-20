/**
 * @file social_search.h
 * @version $Revision$   社会化实时序
 * @brief 
 *  
 **/


#ifndef  __SOCIAL_REALTIME_SEARCH_H_
#define  __SOCIAL_REALTIME_SEARCH_H_

#include <map>
#include <ext/hash_map>
#include <queue>
#include <base_algorithms.h>
#include <social_client.h>
#include <ac_bc.h>
#include <profile.h>

class SocialRealtimeSearch: public BaseAlgorithms
{

public:

	//enum {MAX_ITEMS_KEEP = 5000};

	SocialRealtimeSearch(Flyweight & flyweight);

	virtual ~SocialRealtimeSearch();

	virtual bool Init();

	virtual bool Refresh()
	{
		m_itemMap.clear();
		m_nitem = 0;
		m_sortResult.clear();
		m_membuf.refresh();

		m_start = 0;
		m_num = 0;

		follows.num = 0;

		return true;
	}

	virtual unsigned GetCMD() const
	{
		return 10;
	}

	virtual bool Run(const SizeString * input, SizeString * output);

	virtual bool DynLoad()
	{
		return true;
	}

protected:

	struct user_action
	{
		uint64_t action_type:MAX_SR_AT_BITS;
		uint64_t uid:MAX_SR_UID_BITS;
		uint32_t time;
		user_action(unsigned at = 0, uint64_t u = 0, uint32_t t = 0):action_type(at), uid(u), time(t) {}
	};

	//typedef std::map<uint64_t, user_action> LikeItemsMap;
	typedef __gnu_cxx::hash_map<uint64_t, user_action> LikeItemsMap;
	typedef std::priority_queue<time_t, std::vector<time_t>, std::greater<time_t> > TopQueue;

	struct cmp
	{
		int operator () (const SocialDocValue & a, const SocialDocValue & b) const
		{
		    return a.docid > b.docid ? true:false;
		    /*
			if (a.time > b.time)
			{
				return true;
			}

			return (a.time < b.time ? false : (a.docid > b.docid));
		    */
		}
	};

protected:
	
	virtual bool _parseParam(const SizeString * input);

	int _addItem(uint64_t mid, uint64_t uid, time_t t, unsigned at, TopQueue & times)
	{
		if (t == 0)
		{
			return -1;
		}

		if (m_nitem >= max_items /*MAX_ITEMS_KEEP*/ && t <= times.top())
		{
			return -2;
		}

		LikeItemsMap::iterator iter = m_itemMap.find(mid);
		if (iter == m_itemMap.end())
		{
			m_itemMap[mid] = user_action(at, uid, t);

			if (++m_nitem > max_items /*MAX_ITEMS_KEEP*/)
			{
				times.pop();
			}

			times.push(t);
		}
		else
		{
			if (iter->second.time < t)
			{
				iter->second = user_action(at, uid, t);
				times.push(t);
			}
		}

		return 0;
	}

	static int _getMaxItems()
	{
	    char filename[1024]; 
	    char* name = "/data1/minisearch/socialsearch/conf/rtindex.conf";
	    snprintf(filename, sizeof(filename), "%s", name);
	    FILE * fp = fopen(filename, "r");

	    if (fp == NULL)
	    {
		slog_write(LL_FATAL, "failed to open configure[%s]", filename); 
		return false; 
	    }
	    fclose(fp); 
	    
	    max_items = GetProfileInt(filename, "index", "maxItem", 5000);
	    slog_write(LL_DEBUG,"max items is : %d",max_items);
	    return max_items;
	}

	virtual int _sort()
	{
		if (m_nitem == 0)
		{
			return 0;
		}

		m_sortResult.reserve(m_nitem);

		LikeItemsMap::iterator iend = m_itemMap.end();
		for (LikeItemsMap::iterator iter = m_itemMap.begin(); iter != iend; ++iter)
		{
			SocialDocValue doc;
			doc.docid = iter->first;
			SET_SOCIAL_DOC_UID(doc,iter->second.uid);
			doc.time = iter->second.time;
			doc.action_type = iter->second.action_type;
			m_sortResult.push_back(doc);
		}

		std::sort(m_sortResult.begin(), m_sortResult.end(), cmp());

		return 0;
	}

	virtual int _getfollows();

	virtual int _search();

	virtual bool _output(SizeString * output);

	long _getseqid() const
	{
		return m_flyweight.get<long> ("daSeqid");
	}

protected:

	LikeItemsMap m_itemMap;
	int m_nitem;

	std::vector<SocialDocValue> m_sortResult;

    Flyweight & m_flyweight;

	RealTimeRWIndex * m_rtindex;	/**< 实时索引 */
	EString m_membuf;

	struct timeval m_task_begin;
	int m_start;
	int m_num;
	uint64_t m_uid;
	static int max_items;

	SocialClient<uint64_t> m_social_client;
	FollowList<uint64_t> follows;
};
int SocialRealtimeSearch::max_items = SocialRealtimeSearch::_getMaxItems();

#endif  //__SOCIAL_REALTIME_SEARCH_H_

