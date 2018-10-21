#include <string>
#include <ext_parse.h>
#include <slog.h>
#include <flyweight.h>
#include <resource.h>

#include "realtime_rw_index.h"
#include "socialrealtime_search.h"

DYN_ALGORITHMS(SocialRealtimeSearch);

SocialRealtimeSearch::SocialRealtimeSearch(Flyweight & flyweight):
	BaseAlgorithms(flyweight),
	m_flyweight(flyweight),
	m_membuf(sizeof(gs::attitude) * max_items/*MAX_ITEMS_KEEP*/)
{
	m_rtindex = NULL;
	follows.size = 0;
	follows.uids = NULL;
	follows.num = 0;
}

SocialRealtimeSearch::~SocialRealtimeSearch()
{
	if (follows.uids != NULL)
	{
		free(follows.uids);
		follows.uids = NULL;
	}
}

bool SocialRealtimeSearch::Init()
{
	const char * rtconfig = getParam("rtindex");
	if (rtconfig != NULL)
	{
		m_rtindex = Resource::getResource<RealTimeRWIndex> (rtconfig);
		if (m_rtindex == NULL)
		{
			slog_write(LL_FATAL, "Init realtime index[%s] error", rtconfig);
			return false;
		}
	}

	std::string social_config = CombineFullPath(getParam("social"));
	if (!m_social_client.Init(social_config.c_str()))
	{
		slog_write(LL_FATAL, "Init social pool error");
		return false;
	}

	follows.size = 8*(5000+1);
	follows.uids = (uint64_t *) malloc(follows.size);
	follows.num = 0;
	if (follows.uids == NULL)
	{
		slog_write(LL_FATAL, "malloc follows mem error");
		return false;
	}

	return true;
}

bool SocialRealtimeSearch::Run(const SizeString * input, SizeString * output)
{
	gettimeofday(&m_task_begin, NULL);

	if (!_parseParam(input))
	{
		BCResponse * response = (BCResponse *) output->m_szBuf;
		if (sizeof(*response) + sizeof(SocialDocValue) * m_num >= output->m_uLen)
		{
			slog_write(LL_FATAL, "output buff is not enough");
			return false;
		}

		memset(response, 0, sizeof(*response));
		response->start = m_start;
		response->status = (BC_STATUS_SOCIAL_FALG | BC_STATUS_COMPLETE);

		struct timeval cur;
		gettimeofday(&cur, NULL);
		response->usec = (cur.tv_sec - m_task_begin.tv_sec) * 1000000 + (cur.tv_usec - m_task_begin.tv_usec);

		return false;
	}

	_search();
	
	_sort();

	return _output(output);
}

bool SocialRealtimeSearch::_parseParam(const SizeString * input)
{
	if (input == NULL)
	{
		slog_write(LL_FATAL, "input is error");
		return false;
	}

	if (input->m_uLen < sizeof(BCRequest))
	{
		slog_write(LL_WARNING, "seqid:%ld input param length[%lu] is error", _getseqid(), input->m_uLen);
		return false;
	}

	BCRequest * request = (BCRequest *) input->m_szBuf;
	void * ext = GET_BCREQUEST_EXT(request);
	if (ext == NULL)
	{
		slog_write(LL_WARNING, "seqid:%ld input param has no ext info", _getseqid());
		return false;
	}

	if (ext_get_val(ext, "cuid", &m_uid, sizeof(m_uid)) <= 0 || m_uid == 0)
	{
		slog_write(LL_WARNING, "seqid:%ld input param has no cuid in ext", _getseqid());
		return false;
	}

	_getfollows();

	m_start = request->start;
	if (m_start < 0)
	{
		m_start = 0;
	}

	m_num = request->count;
	if (m_num >  max_items /*MAX_ITEMS_KEEP*/)
	{
		m_num = max_items /*MAX_ITEMS_KEEP*/;
	}

	return true;
}

int SocialRealtimeSearch::_getfollows()
{
	m_social_client.GetFollow(m_uid, &follows);

	if (follows.num <= 5000)
	{
		follows.uids[follows.num++] = m_uid;
	}

	return 0;
}

int SocialRealtimeSearch::_search()
{
	TopQueue toptimes;

	/// 查询实时数据
	if (m_rtindex != NULL)
	{
		for (int i = 0; i < follows.num;++i)
		{
			long uid = follows.uids[i];
			int len = 0;
			int ret = m_rtindex->Search(uid, m_membuf.getBuffer(), m_membuf.size(), &len);
			if (ret != 0 || len % sizeof(gs::attitude) != 0)
			{
				continue;
			}

			gs::attitude * atti = (gs::attitude *) m_membuf.getBuffer();
			for (int j = sizeof(gs::attitude); j <= len; j += sizeof(gs::attitude), ++atti)
			{
				_addItem(atti->mid, uid ,atti->t, atti->action_type, toptimes);
				/**< 内存索引没有按照rank排序，后续可以增加 */
			}
		}
	}

	return 0;
}

bool SocialRealtimeSearch::_output(SizeString * output)
{
	BCResponse * response = (BCResponse *) output->m_szBuf;
	if (sizeof(*response) + sizeof(SocialDocValue) * m_num >= output->m_uLen)
	{
		slog_write(LL_FATAL, "output buff is not enough");
		return false;
	}

	memset(response, 0, sizeof(*response));

	int j = 0;
	for (int i = m_start; i < int (m_sortResult.size()) && j < m_num;)
	{
	    //slog_write(LL_DEBUG, "muid:%llu, gzuid:%llu, mids:%llu ",m_uid,GET_SOCIAL_DOC_UID(m_sortResult[i]),m_sortResult[i].docid);
		response->sids[j++] = m_sortResult[i++];
	}

	response->start = m_start;
	response->count = j;
	response->total = m_sortResult.size();
	response->status = (BC_STATUS_SOCIAL_FALG | BC_STATUS_COMPLETE);

	struct timeval cur;
	gettimeofday(&cur, NULL);
	response->usec = (cur.tv_sec - m_task_begin.tv_sec) * 1000000 + (cur.tv_usec - m_task_begin.tv_usec);

	output->m_uLen = sizeof(*response) + sizeof(SocialDocValue) * j;

	return true;
}

