This is a distinguish feature of Weibo real-time search. 
The idea is simple: if the followee forward or like a tweet, then this tweet has top rank of the follower's search result given that this tweet match the query at the same time. 
We use the shared memory, hash map, and priority queue to design and implement this search module. 

Let's go through this module just by reading the following code snip.
1. let's say uid is the id of the current search user
2. we can get all his followees, we represent them by uid list
3. then for each uid in the followee uid list, we search the shared memory to see there's any tweets, which is represented by mid in this exampel
4. we need hash the uid to find its address in the shared memory, if there's nothing we return nullprt
5. else we return the stuff in that memory block
6. now that we get the (mid, uid, time, action) we need, how do we return them the upper level?
7. if this tweet(mid) is not the map (key is mid, value is [action,uid,time]), we need insert it to the map
	if the results we got is larger than the max, then we need update the priority queue, which is a max heap
8. if this tweet is already in the map, then just update its newly action time
9. as a result, the items in teh 

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
