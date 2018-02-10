/**
* Get the intension of the strategy
* Intention_movie: the user want to get movie results
* 
**/
static bool get_strategy_inten(QsInfo *info, Ac_statistic &ac_data) {
    AC_WRITE_LOG(info, " [get_strategy_inten]");
    int doc_num = info->bcinfo.get_doc_num();
    int social_threshold = INT_MIN;
    if (!(info->strategy_info.ac_social_threshold.empty()))
    {
	social_threshold = atoi(info->strategy_info.ac_social_threshold.c_str());
        AC_WRITE_LOG(info, " [strategy.threshold:%d]", social_threshold );
    }
    social_threshold = social_threshold == INT_MIN ? ac_data.inten_ploy_docnum_min : social_threshold ;

    // for category filter
    if ((info->intention & INTENTION_MOVIE)) 
    {
		int movie_num = 0;
    	for (int i = 0; i < doc_num; ++i) 
		{
			SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
			if (doc == NULL) 
			{
				continue;
			}
			//match or part match movie flag
			if (doc->docattr & (0x1ll << 46) || doc->docattr & (0x1ll << 47))
			{
				movie_num++;
			}
		}
		if( movie_num >= social_threshold ) // means the mannually inserted results other than realtime
		{
			ac_data.need_category_filter = 1;
		}
		else
		{
			ac_data.need_category_filter = -1;
		}
		AC_WRITE_LOG(info, "[need category filter:%d, movie num:%d]",ac_data.need_category_filter, movie_num);
	}

    if (info->strategy_info.ac_social_fwnum.empty() && info->strategy_info.ac_social_likenum.empty()) {
        AC_WRITE_LOG(info, " [fwnum and liknum all empty]");
        return false;
    }
    if (!info->strategy_info.ac_social_fwnum.empty()) {
        std::string val = info->strategy_info.ac_social_fwnum;
        AC_WRITE_LOG(info, " [strategy.fwnum:%s]", val.c_str());
        std::size_t found = val.find(","), start = 0;
        int index = 0;
        while (found != std::string::npos) {
	    std::string value = val.substr(start, found);
            if (index >= MAX_INTENTION_STRATEGY_NUM) {
                break;
            }
            ac_data.inten_ploy_fwnum[index] = atoi(value.c_str());
            AC_WRITE_LOG(info, " [i:%d,fw:%d]", index, ac_data.inten_ploy_fwnum[index]);
            found++;
            start = found;
            index++;
            found = val.find(",", found);
        }
        if (start <= val.size() && index < MAX_INTENTION_STRATEGY_NUM) {
            string value = val.substr(start, val.size());
            int num = atoi(value.c_str());
            if (num > 0) {
                ac_data.inten_ploy_fwnum[index] = num;
                AC_WRITE_LOG(info, " [i:%d,fw:%d]", index, ac_data.inten_ploy_fwnum[index]);
            }
        }
    }
    if (!info->strategy_info.ac_social_likenum.empty()) {
        std::string val = info->strategy_info.ac_social_likenum;
        AC_WRITE_LOG(info, " [strategy.likenum:%s]", val.c_str());
        std::size_t found = val.find(","), start = 0;
        int index = 0;
        while (found != std::string::npos) {
            string value = val.substr(start, found);
            if (index >= MAX_INTENTION_STRATEGY_NUM) {
                break;
            }
            ac_data.inten_ploy_likenum[index] = atoi(value.c_str());
            AC_WRITE_LOG(info, " [i:%d,lk:%d]", index, ac_data.inten_ploy_likenum[index]);
            found++;
            start = found;
            index++;
            found = val.find(",", found);
        }
        if (start <= val.size() && index < MAX_INTENTION_STRATEGY_NUM) {
            string value = val.substr(start, val.size());
            int num = atoi(value.c_str());
            if (num > 0) {
                ac_data.inten_ploy_likenum[index] = num;
                AC_WRITE_LOG(info, " [i:%d,lk:%d]", index, ac_data.inten_ploy_likenum[index]);
            }
        }
    }
    int left_num[MAX_INTENTION_STRATEGY_NUM] = {0};
    for (int i = 0; i < doc_num; ++i) {
        SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
        if (doc == NULL) {
            continue;
        }
        AcExtDataParser attr(doc);
        for (int j = 0; j < MAX_INTENTION_STRATEGY_NUM;j++) {
            if (ac_data.inten_ploy_fwnum[j] <= 0) {
                continue;
            }
            if (attr.get_fwnum() >= ac_data.inten_ploy_fwnum[j] ||
                attr.get_likenum() >= ac_data.inten_ploy_likenum[j]) {
                left_num[j] += 1;
            }
        }
    }
    AC_WRITE_LOG(info, " [inten_ploy_docnum_min:%d]", ac_data.inten_ploy_docnum_min);
    for (int j = MAX_INTENTION_STRATEGY_NUM-1; j >= 0;j--) {
        AC_WRITE_LOG(info, " [j:%d,num:%d]", j, left_num[j]);
        if (left_num[j] >= social_threshold) {
            ac_data.inten_ploy_fwnum_filter = ac_data.inten_ploy_fwnum[j];
            ac_data.inten_ploy_likenum_filter = ac_data.inten_ploy_likenum[j];
            ac_data.need_inten_ploy_filter = 1;
            AC_WRITE_LOG(info, " [filter:{fw:%d,like:%d}]",
                        ac_data.inten_ploy_fwnum_filter, ac_data.inten_ploy_likenum_filter);
            return true;
        }
	}

	ac_data.need_inten_ploy_filter = -1;
	AC_WRITE_LOG(info, " [filter:{fw:%d,like:%d}]",
	            ac_data.inten_ploy_fwnum_filter, ac_data.inten_ploy_likenum_filter);
	return false;
}
/*
static bool get_strategy(QsInfo *info, Ac_statistic &ac_data) {
    AC_WRITE_LOG(info, " [get_strategy]");
	Json::Reader reader;
	Json::Value  root;
	if (info->json_intention == NULL || info->json_intention->text == NULL ||
        !reader.parse(info->json_intention->text, root)) {
        AC_WRITE_LOG(info, " [json_intention NULL]");
        if (info->json_intention && info->json_intention->text) {
            AC_WRITE_LOG(info, " [json.parse error:%s]", info->json_intention->text);
        }
        return false;
	}
    // "ac_social_strategy":{"fwnum":"1,2,4","likenum":"1,5,10"}
    if (root.isMember("strategy_info") && root["strategy_info"].isMember("ac_social_strategy")) {
        if (root["strategy_info"]["ac_social_strategy"].isMember("fwnum") &&
            root["strategy_info"]["ac_social_strategy"]["fwnum"].type() == Json::stringValue) {
            std::string val = root["strategy_info"]["ac_social_strategy"]["fwnum"].asString();
            AC_WRITE_LOG(info, " [strategy.fwnum:%s]", val.c_str());
            std::size_t found = val.find(","), start = 0;
            int index = 0;
            while (found != std::string::npos) {
                string value = val.substr(start, found);
                if (index >= MAX_INTENTION_STRATEGY_NUM) {
                    break;
                }
                ac_data.inten_ploy_fwnum[index] = atoi(value.c_str());
                found++;
                start = found;
                AC_WRITE_LOG(info, " [start:%d,all:%d]", start, val.size());
                index++;
                found = val.find(",", found);
            }
            if (start <= val.size() && index < MAX_INTENTION_STRATEGY_NUM) {
                string value = val.substr(start, val.size());
                ac_data.inten_ploy_fwnum[index] = atoi(value.c_str());
            }
        }
        if (root["strategy_info"]["ac_social_strategy"].isMember("likenum") &&
            root["strategy_info"]["ac_social_strategy"]["likenum"].type() == Json::stringValue) {
            std::string val = root["strategy_info"]["ac_social_strategy"]["likenum"].asString();
            AC_WRITE_LOG(info, " [strategy.likenum:%s]", val.c_str());
            std::size_t found = val.find(","), start = 0;
            int index = 0;
            while (found != std::string::npos) {
                string value = val.substr(start, found);
                if (index >= MAX_INTENTION_STRATEGY_NUM) {
                    break;
                }
                ac_data.inten_ploy_likenum[index] = atoi(value.c_str());
                found++;
                start = found;
                index++;
                found = val.find(",", found);
            }
            if (start <= val.size() && index < MAX_INTENTION_STRATEGY_NUM) {
                string value = val.substr(start, val.size());
                ac_data.inten_ploy_likenum[index] = atoi(value.c_str());
            }
        }
        int doc_num = info->bcinfo.get_doc_num();
        int left_num[MAX_INTENTION_STRATEGY_NUM] = {0};
        for (int i = 0; i < doc_num; ++i) {
            SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
            if (doc == NULL) {
                continue;
            }
            AcExtDataParser attr(doc);
            for (int j = 0; j < MAX_INTENTION_STRATEGY_NUM;j++) {
                if (attr.get_fwnum() >= ac_data.inten_ploy_fwnum[j] ||
                    attr.get_likenum() >= ac_data.inten_ploy_likenum[j]) {
                    left_num[j] += 1;
                }
            }
        }
        for (int j = MAX_INTENTION_STRATEGY_NUM-1; j >= 0;j--) {
            if (left_num[j] >= ac_data.inten_ploy_docnum_min) {
                ac_data.inten_ploy_fwnum_filter = ac_data.inten_ploy_fwnum[j];
                ac_data.inten_ploy_likenum_filter= ac_data.inten_ploy_likenum[j];
                ac_data.need_inten_ploy_filter = true;
                return true;
            }
        }
    }
    return false;
}
*/

static inline bool str2json(const std::string & str, Json::Value & root)
{
    try 
    {   
        Json::Reader reader;

        bool rt = reader.parse(str, root);
        if (!rt)
        {   
            return false;
        }   
    }   
    catch(...)
    {   
        return false;
    }   

    return true;
}

static void get_json_member(QsInfo *info,Json::Value& tmp,vector<pair<string,string> >& res)
{
	Json::Value::Members members = tmp.getMemberNames(); 
	typedef Json::Value::Members::iterator mit;
	for(mit it = members.begin(); it != members.end(); ++it) 
	{
		string first = *it;
		string second = tmp[first].asString();
		res.push_back(make_pair(first,second));
		//AC_WRITE_LOG(info, "[json: frist:%s, second:%s]",first.c_str(),second.c_str());
	}

}

static bool get_da_grades(QsInfo *info, Ac_statistic& ac_sta)
{
	Json::Reader reader;
	Json::Value root;
	
	string grades;
	string good_grade;
	string super_grade;
	string low_grade;
	string del_grade;
	string threshold;

	if (info->json_intention == NULL || info->json_intention->text == NULL ||!reader.parse(info->json_intention->text, root))
    {    
        AC_WRITE_LOG(info, " [json_intention NULL]");
        if (info->json_intention && info->json_intention->text)
        {    
            AC_WRITE_LOG(info, " [json.parse error:%s]", info->json_intention->text);
        }    
        return false;
    }

	if (!root.isMember("strategy_info")) return false;
	if (!root["strategy_info"].isMember("ac_grade_info")) return false;
	
	//if (root.isMember("ac_grade_info"))
    //{    
		if(root["strategy_info"]["ac_grade_info"].isMember("grades"))
		{
			get_json_member(info,root["strategy_info"]["ac_grade_info"]["grades"],ac_sta.conditions);
		}
		else return false;
		
		if(root["strategy_info"]["ac_grade_info"].isMember("good_grade"))
		{
			good_grade = root["strategy_info"]["ac_grade_info"]["good_grade"].asString();
			if(!good_grade.empty())
				ac_sta.good_grade = atoi(good_grade.c_str());
		}
        else return false;
		
		if(root["strategy_info"]["ac_grade_info"].isMember("super_grade"))
		{
			super_grade = root["strategy_info"]["ac_grade_info"]["super_grade"].asString();
			if(!super_grade.empty())
				ac_sta.super_grade = atoi(super_grade.c_str());
		}
        else return false;
		
		if(root["strategy_info"]["ac_grade_info"].isMember("low_grade"))
		{
			low_grade = root["strategy_info"]["ac_grade_info"]["low_grade"].asString();
			if(!low_grade.empty())
				ac_sta.low_grade = atoi(low_grade.c_str());
		}
        else return false;
		
		if(root["strategy_info"]["ac_grade_info"].isMember("del_grade"))
		{
			low_grade = root["strategy_info"]["ac_grade_info"]["del_grade"].asString();
			if(!low_grade.empty())
				ac_sta.low_grade = atoi(low_grade.c_str());
		}
        else return false;

		if(root["strategy_info"]["ac_grade_info"].isMember("threshold"))
		{
			get_json_member(info, root["strategy_info"]["ac_grade_info"]["threshold"],ac_sta.grade_threshold);
		}
		
    //} 
	//else return false;
}
//create calc exprs from da exprs or default exprs
static bool create_calc_exprs(QsInfo* info,Ac_statistic& ac_sta)
{
    AC_WRITE_LOG(info, " [auto grade generating...]");
	int is_da_control = int_from_conf(IS_DA_CONTROL,0);

	//如果DA没有发送参数或参数解析失败，则走默认参数
	if(!is_da_control || !get_da_grades(info, ac_sta))
	{
		Json::Value head_body; 
		Json::Value body;
		str2json(ac_sta.default_exprs,head_body);
		body = head_body["info"];
		get_json_member(info,body, ac_sta.conditions);

		str2json(ac_sta.default_thresholds, head_body);
		body = head_body["threshold"];
		get_json_member(info,body ,ac_sta.grade_threshold);
	}

	//grades info:
	// grade     expr
	// 200:    [social]==1
	// 100:    [good_uid]>0
	//  14:    [social]==2
	//  13:    [intention]&0x04 && [fwnum]>=1 && [likenum]>=10
	//  12:    [valid_fwnum]==1
	//  11:    ([intention]&0x04)&& (([docattr]&(0x1<<46))||([docattr]&(0x1 << 47)))
    for( int i = 0 ; i < ac_sta.conditions.size(); ++ i)
    {
        char* condition = (char*) ac_sta.conditions[i].second.c_str();
		int degree = atoi(ac_sta.conditions[i].first.c_str());
        int ok = expr_create(info, condition, strlen(condition), ac_sta.exprs[degree]); // ac_sta.exprs要改成map： unordered_map<int, expr>;
        if( !ok ) 
		{
			ac_sta.expr_init_ok = false;
			return false;
		}
		//AC_WRITE_LOG(info, "[degree:%d,expr:%s]",degree, condition);
    }

    AC_WRITE_LOG(info, " [create expr success?:%d]",  ac_sta.expr_init_ok);
    return true;
}

static bool get_auto_grades(QsInfo* info,Ac_statistic& ac_sta)
{
	//如果表达式计算失败则走老逻辑
	//默认参数的存在使得这种情况基本不会出现
	bool ok = create_calc_exprs(info, ac_sta);
	//if configure said that we should follow the old version then return false or return true
	return ok;
}

static void get_grade_threshold(QsInfo *info, Ac_statistic& ac_sta) // expression and threshold
{
    int doc_num = info->bcinfo.get_doc_num();
    int qualified_num = 0;
	//threshold
	//grade 13: 100
	//grade 11: 100
    for ( int i = 0; i < ac_sta.grade_threshold.size(); ++i )// if grade_threshold.empty() do nothing !
    {
        qualified_num = 0;
		int degree = atoi(ac_sta.grade_threshold[i].first.c_str());
		int threshold = atoi(ac_sta.grade_threshold[i].second.c_str());
        for (int j = 0 ; j < doc_num; ++j )
        {
            SrmDocInfo *doc = info->bcinfo.get_doc_at(j);
            if( doc == NULL ) continue;

            if( expr_calc(info, ac_sta.exprs[degree], doc) )
            {
                qualified_num++;
            }
			if ( qualified_num >= threshold ) break;
        }
		ac_sta.grade_threshold_filter[degree] = (qualified_num >= threshold ? 1 : -1); 
		AC_WRITE_LOG(info, " [degree:%d, qualified_num:%d, filter:%d]",degree,qualified_num, ac_sta.grade_threshold_filter[degree]);
		
    }
}

static int get_degree_by_exprs(QsInfo *info, Ac_statistic& ac_sta, SrmDocInfo * doc, bool check_all) // get degree by expr and threshold
{
	int degree = 0;
	//int no_filter;
	int check_num = 100;
	if(!check_all)
	{
		//check_num = int_from_conf(CHECK_NUM,2);
		check_num = 2;
	}

    for ( expr_it it = ac_sta.exprs.begin(); it != ac_sta.exprs.end() && check_num--; ++it )
    {
		//检查某个doc的档次
		degree = it->first;
		//no_filter = ac_sta.grade_threshold_filter[degree]; 
		// 0/1表示不需要过滤或满足条件不用过滤，-1表示不满足阈值要过滤掉
		//no_filter = true;
		/*
		if(ac_sta.grade_threshold_filter.find(degree) != ac_sta.grade_threshold_filter.end())
		{
			if(ac_sta.grade_threshold_filter[degree] == 1) 
				no_filter = true;
			else
				no_filter = false;
		}
		*/

        if((ac_sta.need_inten_ploy_filter>=0 || ac_sta.need_category_filter>=0) && expr_calc(info, it->second, doc) )
        {
    		return degree;
        }
    }
	return DEGREE_7_NOACT_DUP ;
}

static bool ac_auto_reranking_base(QsInfo *info, const AcRerankData& data, Ac_statistic& ac_sta) 
{
	if (info == NULL) {
		return false;
	}
	QsInfoBC *bcinfo = &info->bcinfo;
	int ranktype = RankType(info);
	int doc_num = bcinfo->get_doc_num();
    int orig_num = doc_num;
	ac_sta.original_num = doc_num;
	ac_sta.total_num = doc_num;
    // F_PARAM get dup flag
    int resort_flag = 1;
    int dup_flag = 1;
    ac_set_flag(info, &resort_flag, &dup_flag);
    ac_sta.dup_flag = dup_flag;
    int result_keep_num = int_from_conf(RESULT_KEEP_NUM, 1000);
    ac_sta.bc_input_num = doc_num;
    if (ac_sta.dup_flag && doc_num <= ac_sta.min_num_skip_dup) {
        ac_sta.dup_flag = 0;
    }

	AC_WRITE_LOG(info, " [auto ranking]");
    // F_PARAM strategy set socialtime
    if (info->parameters.getXsort() == std::string("social") &&
        info->parameters.getUs() == 1 &&
        (info->parameters.gray == 1 || info->parameters.socialtime)) {
        ac_sta.is_socialtime = true;
    }
    std::string key;
    if (info->params.key) {
        key = info->params.key;
    } else {
        key = info->params.query;
    }
    // F_PARAM strategy set cluster_repost
    if (info->parameters.getXsort() == std::string("social") &&
        info->parameters.getUs() == 1 &&
        info->parameters.cluster_repost == 1) {
        ac_sta.is_cluster_repost = true;
        ac_sta.min_repost_num = 2;
    }
	// int top_n_degree = int_from_conf(TOP_N_DEGREE, DEGREE_9_GOOD);
    //AC_WRITE_LOG(info, " [top_n_degree:%d]", DEGREE_9_GOOD);
    AC_WRITE_LOG(info, " [top_n_degree:%d]", ac_sta.good_grade);
    bool check_sandbox = true;
    /*
	auto_reference<ACStringSet> sandbox_query("sandbox_query");
	if (*sandbox_query == NULL) {
        check_sandbox = false;
	} else {
        if (!(info->intention & INTENTION_STAR) && !sandbox_query->test(key)) {
            check_sandbox = false;
        }
    }
    */
    int bc_check_black = int_from_conf(BLACK_LIST_CHECK_SWITCH, 1);
    //{TODO:del
    auto_reference<SourceIDDict> black_dict("source_id.dict");
	auto_reference<ACLongSet> mask_mids("mask_mids");
	auto_reference<ACLongSet> manual_black_uids("uids_black_manual.txt");
	auto_reference<DASearchDictSet> black_uids("uids_black");
	auto_reference<DASearchDictSet> plat_black("plat_black");
	auto_reference<DASearchDict> suspect_uids("suspect_uids");

    //}
    time_t time_now = ac_sta.curtime;
	time_t delay_time = time_now - int_from_conf(SUSPECT_DELAY_TIME, 172800);
	time_t delay_time_qi20 = time_now - int_from_conf(QI20_DELAY_TIME, 604800);

	char* source_black = bitdup_get(g_sort.bitmap, g_sort.source_black_type);
	//auto_reference<DASearchDictSet> good_uids("good_uids");
    auto_reference<SourceIDDict> dict("old_source_id.dict");
	auto_reference<DASearchDict> uids_validfans_week("uids_validfans_week");
	auto_reference<DASearchDict> uids_validfans_hour("uids_validfans_hour");
	//auto_reference<DASearchDictSet> dup_black_uids("dup_uids_black");
	//auto_reference<ACLongSet> sandbox_uids("sandbox_uids");
    // for old imgcheat
    //auto_reference<DASearchDictSet> uid_source_mul_dict("uid_source_mul_dict");
    //auto_reference<DASearchDictSet> uid_source_suspect_dict("uid_source_suspect_dict");
	//auto_reference<ACLongSet> uid_source_black_source("uid_source_black_source");
	//auto_reference<ACLongSet> uid_source_white_source("uid_source_white_source.txt");
	auto_reference<ACStringSet> img_check_query("hot_query.txt");
	/*
    if (!(*good_uids)) {
        AC_WRITE_LOG(info, " [good_uids load error]");
    }
	*/
    int inten_user_type = intention_user_type(info->intention);
	QsInfoSocial *social = &info->socialinfo;
    // F_PARAM strategy sort_time
	//get_grade_threshold(info, ac_sta);

    if (!data.degrade) {
        int sort_time_num = doc_num;
        if (sort_time_num > data.max_act_rerank_doc) {
            sort_time_num = data.max_act_rerank_doc;
        }
        if (ac_sta.is_socialtime) {
            for (int i = 0; i < sort_time_num; ++i) {
                SrmDocInfo *doc = bcinfo->get_doc_at(i);
                if (doc == NULL) {
                    continue;
                }
                UpdateFamousConcernInfo(info, doc, inten_user_type);
                UpdataTimeOne(info, doc, inten_user_type, time_now);
            }
        }
        bcinfo->sort(0, sort_time_num, my_time_cmp);
        // get_strategy(info, ac_sta);
        get_strategy_inten(info, ac_sta);
		if ( ac_sta.need_inten_ploy_filter==1 || ac_sta.need_category_filter==1 )
		{
			AC_WRITE_LOG(info, " [film result >= 100, no recall qi low doc]");
			ac_sta.recall_qi_low_doc = 0;
			ac_sta.recall_qi_low_doc_week = 0;
			ac_sta.recall_qi_low_doc_month = 0;
		}
    }
    /* <abtest_social_switch>0</abtest_social_switch>
     * <abtest_social_0>9</abtest_social_0>
     * <abtest_csort_del>0</abtest_csort_del>
    if (info->parameters.abtest) {
        if ((info->intention & INTENTION_HOTSEARCH_WORD) ||
            (info->intention & INTENTION_PLACE) ||
            *img_check_query && img_check_query->test(key)) {
            ac_sta.check_pic_source = true;
            ac_sta.qualified_num = 0;
            ac_sta.original_pic_num = 0;
            ac_sta.orig_video_num = 0;
            ac_sta.all_pic_num = 0;
            ac_sta.all_video_num = 0;
            ac_sta.del_pic_num = 0;
            ac_sta.del_other_num = 0;
            ac_sta.del_video_num = 0;
        }
    } else {
    }
    */
    if (*img_check_query && img_check_query->test(key)) {
        if (ac_sta.check_pic_source_dict) {
            ac_sta.check_pic_source = true;
            ac_sta.check_pic_source_dict_query = true;
            ac_sta.qualified_num = 0;
            ac_sta.original_pic_num = 0;
            ac_sta.orig_video_num = 0;
            ac_sta.all_pic_num = 0;
            ac_sta.all_video_num = 0;
            ac_sta.del_pic_num = 0;
            ac_sta.del_other_num = 0;
            ac_sta.del_video_num = 0;
        }
    } else if (ac_sta.check_pic_source_normal) {
        ac_sta.check_pic_source = true;
    }
    int valid_forward_fwnum = int_from_conf(VALID_FORWARD_FWNUM, 1);
    // last 40, don't act cluster
	for (;
         ac_sta.act_index < (doc_num - ac_sta.too_few_limit_doc_num) &&
         ac_sta.act_index < data.max_act_rerank_doc;
         ac_sta.act_index++) {
		SrmDocInfo *doc = bcinfo->get_doc_at(ac_sta.act_index);
        if (doc == NULL) {
            ac_sta.doc_null++;
            continue;
        }
        AcExtDataParser attr(doc);
        uint64_t uid = GetDomain(doc);
        // F_PARAM filter
        //if (bc_check_black)
		//{
		//	if(check_invalid_doc_mid(info, doc, uid, attr, ac_sta, true)) continue;
		//}
		//else
		//{
		//	if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
		//}
		if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
        bool is_good_uids = false;
		int white_type = PassByWhite(attr);
        if (white_type) {
            is_good_uids = true;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [act:"PRIU64","PRIU64",%d,USER_TYPE_GOOD_UID_OR_WHITE_WEIBO]", doc->docid, uid, white_type);
        }
        uint64_t docattr = doc->docattr;
        int source = attr.get_digit_source();
		int is_social = getSocialRelation(info, uid, docattr, social);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [act:"PRIU64","PRIU64",%d:%d, good:%d,social:%d]",
            doc->docid, uid, doc->bcidx, doc->bsidx, is_good_uids, is_social);
        // F_PARAM filter
        if (is_good_uids || (is_social == 1)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [good_uid or social pass black:"PRIU64","PRIU64",%d:%d]",
                doc->docid, uid, doc->bcidx, doc->bsidx);
        } else {
			if(check_manual_black_uids(info, doc, attr, ac_sta, uid, manual_black_uids,delay_time_qi20, true)) {
				continue;
			}
            if (!bc_check_black && check_invalid_doc(info, doc, uid, attr, check_sandbox, delay_time, delay_time_qi20, ac_sta,
                                  mask_mids, manual_black_uids, black_uids, plat_black,
                                  suspect_uids, source, black_dict, true)) {
                continue;
            }
        }
        // F_PARAM strategy
        bool black_source = false;
        if (source && BITDUP_EXIST(source_black, source, MAX_SOURCE_SIZE)) { 
            black_source = true;
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_BLACK_UID_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                            " [source black:"PRIU64"]", doc->docid);
        }
		doc->filter_flag = 0;
        update_validfans_doc(info, doc, uid, uids_validfans_week, uids_validfans_hour);
        bool is_dup = false;
        int degree = 0;
        uint64_t qi = attr.get_qi();
		int granularity = get_granularity_qi(qi);
		bool is_bad_vip_or_source = doc_is_bad_result(info, doc, attr, dict);
        int dup_result = 0;

		degree = get_degree_by_exprs(info, ac_sta, doc,true);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [init d:"PRIU64",%d]", doc->docid, degree);
        bool check_pic_source_failed = false;
        bool already_check_pic_source = false;
		if(degree >= ac_sta.super_grade)
		{
			set_degree(doc, degree);
			is_dup = is_dup_update_doc(info, doc, attr, ac_sta, false, dup_result);	
			ac_sta.degree_num[degree]++;
		}
		else if(degree > ac_sta.good_grade)
		{
			set_degree(doc, degree);
			ac_sta.degree_num[degree]++;
            if(ac_sta.check_pic_source &&
               ac_sta.cluster_data_update.size() < ac_sta.cluster_first_page_num_degree) {
                CheckClusterDocPicSource(info, doc, ac_sta, attr, already_check_pic_source,
                            check_pic_source_failed);
            }
            if (!check_pic_source_failed) {
                is_dup = is_dup_update_doc(info, doc, attr, ac_sta, false, dup_result);	
                if(is_dup)
                {
                    if(dup_result)
                    {
                        degree = DEGREE_3_DUP_DUP;
                    }
                    else
                    {
                        degree = DEGREE_4_DUP;
                    }
                }
            }
			//else ac_sta.degree_num[degree]++;
		}
		else if(degree < ac_sta.del_grade)
		{
			ac_sta.degree_0++;
            set_degree(doc,degree);
			DelDocAddDupPrintLog(info, doc, attr, "less than del_grade", true, ac_sta);
			continue;
		}
		else
		{
			if(degree_is_del(info, doc, granularity))
			{
                degree = DEGREE_0;
				set_degree(doc,degree);
				ac_sta.degree_0++;
				DelDocAddDupPrintLog(info, doc, attr, "degree_is_del", true, ac_sta);
				continue;
			}
			else if(black_source || is_bad_vip_or_source ||(granularity < 3))
			{
				degree = ac_sta.low_grade;
				set_degree(doc,degree);
				ac_sta.degree_num[degree]++;
			}
			else
			{
				degree = ac_sta.good_grade;
				set_degree(doc,degree);
                if(ac_sta.check_pic_source &&
                   ac_sta.cluster_data_update.size() < ac_sta.cluster_first_page_num_degree) {
                    CheckClusterDocPicSource(info, doc, ac_sta, attr, already_check_pic_source,
                                check_pic_source_failed);
                }
                if (!check_pic_source_failed) {
                    is_dup = is_dup_update_doc(info, doc, attr, ac_sta, false, dup_result);
                    if(is_dup) {
                        degree = dup_result != 0 ? DEGREE_3_DUP_DUP : DEGREE_4_DUP;
                    } else {
                        degree = ac_sta.good_grade;
                        ac_sta.degree_num[degree]++;
                    }
                }
			}
		}
        set_degree(doc, degree);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [top_n:%d,"PRI64", d:%d:%d]", ac_sta.act_index, doc->docid, degree, doc->filter_flag);
        if(degree < ac_sta.good_grade) {
            ac_sta.degree_small++;
            continue;
        }
        ac_sta.topN_top++;
        // F_PARAM strategy
        DupClusterActOneUpdate(info, doc, attr, ac_sta);
        if (data.degrade_state <= RERANK_UPDATE_DEGRADE_STATE_1) {
            if (ac_sta.cluster_data_update.size() > data.keep_top_n &&
                ac_sta.topN_top > data.cluster_top_buff * data.keep_top_n) {
                break;
            }
        } else {
            if (ac_sta.topN_top > data.cluster_top_buff * data.keep_top_n) {
				 AC_WRITE_LOG(info, "[topn_top]");
                break;
            }
        }
	}
    // for cluster
    AC_WRITE_LOG(info, " [all:%d, act:%d, top:%d, small:%d, del: black:%d, degree0:%d, act:%d=%d, "
                "recall:%d:%d:%d, is_dup_replace_del:%d]",
                doc_num, ac_sta.act_index, ac_sta.topN_top, ac_sta.degree_small, ac_sta.black_num,
                ac_sta.degree_0, ac_sta.act_index,
                (ac_sta.degree_small + ac_sta.topN_top + ac_sta.black_num + ac_sta.degree_0),
                ac_sta.recall_qi_low_doc, ac_sta.recall_qi_low_doc_week, ac_sta.recall_qi_low_doc_month,
                ac_sta.is_dup_replace_del);

	//print grading results
	__gnu_cxx::hash_map<int,int>::iterator dit = ac_sta.degree_num.begin();
	for(;dit != ac_sta.degree_num.end();++dit)
	{
		AC_WRITE_LOG(info, "[d:%d,n:%d]",dit->first,dit->second);
	}

    int real_top = 0;
    // F_PARAM strategy
    {
        AC_WRITE_LOG(info," [size:%d:%d]", ac_sta.cluster_data_update.size(),
                    ac_sta.cluster_for_picsource_check.size());
        std::set<uint64_t> multi_dup_del;
        // remove only root
        __gnu_cxx::hash_map<uint64_t, AcDupRepostClusterDataUpdate>::iterator it =
                    ac_sta.cluster_data_update.begin();
        for (; it != ac_sta.cluster_data_update.end(); ++it) {
            // remove  count <=1 && orig's weibo
            if (it->second.root_cluster ||
                it->second.count <= 1 && it->second.root_doc) {
                AC_WRITE_DOC_DEBUG_LOG(info, it->second.root_doc, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                    " [cluster remove only root dup:"PRI64", root_cluster:%d, count:%d, "PRI64"]",
                    it->first, it->second.root_cluster, it->second.count, it->second.root_doc->docid);
                multi_dup_del.insert(it->first);
                std::map<uint64_t, ClusterDocid>::iterator it_cluster =
                    ac_sta.cluster_for_picsource_check.find(it->second.root_doc->docid);
                if (it_cluster != ac_sta.cluster_for_picsource_check.end()) {
                    it_cluster->second.act = 1;
                }
            }
        }
        std::set<uint64_t>::iterator it_dup = multi_dup_del.begin();
        for (; it_dup != multi_dup_del.end(); ++it_dup) {
            __gnu_cxx::hash_map<uint64_t, AcDupRepostClusterDataUpdate>::iterator it_del =
                    ac_sta.cluster_data_update.find(*it_dup);
            if (it_del != ac_sta.cluster_data_update.end()) {
                AC_WRITE_DEBUG_LOG(info, PRINT_CLUSTER_LOG_LELVE, SHOW_DEBUG_NEWLINE,
                            " [cluster del: dup:"PRI64"]", *it_dup);
                ac_sta.cluster_data_update.erase(it_del);
            }
        }
        // F_PARAM strategy
        ClusterRepostUpdate(info, ac_sta);
        ClusterRepostDelUpdate(info, ac_sta);
        for (int i = 0; i < ac_sta.act_index && i < doc_num; ++i) {
            SrmDocInfo *doc = bcinfo->get_doc_at(i);
            if (doc == NULL) {
                continue;
            }
            if (doc->filter_flag & 0x1) {
                // & 0x1 must degree >= DEGREE_9_GOOD
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG del_cluster:"PRI64"]", doc->docid);
                info->bcinfo.del_doc(doc);
                ac_sta.cluster_del++;
                ac_sta.topN_top--;
                continue;
            }
            int degree = get_degree(doc);
            //if (degree < DEGREE_9_GOOD) 
            if (degree < ac_sta.good_grade) {
                continue;
            }
            real_top++;
            UpdateCheckRecallData(info, ac_sta, doc);
            ac_sta.top_time_num++;
            if (ac_sta.top_n_time > doc->rank) {
                ac_sta.top_n_time = doc->rank;
            }
            if (doc->cluster_info.time && ac_sta.top_cluster_time > doc->rank) {
                ac_sta.top_cluster_time = doc->rank;
            }
        }
    }
    int orig_top = ac_sta.topN_top;
    ac_sta.topN_top = real_top;
    if (ac_sta.topN_top < ac_sta.min_topN_num_for_recall) {
        ac_sta.need_recall_dup_num = ac_sta.min_topN_num_for_recall - ac_sta.topN_top;
    }
    if ((info->intention & INTENTION_HOTSEARCH_WORD) &&
        int_from_conf(HOT_QUERY_STOP_DUPRECALL, 1)) {
        ac_sta.need_recall_dup_num = 0;
    }
    AC_WRITE_LOG(info, " [act:%d, top:%d:%d, dup(need_recall:%d), DEL: black:%d, degree0:%d, cluster:%d"
                ", recall:%d:%d:%d, top_k:%d,"PRIU64","PRIU64", is_dup_replace_del:%d, doc_null:%d]",
                ac_sta.act_index, ac_sta.topN_top, orig_top, ac_sta.need_recall_dup_num,
                ac_sta.black_num, ac_sta.degree_0, ac_sta.cluster_del,
                ac_sta.recall_qi_low_doc, ac_sta.recall_qi_low_doc_week, ac_sta.recall_qi_low_doc_month,
                ac_sta.top_time_num, ac_sta.top_n_time, ac_sta.top_cluster_time,
                ac_sta.is_dup_replace_del, ac_sta.doc_null);
    // for 1000 
    ac_sta.now_del_doc = ac_sta.black_num + ac_sta.degree_0 + ac_sta.cluster_del;
    ac_sta.no_act_doc_num = doc_num - ac_sta.act_index;
    // for 1000 already degree
    if (ac_sta.need_recall_dup_num > 0) {
        // F_PARAM strategy
        //TopNRecallDup(info, data, doc_num, DEGREE_9_GOOD, ac_sta);
        TopNRecallDup(info, data, doc_num, ac_sta.good_grade, ac_sta);
    }
    TopNActDegreeNoactAndDelDup(info, data, doc_num, ac_sta.good_grade, ac_sta);
    AC_WRITE_LOG(info,
        " [t:"PRIU64", all:%d, no_act:%d, top:%d, dup(need_recall:%d,recall:%d), act_keep:%d"
        ", act_time_del:%d, del:%d, act:%d==%d, recall:%d:%d:%d, is_dup_replace_del:%d]",
        ac_sta.top_n_time, doc_num, ac_sta.no_act_doc_num, ac_sta.topN_top, ac_sta.need_recall_dup_num,
        ac_sta.recall_dup_num, ac_sta.act_keep_doc,
        ac_sta.act_del_time, ac_sta.now_del_doc, ac_sta.act_index,
        (ac_sta.topN_top + ac_sta.act_keep_doc + ac_sta.act_del_time + ac_sta.now_del_doc),
        ac_sta.recall_qi_low_doc, ac_sta.recall_qi_low_doc_week, ac_sta.recall_qi_low_doc_month,
        ac_sta.is_dup_replace_del);
    // F_PARAM strategy
    adjust_nick_split(info);
    adjust_nick_partial_result(info);
    // adjust_movie_result(info);
    // adjust category results
    adjust_category_result(info, int_from_conf(CATEGORY_RESULT_TIMELINE, 100), 0);
    // for 1000 other, no cluster
    int left_black = 0;
    ac_sta.top_n_left_better_time = ac_sta.top_n_time;
    // F_PARAM strategy
    for (int i = ac_sta.act_index; i < doc_num; ++i) {
        SrmDocInfo *doc = bcinfo->get_doc_at(i);
        if (doc == NULL) {
            continue;
        }
        // enought
        // F_PARAM strategy
        if (ac_sta.act_keep_doc + ac_sta.topN_top > result_keep_num + ac_sta.uid_social_num_buf) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN KEEP_NUM_OK :"PRI64", :%d]", doc->docid, i);
            info->bcinfo.del_doc(doc);
            continue;
        }
        // F_PARAM strategy
        // check time when no recall_qi_low_doc
        if (ac_sta.recall_qi_low_doc <= 0 &&
            ac_sta.recall_qi_low_doc_week <= 0 &&
            ac_sta.recall_qi_low_doc_month <= 0 &&
            doc->rank >= ac_sta.top_n_time) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN >top_n_time :"PRI64", :%d]", doc->docid, i);
            info->bcinfo.del_doc(doc);
            ac_sta.left_time_bad++;
            continue;
        }
        AcExtDataParser attr(doc);
        uint64_t uid = GetDomain(doc);
        // F_PARAM filter
        // check black
        //if (!bc_check_black && check_invalid_doc_mid(info, doc, uid, attr, ac_sta, true)) {
        //    continue;
        //}
		//else if (bc_check_black)
		//{
		//	if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
		//}

		if (check_invalid_doc_mid(info, doc, uid, attr, ac_sta, mask_mids, true )) continue;
       // bool is_good_uids = false;
	   // int white_type = PassByWhite(attr);
       // if (white_type) {
       //     is_good_uids = true;
       //     AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
       //         " [act:"PRIU64","PRIU64",%d,USER_TYPE_GOOD_UID_OR_WHITE_WEIBO]", doc->docid, uid, white_type);
       // }

		int degree = get_degree_by_exprs(info, ac_sta, doc, false);
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
            " [>act_index act:"PRIU64","PRIU64",%d:%d, good/social:%d]",
            doc->docid, uid, doc->bcidx, doc->bsidx, degree);
		if(degree >= ac_sta.super_grade)  
		{
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [good_uid or social pass black:"PRIU64","PRIU64",%d:%d]",
                doc->docid, uid, doc->bcidx, doc->bsidx);
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [<1000 social or vip keep:"PRI64", index:%d,good/social:%d]",
                    doc->docid, i, degree);

            ac_sta.act_keep_doc++;
			set_degree(doc,degree);
			if (!update_repost_fake_cluster_struct(info, doc, attr, ac_sta))
			{
				AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
						" [DEL_LOG outTopN update_repost :"PRIU64",dup del:"PRIU64", :%d]",
						doc->docid, attr.get_dup(), i);
				info->bcinfo.del_doc(doc);
				ac_sta.update_repost++;
				continue;
			}
			int dup_result = 0;
			is_dup_update_doc(info, doc, attr, ac_sta, true, dup_result);
			continue;
		}
		else
		{
			int source = attr.get_digit_source();
			if(check_manual_black_uids(info, doc, attr, ac_sta, uid, manual_black_uids, delay_time_qi20, true)) {
				continue;
			}
			if (!bc_check_black && check_invalid_doc(info, doc, uid, attr, check_sandbox, delay_time, delay_time_qi20, ac_sta,
						mask_mids, manual_black_uids, black_uids, plat_black,suspect_uids, source, black_dict, true))
				continue;
		}
		
        // check bad
        // F_PARAM strategy
		int granularity = get_granularity_qi(attr.get_qi());
        if (degree_is_del(info, doc, granularity)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN degree_del :"PRI64", :%d]", doc->docid, i);
            info->bcinfo.del_doc(doc);
            ac_sta.left_bad++;
            continue;
        }
        if(ac_sta.check_pic_source &&
           (ac_sta.topN_top+ac_sta.act_keep_doc) < ac_sta.cluster_first_page_num_after_cluster) {
            ac_sta.qualified_num++;
            uint64_t digit_attr = attr.get_digit_attr();
            hot_cheat_check_addinfo(info, digit_attr,
                        (ac_sta.topN_top+ac_sta.act_keep_doc), ac_sta);
            if (CheckPicSource(info, doc, ac_sta, attr, digit_attr)) {
                ac_sta.degree_0++;
                DelDocAddDupPrintLog(info, doc, attr, "source_check_failed", false, ac_sta);
                continue;
            }
        }
        // for update repost cluster_struct
        // for repost must > last_cluster_time
        // F_PARAM strategy
        if (CheckForwardClusterDel(info, attr, ac_sta, doc)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN repost_cluster t:%u > cluster_t:"PRIU64",dup del:"PRI64", :%d]",
                    doc->rank, ac_sta.top_cluster_time, doc->docid, i);
            info->bcinfo.del_doc(doc);
            ac_sta.left_cluser_time_error++;
            continue;
        }
        // for update repost cluster_struct
        // F_PARAM strategy
        if (!update_repost_fake_cluster_struct(info, doc, attr, ac_sta)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN update_repost :"PRIU64",dup del:"PRIU64", :%d]",
                    doc->docid, attr.get_dup(), i);
            info->bcinfo.del_doc(doc);
            ac_sta.update_repost++;
            continue;
        }
        // check dup
		doc->filter_flag = 0;
        degree = 0;
        // validfans need in is_bad_blue_vip_doc <= doc_is_bad_result
        // update_validfans_doc(info, doc, uid, uids_validfans_week, uids_validfans_hour);
        // check low and recall
        // F_PARAM strategy
        int invalid = is_invalid_topN_left_doc(info, doc, attr, ac_sta);
        if (invalid) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [<1000 > top_n invalid:%d set flag:"PRI64", :%d]", invalid, doc->docid, i);
            degree = DEGREE_8_RECALL;
            doc->filter_flag = 0;
            doc->filter_flag |= degree << 20;
            ac_sta.left_recall++;
            continue;
        }
        //doc_num - i < ac_sta.too_few_limit_doc_num &&
        // F_PARAM strategy
        int dup_result = 0;
        if (is_dup_update_doc(info, doc, attr, ac_sta, true, dup_result)) {
            if (!dup_result && ac_sta.need_recall_dup_num > 0) {
                degree = DEGREE_6_RECALL_LOW_DUP;
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [RECALL_LOW_DUP_BETTER 3, d:%d :"PRI64", t:%u]",
                        degree, doc->docid, doc->rank);
            } else {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG outTopN dup:"PRI64", :%d]", doc->docid, i);
                info->bcinfo.del_doc(doc);
                ac_sta.left_dup++;
                continue;
            }
        }
        // F_PARAM strategy
        // for recall
        if (UpdateCheckRecallData(info, ac_sta, doc)) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [<1000 recall_doc_d keep:"PRI64", :%d]", doc->docid, i);
            ac_sta.act_keep_doc++;
            ac_sta.recall_doc_num++;
            if (degree == DEGREE_6_RECALL_LOW_DUP) {
                ac_sta.need_recall_dup_num--;
                ac_sta.recall_dup_num++;
            }
            continue;
        }
        // F_PARAM strategy
        if (doc->rank >= ac_sta.top_n_time) {
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [DEL_LOG outTopN >top_n_time :"PRI64", :%d]", doc->docid, i);
            info->bcinfo.del_doc(doc);
            ac_sta.left_time_bad++;
            continue;
        }
        if (degree == DEGREE_6_RECALL_LOW_DUP) {
            ac_sta.need_recall_dup_num--;
            ac_sta.recall_dup_num++;
        }
        if (doc->rank < ac_sta.top_n_left_better_time) {
            ac_sta.top_n_left_better_time = doc->rank;
        }
        // 1: < top_n_time
        // 2: >= top_n_time, but need recall, so all keep
        AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                " [<1000 > top_n keep:"PRI64", d:%d,:%d]", doc->docid, degree, i);
        ac_sta.act_keep_doc++;
    }
    AC_WRITE_LOG(info, " [act_keep_doc:%d, top:%d, result_keep_num:%d]",
                ac_sta.act_keep_doc, ac_sta.topN_top, result_keep_num);
    // for recall
    // F_PARAM strategy
    if (ac_sta.act_keep_doc + ac_sta.topN_top < result_keep_num) {
        for (int i = ac_sta.act_index; i < doc_num; ++i) {
            SrmDocInfo *doc = bcinfo->get_doc_at(i);
            if (doc == NULL) {
                continue;
            }
            int degree = get_degree(doc);
            if (degree != DEGREE_8_RECALL) {
                continue;
            }
            if (ac_sta.act_keep_doc + ac_sta.topN_top > result_keep_num + ac_sta.uid_social_num_buf) {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG recall KEEP_NUM_ok :"PRI64", :%d]", doc->docid, i);
                info->bcinfo.del_doc(doc);
                continue;
            }
            if ((ac_sta.bc_input_num > ac_sta.min_num_skip_dup) &&
                (doc->rank >= ac_sta.top_n_time || doc->rank >= ac_sta.top_n_left_better_time)) {
                AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                        " [DEL_LOG outTopN last >top_n_time(%d>%d):"PRI64":%d,%u > "PRIU64","PRIU64"]",
                        ac_sta.bc_input_num, ac_sta.min_num_skip_dup,
                        doc->docid, i, doc->rank, ac_sta.top_n_time, ac_sta.top_n_left_better_time);
                info->bcinfo.del_doc(doc);
                ac_sta.left_time_bad++;
                continue;
            }
            doc->filter_flag = 0;
            AcExtDataParser attr(doc);
            if(ac_sta.check_pic_source &&
               (ac_sta.topN_top+ac_sta.act_keep_doc) < ac_sta.cluster_first_page_num_after_cluster) {
                ac_sta.qualified_num++;
                uint64_t digit_attr = attr.get_digit_attr();
                hot_cheat_check_addinfo(info, digit_attr,
                            (ac_sta.topN_top+ac_sta.act_keep_doc), ac_sta);
                if (CheckPicSource(info, doc, ac_sta, attr, digit_attr)) {
                    ac_sta.degree_0++;
                    DelDocAddDupPrintLog(info, doc, attr, "source_check_failed", false, ac_sta);
                    continue;
                }
            }
            int dup_result = 0;
            if (is_dup_update_doc(info, doc, attr, ac_sta, true, dup_result)) {
                if (!dup_result && ac_sta.need_recall_dup_num > 0) {
                    degree = DEGREE_6_RECALL_LOW_DUP;
                    ac_sta.need_recall_dup_num--;
                    ac_sta.recall_dup_num++;
                    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                            " [RECALL_LAST_LOW_DUP, d:%d :"PRI64", t:%u]",
                            degree, doc->docid, doc->rank);
                } else {
                    AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                            " [DEL_LOG outTopN last dup:"PRI64", :%d]", doc->docid, i);
                    info->bcinfo.del_doc(doc);
                    ac_sta.left_dup++;
                    continue;
                }
            }
            AC_WRITE_DOC_DEBUG_LOG(info, doc, PRINT_RERANK_UPDATE_LOG, SHOW_DEBUG_NEWLINE,
                    " [>1000 recall rank:%u cluster_time:"PRIU64","PRIU64" del:"PRI64", :%d]",
                    doc->rank, ac_sta.top_cluster_time, GET_FORWARD_FLAG(attr.get_digit_attr()),
                    doc->docid, i);
            ac_sta.recall_doc_num++;
            ac_sta.act_keep_doc++;
        }
    }
	info->bcinfo.refresh();
    doc_num = bcinfo->get_doc_num();
	bcinfo->sort(0, doc_num, my_time_cmp);
    // for uid dup, and socialtime limit
    // F_PARAM strategy
    ac_uid_dup_and_socialtime_limit(info, data, doc_num, ac_sta);
    AC_WRITE_LOG(info, " [> top_N: left_time_bad:%d, balck:%d, update_repost:%d, left_bad:%d"
                       ", left_dup:%d, left_recall:%d, uid_dup:%d, cluster_del:%d"
                       ", real recall:%d, repost root_mid_del:%d, is_dup_replace_del:%d] ",
                ac_sta.left_time_bad, ac_sta.black_num, ac_sta.update_repost, ac_sta.left_bad,
                ac_sta.left_dup, ac_sta.left_recall, ac_sta.del_uid_dup_num,
                ac_sta.repost_cluster_del_num, ac_sta.recall_doc_num, ac_sta.repost_root_mid_del_count,
                ac_sta.is_dup_replace_del);
	info->bcinfo.refresh();
    // last sort by time
	doc_num = bcinfo->get_doc_num();
    reset_rank_use_time(info,ranktype);
	bcinfo->sort(0, doc_num, my_time_cmp);
    int before_pic_num = doc_num;
/*    
    if (*uid_source_dict and *realtime_hotquery and realtime_hotquery->test(key))
    //if (*uid_source_dict) 
    {
        hot_cheat_check(info, ac_sta, doc_num, uid_source_dict);
        info->bcinfo.refresh();
    }
    */
    // F_PARAM strategy
    if (ac_sta.check_pic_source) {
        AC_WRITE_LOG(info, " [new_before_hot:pic:(%d,%d,%d) video:(%d,%d,%d) del:(%d,%d]",
                    ac_sta.original_pic_num, ac_sta.all_pic_num,
                    (ac_sta.all_pic_num - ac_sta.del_pic_num),
                    ac_sta.orig_video_num, ac_sta.all_video_num,
                    (ac_sta.all_video_num - ac_sta.del_video_num),
                    ac_sta.del_pic_num, ac_sta.del_other_num);
        AC_WRITE_LOG(info, " [hot_cheat_check]");
        int use_new_hot_pic_source = int_from_conf(USE_NEW_HOT_PIC_SOURCE, 1);
        if (use_new_hot_pic_source) {
            hot_cheat_check_v2(info, ac_sta, doc_num);
        } else {
            hot_cheat_check(info, ac_sta, doc_num);
        }
        info->bcinfo.refresh();
    }
	doc_num = bcinfo->get_doc_num();
    AC_WRITE_LOG(info, " [f_1:%d, f_2:%d, o_num:%d]", before_pic_num, doc_num, orig_num);
    
	//img_check_query == hot_query.txt
	if ((info->intention & INTENTION_HOTSEARCH_WORD) && *img_check_query && img_check_query->test(key))
	//if ( *img_check_query && img_check_query->test(key))
	{
		AC_WRITE_LOG(info, " [new source check duplication");
		drop_duplicated_new_source(info, ac_sta, doc_num);
		info->bcinfo.refresh();
	}
	int after_source_num = bcinfo->get_doc_num();
    AC_WRITE_LOG(info, " [before:%d, after:%d, o_num:%d]", doc_num, after_source_num, orig_num);
    return true;
}
