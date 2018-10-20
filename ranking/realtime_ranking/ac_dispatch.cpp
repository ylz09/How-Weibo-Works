/***************************************************************************
 * 
 * Copyright (c) 2011 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/
 
/**
 * @file ac_dispatch.cpp
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/03/16
 * @version 1.0
 * @brief 
 *  
 **/

#include "call_data.h"
#include "ac_include.h"
#include "ac_miniblog.h"
#include "ac_utils.h"
#include "ac_g_conf.h"

/**
 * get the mechine list(ids) of next to search for bc.
 *
 * @param [in/out] info
 * @param [in]     groups
 * @param [in]     idx
 * @return AC_SUCC dispath success and then ac framework would (re)seach acording to
 *         the machine list of _idx_
 *         AC_FAIL dispath fail and then ac framework would do DC 
 */
int ac_dispatch(QsInfo *info, std::vector<MGroup *>&  groups, uint32_t *idx){
	int max = MAX_PRIORITY;
	int suit = -1;
	int step = 0;
	bool m_sample = false, d_sample = false;
	uint32_t res_addr = info->bcinfo.addr_res;

	info->bcinfo.dispatch_count ++;

    int max_res_num = GlobalConfSingleton::GetInstance().getAcConf().global.request_bc_threshhold;
    if (info->parameters.getXsort() == std::string("hot") &&
        info->parameters.getUs() == 1) {
        //GlobalConfSingleton::GetInstance().getAcConf().global.hot_request_bc_threshhold;
        max_res_num = conf_get_int(info->conf, "hot_request_bc_threshhold", 100);
    }
	if ((int)info->bcinfo.get_doc_num() >= max_res_num || info->bcinfo.dispatch_count >= 0xf){
 		return AC_FAIL;
	}

	for (uint32_t i = 0; i < groups.size(); i ++)
	{
		if (groups[i]->idx == ac_conf_msample_idx())
		{
			m_sample = true;
		}
		else if (groups[i]->idx == ac_conf_dsample_idx())
		{
			d_sample = true;
		}
	}

	if (res_addr != 0){
      while (step < (int)groups.size()){
			if(groups[step]->idx == ac_conf_msample_idx()|| groups[step]->idx == ac_conf_dsample_idx()){
				step ++;
				continue;
			}
			if(res_addr & (1 << groups[step]->idx) && groups[step]->priority < max){
				max = groups[step]->priority;
			}
			step ++;
		}	
	}

	for (uint32_t i = 0; i < groups.size(); i ++){
		if (groups[i]->idx == ac_conf_msample_idx()|| groups[i]->idx == ac_conf_dsample_idx()){
			continue;
		}
		if (groups[i]->priority < max && groups[i]->priority >= suit){
			suit = groups[i]->priority;
		}
	}

	if (suit == -1){
		if (res_addr == 0){
			if (m_sample)
				*idx |= (1 << ac_conf_msample_idx());
			if (d_sample)
				*idx |= (1 << ac_conf_dsample_idx());
			return AC_SUCC;
		}else{
			return AC_FAIL;
		}
	}

	*idx = 0;
	if (res_addr == 0){
		if (m_sample) {
			*idx |= (1 << ac_conf_msample_idx());
		}
		if (d_sample) {
			*idx |= (1 << ac_conf_dsample_idx());
		}
	}else{
		if (m_sample && (!info->bcinfo.bc[ac_conf_msample_idx()] 
					|| info->bcinfo.bc[ac_conf_msample_idx()]->status == 3)){
			*idx |= (1 << ac_conf_msample_idx());
		}
		if (d_sample && (!info->bcinfo.bc[ac_conf_dsample_idx()] ||
					info->bcinfo.bc[ac_conf_dsample_idx()]->status == 3)){
			*idx |= (1 << ac_conf_dsample_idx());
		}
	}

	bool no_social_follows = false;
	if ((info->parameters.cuid == 0 && info->parameters.getXsort() == std::string("social")) ||  
		(info->parameters.jx_cuid == 0 && info->parameters.getXsort() == std::string("hot")) || info->parameters.getUs() != 1 ||  
		(info->parameters.getXsort() != std::string("social") && info->parameters.getXsort() != std::string("hot")) ||
		(info->parameters.gray == 0 && info->parameters.socialtime == 0 && info->parameters.getXsort() == std::string("social"))) {
		no_social_follows = true;
	}   

	for (uint32_t i = 0; i < groups.size(); i ++){
		if (groups[i]->priority == suit){
			if (groups[i]->idx != ac_conf_msample_idx() && groups[i]->idx != ac_conf_dsample_idx()) {
				if (no_social_follows && (strcmp(get_bcgroup_name(groups[i]->idx), "social_follows") == 0)) {
						continue;
				}
                if (info->getToLevelOfBC(groups[i]->idx) == TIMEOUT_OK) {        
                    *idx |= (1 << groups[i]->idx);
                } else {
                    SLOG_WARNING("[seqid:%u] bc[%d] timeout so no dispatch it!!", info->seqid, groups[i]->idx);
                }
			}
		}
	}

	return AC_SUCC;
}

int ac_dispatch_sample(QsInfo *info, std::vector<MGroup *>&  groups, uint32_t *idx){
	for (uint32_t i = 0; i < groups.size(); i ++){
		if (groups[i]->idx == ac_conf_msample_idx()){
			*idx |= (1 << ac_conf_msample_idx());
		}
		else if (groups[i]->idx == ac_conf_dsample_idx()){
			*idx |= (1 << ac_conf_dsample_idx());
		}
	}
    return 0;
}

int ac_dispatch_mem(QsInfo* info){
	int idx = (1 << ac_conf_mem_idx());
	if (info->params.flag & QSPARAMS_FLAG_BCSPEC){
		if (info->params.bc_idx & idx){
			info->params.bc_idx = idx;
		}else{
			return -1;
		}
	}else{
		info->params.flag |= QSPARAMS_FLAG_BCSPEC;
		info->params.bc_idx = idx;
	}
	return 0;
}

//�Ƿ���Ҫ���μ���
int ac_dispatch_research(QsInfo *info){
	return 0;
}

