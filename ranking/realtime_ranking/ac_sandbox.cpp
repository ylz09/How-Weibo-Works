/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * $Id$ 
 * 
 **************************************************************************/
 
/**
 * @file ac_sandbox.cpp
 * @author wuhua1(minisearch@staff.sina.com.cn)
 * @date 2014/10/27 03:34:28
 * @version $Revision$ 
 * @brief 
 *  
 **/

#include <intention_def.h>
#include <dacore/auto_reference.h>

#include "ac_type.h"
#include "config.h"
#include "ac_hot_ext_data_parser.h"
#include "ac_sandbox.h"
#include "ac_intervene_dict.h"
#include "search_dict.h"

extern ACServer *g_server;

void static CheckQi20(QsInfo* info, std::set<uint64_t>* already_del_mid, bool need_refresh) {
	int delcount = 0;
	int ndocs = info->bcinfo.get_doc_num();
	register bool delflag = false;
	time_t delay_time = time(NULL) - conf_get_int(g_server->conf_current, "qi20_delay_time", 604800);
	for (int i = 0; i < ndocs; ++i) {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (doc == NULL) {
			continue;
		}
		if (doc->rank >= delay_time) {
			AcExtDataParser attr(doc);
			if (attr.get_qi() & (0x1 << 20)) {
				++delcount;
				DOC_WRITE_ACDEBUG(doc, "qi(20b)");
				if (doc->docid == info->parameters.debugid) {
					AC_WRITE_LOG(info, " {%lu is qi(20b)} ", doc->docid);
				}
				
                if (already_del_mid && !(attr.get_digit_attr() & (0x1 << 2))) {
                    already_del_mid->insert(doc->docid);
                }
				info->bcinfo.del_doc(doc);
				if (!delflag) {
					delflag = true;
				}
				continue;
			}
        }
    }
	if (need_refresh && delflag) {
		info->bcinfo.refresh();
	}
}

int ac_sandbox(QsInfo* info, std::set<uint64_t>* already_del_mid, bool need_refresh)
{
    CheckQi20(info, already_del_mid, need_refresh);
	auto_reference<ACStringSet> sandbox_query("sandbox_query");
	if (*sandbox_query == NULL) {
		return -1;
	}

	if (!(info->intention & INTENTION_STAR) && !sandbox_query->test(info->params.key)) {
		return 1;
	}

	auto_reference<ACLongSet> sandbox_uids("sandbox_uids");
	if (*sandbox_uids == NULL) {
		return -1;
	}
	
	auto_reference<DASearchDict> suspect_uids("suspect_uids");

	int delcount = 0;
	int ndocs = info->bcinfo.get_doc_num();
	register bool delflag = false;
	time_t delay_time = time(NULL) - conf_get_int(g_server->conf_current, "suspect_delay_time", 172800);
	for (int i = 0; i < ndocs; ++i) {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (doc == NULL) {
			continue;
		}

		int64_t uid = GetDomain(doc);
		if (sandbox_uids->find(uid)) {
			++delcount;
			DOC_WRITE_ACDEBUG(doc, "in sanbox");
			if (doc->docid == info->parameters.debugid) {
				AC_WRITE_LOG(info, " {%lu in sanbox} ", doc->docid);
			}

			AcExtDataParser attr(doc);
            if (already_del_mid && !(attr.get_digit_attr() & (0x1 << 2))) {
                already_del_mid->insert(doc->docid);
            }
			info->bcinfo.del_doc(doc);
			if (!delflag) {
				delflag = true;
			}

			continue;
		}

		if (*suspect_uids) {
			int ret = suspect_uids->Search(uid);
			if (ret == -1) {
				continue;
			}

			if (ret == 0 || doc->rank >= delay_time) {
				++delcount;
				DOC_WRITE_ACDEBUG(doc, "in suspect_uids:%d", ret);
				if (doc->docid == info->parameters.debugid) {
					AC_WRITE_LOG(info, " {%lu in suspect_uids:%d} ", doc->docid, ret);
				}

                AcExtDataParser attr(doc);
                if (already_del_mid && !(attr.get_digit_attr() & (0x1 << 2))) {
                    already_del_mid->insert(doc->docid);
                }
				info->bcinfo.del_doc(doc);
				if (!delflag) {
					delflag = true;
				}
			}
		}
	}

	AC_WRITE_LOG(info, " {sandbox:%d} ", delcount);

	if (need_refresh && delflag) {
		info->bcinfo.refresh();
	}

	return 0;
}

int ac_black_mids_mask(QsInfo* info, std::set<uint64_t>* already_del_mid, bool need_refresh)
{
	auto_reference<ACLongSet> mask_mids("mask_mids");
	if (*mask_mids == NULL) {
		return -1;
	}

	int delcount = 0;
	int ndocs = info->bcinfo.get_doc_num();
	register bool delflag = false;
	for (int i = 0; i < ndocs; ++i) {
		SrmDocInfo *doc = info->bcinfo.get_doc_at(i);
		if (doc == NULL) {
			continue;
		}

		if (mask_mids->find(doc->docid)) {
			++delcount;
			DOC_WRITE_ACDEBUG(doc, "in mask_mids");
			if (doc->docid == info->parameters.debugid) {
				AC_WRITE_LOG(info, " {%lu in mask_mids} ", doc->docid);
			}

			AcExtDataParser attr(doc);
            if (already_del_mid && !(attr.get_digit_attr() & (0x1 << 2))) {
                already_del_mid->insert(doc->docid);
            }
			info->bcinfo.del_doc(doc);
			if (!delflag) {
				delflag = true;
			}
		}
	}

	AC_WRITE_LOG(info, " {midmask:%d} ", delcount);

	if (need_refresh && delflag) {
		info->bcinfo.refresh();
	}

	return 0;
}

int ac_sandbox_doc(QsInfo* info, SrmDocInfo *doc, int64_t uid, time_t delay_time,
            const auto_reference<DASearchDict>& suspect_uids) {
    if (doc == NULL) {
        return 0;
    }
    if (*suspect_uids) {
        int ret = suspect_uids->Search(uid);
        if (ret == -1) {
            return 0;
        }
        if (ret == 0 || doc->rank >= delay_time) {
            return 2;
        }
    }

return 0;
}
