/***************************************************************************
 * 
 * Copyright (c) 2013 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/

/**
 * @file ac_truncate.cpp
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2012/04/26
 * @version 1.0
 * @brief 
 * truncate the weibo size to 2000
 *  
 **/

#ifndef __AC_TRUNCATE_H_
#define __AC_TRUNCATE_H_

#include "ac_truncate.h"



void truncate_size(QsInfo * info) {
    if (info == NULL) {
        SLOG_FATAL("[seqid:%u] can not init info(NULL)",
                   info->seqid);
    }
    
    for (int i = 2000; i < info->bcinfo.get_doc_num(); i++) {
        SrmDocInfo * pdoc = info->bcinfo.get_doc_at(i);
        if (pdoc == NULL) {
            continue;
        }
        info->bcinfo.del_doc(pdoc);
    }

    info->bcinfo.refresh();
}

#endif
