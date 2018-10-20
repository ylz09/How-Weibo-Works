/***************************************************************************
 * 
 * Copyright (c) 2013 Sina.com, Inc. All Rights Reserved
 * 1.0
 * 
 **************************************************************************/

/**
 * @file ac_cate.cpp
 * @author hongbin2(hongbin2@staff.sina.com.cn)
 * @date 2013/04/09 
 * @version 1.0
 * @brief 
 *
 * social category(following, friends, vips) functions
 **/

#include "ac_cate.h"


void cate_filter_ori(QsInfo * info) {
    if (!info->parameters.getHasori()) {
        return;
    }

    for (int i = 0; i < info->bcinfo.get_doc_num();i++) {
        SrmDocInfo * doc = info->bcinfo.get_doc_at(i);
        if (doc && !doc->isOri()) {
            info->bcinfo.del_doc(doc);
        }
    }

    info->bcinfo.refresh();
    
}
