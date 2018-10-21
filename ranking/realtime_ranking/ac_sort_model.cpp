#include "ac_sort_model.h"


bool fwnum_cmp(const SrmDocInfo * a, const SrmDocInfo *b) {
    if (a && !b) {
        return true;
    }

    if (!a && b) {
        return false;
    }

    if (!a && !b) {
        return true;
    }

    AcExtDataParser attr_a(a);
    AcExtDataParser attr_b(b);
    uint64_t fwnum_a = attr_a.get_fwnum();
    uint64_t fwnum_b = attr_b.get_fwnum();
    if (a->extra) {
        if (b->extra) {
            return fwnum_a > fwnum_b;
        }
        return true;
    }
    if (b->extra) {
        return false;
    }

    return (a->value > b->value) ? true: (a->rank > b->rank);
}

bool FwnumSortCmp::operator()(const SrmDocInfo * a, const SrmDocInfo*b) const {
    return fwnum_cmp(a, b);
}

bool cmtnum_cmp(const SrmDocInfo * a, const SrmDocInfo *b) {
    AcExtDataParser attr_a(a);
    AcExtDataParser attr_b(b);
    uint64_t cmtnum_a = attr_a.get_cmtnum();
    uint64_t cmtnum_b = attr_b.get_cmtnum();
    if (a->extra)
    {
        if (b->extra)
        {
            return cmtnum_a > cmtnum_b;
        }
        return true;
    }
    if (b->extra)
    {
        return false;
    }

    return (a->value > b->value) ? true: (a->rank > b->rank);
}

bool CmtnumSortCmp::operator()(const SrmDocInfo * a, const SrmDocInfo*b) const {
    return cmtnum_cmp(a, b);
}


