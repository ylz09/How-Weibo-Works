/**
 * @file ac_source_id_dict.cpp
 * @brief source id 打压词典，词典中的id满足某些条件会被打压
 * @author qiupengfei
 * @version 1.0
 * @date 2014-04-15
 */



#include "ac_source_id_dict.h"
#include <fstream>
#include "slog.h"
#include "stdlib.h"

#define MAX_INNER_UID 1024

BaseResource * SourceIDDict::Clone() {
    return new (std::nothrow) SourceIDDict();
}

/**
 * @brief 加载词典，词典每一行是一个id
 *
 * @param [in] path   : const char*
 * @param [in] dict   : const char*
 * @return  bool 
 * @author qiupengfei
 * @date 2014/04/22 16:57:36
**/
bool SourceIDDict::Load(const char * path, const char * dict)
{
    _source_dict.clear();
    int path_len = strlen(path);
    int dict_len = strlen(dict);
    int file_len = path_len + dict_len;
    char file[file_len + 1];
    int pos = 0;
    pos = snprintf(file + pos, file_len, "%s/", path);
    pos = snprintf(file + pos, file_len, "%s", dict);

    std::ifstream fin(file);

    char str[MAX_LINE_LEN];

    while (fin.getline(str, MAX_LINE_LEN)) 
    {
        uint64_t sid = 0;
        sid = atoi(str);
        SLOG_DEBUG("source_id_dict:%u", sid);
        if (sid != 0)
        {
            _source_dict.insert(sid);
        }
    }
    fin.close();
    return true;
}

int SourceIDDict::find(uint64_t sid) const
{
    std::set<uint64_t>::iterator it;
    it = _source_dict.find(sid);
    if (it != _source_dict.end())
    {
        return 1;
    }
    return 0;
}

int SourceIDDict::count()
{
    return _source_dict.size();
}

