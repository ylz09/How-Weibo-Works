#include "ac_include.h"
#include "ac_query_lib.h"

#define BS_FIELD_NUM	12
#define BS_FIELDS {\
	FIELD_DOCID, \
	FIELD_BCIDX, \
	FIELD_GROUP, \
	FIELD_DOMAIN, \
	FIELD_ATTRLOW32, \
	FIELD_ATTRHIGH32, \
	FIELD_ATTR64, \
	FIELD_ATTR96, \
	FIELD_RANK, \
	FIELD_DOCATTR, \
	FIELD_SCORE, \
	FIELD_RELATION,\
}

extern void field_build_idx(MFieldInfo *info);
#ifndef WIN32
static char* strlwr(char *p){
	while(*p){
		*p = tolower(*p);
		p ++;
	}
	return NULL;
}
#endif

//load fields
int load_fields(char *filename, TConf *conf){
	char line[256];
	FILE *fp = NULL;
	char *val, *match = "[FIELDID=";
	int match_len = strlen(match);
	int ret, default_num = 0;
	MField *field = NULL;
	uint8_t id, idx;
	int i;
	char *nomark, *mark, *p;
	char *marks[32], *nomarks[32];
	int nomark_num = 0, mark_num = 0;
	const char *field_names[] = BS_FIELDS;

	char* content = NULL;
	char* status = NULL;

	nomark = conf_get_str(conf, "nomark_fields", "");
	mark = conf_get_str(conf, "mark_fields", "");

	if(*nomark){
		p = nomark;
		nomark_num = 0;
		while(*p == ',')p ++;
		if(*p)nomarks[nomark_num++] = p;
		while(*p){
			if(*p == ','){
				while(*p == ',')*p++=0;
				nomarks[nomark_num++] = p;
			}
			p ++;
		}
	}

    if(*mark){
		p = mark;
		mark_num = 0;
		while(*p == ',')p ++;
		if(*p)marks[mark_num++] = p;
		while(*p){
			if(*p == ','){
				while(*p == ',')*p++=0;
				marks[mark_num++] = p;
			}
			p ++;
		}
	}

	content = conf_get_str(conf, "filter_field", "CONTENT");
	status = conf_get_str(conf, "filter_status", "STATUS");
	
	fp = fopen(filename, "rb");
	if(fp == NULL){
		slog_write(LL_WARNING, "?ļ??????? %s", filename);
		goto LB_ERROR;
	}
	while(!feof(fp)){
		if(fgets(line, 256, fp) == NULL){
			break;
		}
		ret = strlen(line) - 1;
		while(line[ret] == '\r' || line[ret] == '\n' || line[ret] == ' ' || line[ret] == '\t'){
			line[ret--] = 0;
		}
		if(memcmp(line, match, match_len) == 0){
			id = (uint8_t)atoi(line + match_len);
			if(id == 0){
				goto LB_ERROR;
			}
			field = conf->field_info.fields + id;
			field->id = id;
		}else if(memcmp(line, "NAME=", 5) == 0){
			strcpy(field->label, line + 5);
		}else if(memcmp(line, "TYPE=", 5) == 0){
			val = line + 5;
			if(strcmp(val, "STRING") == 0){
				field->type = TYPE_CHAR;
			}else if(strcmp(val, "INT") == 0){
				field->type = TYPE_INT;
			}else if(strcmp(val, "INT64") == 0){
				field->type = TYPE_LONG;
			}
		}else if(memcmp(line, "BEGINBIT=", 9) == 0){
			field->bit_start = atoi(line + 9);
		}else if(memcmp(line, "ENDBIT=", 7) == 0){
			field->bit_end = atoi(line + 7);
		}else if(memcmp(line, "DEFAULT_IDX=", 12) == 0){
			if(atoi(line + 12) == 1){
				field->default_idx = ++default_num;
			}
		}

		if(field->type && *field->label){
			if(field->status == 0){
				if(field->type == TYPE_CHAR){
					if(mark_num){
						field->mark = 0;
						for(i = 0; i < mark_num; i ++){
							if(strcmp(marks[i], field->label) == 0){
								field->mark = 1;
								break;
							}
						}
					}else if(nomark_num){
						field->mark = 1;
						for(i = 0; i < nomark_num; i ++){
							if(strcmp(nomarks[i], field->label) == 0){
								field->mark = 0;
								break;
							}
						}
					}else{
						field->mark = 1;
					}
				}
				field->status = 1;
				strcpy(field->low_label, field->label);
				strlwr(field->low_label);
			}
			if (conf->filter_id == 0 && field->type == TYPE_CHAR){
				if (strcmp(content, field->label) == 0){
					conf->filter_id = field->id;
				}
			}	
			if (conf->status_id == 0 && field->type == TYPE_INT){
				if (strcmp(status, field->label) == 0){
					conf->status_id = field->id;		
				}	
			}	
		}
	}
	fclose(fp);
	idx = MAX_FIELD_NUM - BS_FIELD_NUM;
	for(i = 0; i < BS_FIELD_NUM; i ++){
		field = conf->field_info.fields + idx;
		field->status = 1;
		field->id = idx;
		strcpy(field->label, field_names[i]);
		field->type = TYPE_INT;
		field->mark = 0;
		field->extra = 1;
		conf->field_ids[i] = idx;
		idx ++;
	}
	field_build_idx(&conf->field_info);
	return 0;

LB_ERROR:
	if(fp){
		fclose(fp);
	}
	return -1;
}
