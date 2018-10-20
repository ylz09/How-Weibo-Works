#include <stdint.h>
#include <string.h>
#include "rbtree/rtree.h"
#include "resort.h"

#ifdef __cplusplus
extern "C" {
#endif

int resort(void *result, int num, void *env)
{
	if(num<50) return 0;
	rb_tree rbtree;
	rb_init_tree(&rbtree);
	rb_item item;
	int topnum=num>500?500:num;
	int i;
	int index=0;
	for(i = 0; i < topnum; i++)
	{
		void *ptr = R_AT(result, i);
		//uid cluster
		unsigned int uid = R_UID(ptr);
		//qi
		//unsigned int qi=R_QI(ptr);
		//source
		unsigned int source = R_SOURCE(ptr);
		//long long unsigned int mid=R_MID(ptr);

		item.key=(int)uid;
		item.value=index;
		rb_item* pitem=rb_search_item(&rbtree, &item);
		if(pitem==NULL){
			int ret=rb_insert_item(&rbtree,&item);
			if(ret<0) break;
		}
		else{
			if(index-pitem->value<20){
				R_DEL(result, i);
				continue;
			}else{
				pitem->value=index;
			}
		}
		//source cluster
#ifndef HARD_SEARCH
		if((qi&268435456)>0){
#endif
#ifdef HARD_SEARCH
		if(!R_WHITE_SOURCE(ptr)){
#endif
			item.key=(int)source;
			item.value=index;
			pitem=rb_search_item(&rbtree, &item);
			if(pitem==NULL){
				int ret=rb_insert_item(&rbtree,&item);
				if(ret<0) break;
			}
			else{
				if(index-pitem->value<10){
					R_DEL(result, i);
					continue;
				}else{
					pitem->value=index;
				}
			}
		}
		index++;
	}
	return 0;
}
#ifdef __cplusplus
}
#endif
