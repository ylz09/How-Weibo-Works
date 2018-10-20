#include"rtree.h"

int main()
{
	rb_tree rbtree;
	printf("rbtree size: %d\n",sizeof(rbtree));
	printf("rbnode size: %d\n",sizeof(struct rb_node));
	rb_item item;
	int ret;
	int type,key,value;
	while(1){
		rb_init_tree(&rbtree);
		while(1){
			scanf("%d %d %d",&type,&key,&value);
			if(type==-1) break;
			if(type==1){//insert
				item.key=key;
				item.value=value;
				ret=rb_insert_item(&rbtree, &item);
				printf("ret:%d\n",ret);
				if(ret==0)
					printf("Insert success!\n");
				else if(ret==1)
					printf("Key exist!\n");
				else{
					printf("Error!\n");
				}
			}
			else if(type==2){//find
				rb_item item_key;
				item_key.key=key;
				//int ret=search_item(&rbtree, pitem_key);
				rb_item* pitem=rb_search_item(&rbtree, &item_key);
				if(pitem==NULL)
					printf("key %d not found!\n",key);
				else{
					printf("key %d found value: %d\n",key,pitem->value);
				}
			}
			else if(type==3){//modify
				rb_item item_key;
				rb_item* pitem_key=&item_key;
				item_key.key=key;
				rb_item* pitem=rb_search_item(&rbtree, pitem_key);
				if(pitem==NULL)
					printf("key %d not found!\n",key);
				else{
					printf("key %d found value:%d modify to:%d\n",key,pitem->value,value);
					pitem->value=value;
				}
			}
		}
	}
	return 0;
}

