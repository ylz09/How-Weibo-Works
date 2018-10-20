#ifndef __RB_TREE_H__
#define __RB_TREE_H__

#include <stdlib.h>
#include "rbtree.h"
#include <stdio.h>

#define MAX_RB_ITEM 3000


typedef struct _rb_item {
	int key;		///< 节点上的key
	int value;              ///< 节点上的value  
	struct rb_node rb_node; ///< 节点
}rb_item;

typedef struct _rb_tree {
	struct rb_root rbroot;		///< 根节点
	rb_item rbitem[MAX_RB_ITEM];	///< 节点数组
	int num;			///< 节点个数
}rb_tree;

//void delete_item(rb_tree * rbtree, rb_item *item);

/*
return:
	1:已经存在
	0:正常插入
       -1:已满
*/
int rb_insert_item(rb_tree * rbtree, rb_item *item);

/*
return:
	非NULL:找到，返回值的指针，可通过指针修改value
	NULL:没找到
*/
rb_item* rb_search_item(rb_tree * rbtree, rb_item *pitem);

void rb_init_tree(rb_tree * rbtree);

#endif
