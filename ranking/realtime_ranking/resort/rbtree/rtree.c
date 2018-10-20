#include"rtree.h"

/*
void delete_item(rb_tree * rbtree, rb_item *item);
{
	return rb_erase(&item->rb_node, &rbtree->rbroot);
}
*/

int rb_insert_item(rb_tree * rbtree, rb_item *item)
{
	struct rb_node **p = &(rbtree->rbroot.rb_node);
	struct rb_node *parent = NULL;
	rb_item *it;
	int num=rbtree->num;
	while(*p)
	{
		parent = *p;
		it = rb_entry(parent, rb_item, rb_node);

		if(item->key < it->key)
			p = &(*p)->rb_left;
		else if(item->key > it->key)
			p = &(*p)->rb_right;
		else{//exist
			return 1;
		}
	}
	if(num>=MAX_RB_ITEM) return -1;
	rbtree->rbitem[num]=*item;
	rb_link_node(&(rbtree->rbitem[num].rb_node), parent, p);
	rb_insert_color(&(rbtree->rbitem[num].rb_node), &(rbtree->rbroot));
	rbtree->num++;
	return 0;
}

//int search_item(rb_tree * rbtree, rb_item *pitem)
rb_item* rb_search_item(rb_tree * rbtree, rb_item *pitem)
{
	struct rb_node *n = rbtree->rbroot.rb_node;
	rb_item *item;

	while (n)
	{
		item = rb_entry(n, rb_item, rb_node);

		if (pitem->key < item->key)
			n = n->rb_left;
		else if (pitem->key > item->key)
			n = n->rb_right;
		else{
			return item;
		}
	}
	return NULL;
}

void rb_init_tree(rb_tree * rbtree)
{
	rbtree->num=0;
	rbtree->rbroot.rb_node=NULL;
}


