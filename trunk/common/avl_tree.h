
#ifndef AVL_TREE_H
#define AVL_TREE_H

#include <stdio.h>
#include <stdlib.h>
#include "common_define.h"

typedef struct tagAVLTreeNode {
	void *data;
	struct tagAVLTreeNode *left;
	struct tagAVLTreeNode *right;
	byte balance;
} AVLTreeNode;

typedef void (*DataOpFunc) (void *data);

typedef struct tagAVLTreeInfo {
	AVLTreeNode *root;
	int count;
	FreeDataFunc free_data_func;
	CompareFunc compare_func;
} AVLTreeInfo;

#ifdef __cplusplus
extern "C" {
#endif

void avl_tree_init(AVLTreeInfo *tree, FreeDataFunc free_data_func, \
	CompareFunc compare_func);
void avl_tree_destroy(AVLTreeInfo *tree);

int avl_tree_insert(AVLTreeInfo *tree, void *data);
int avl_tree_replace(AVLTreeInfo *tree, void *data);
void *avl_tree_delete(AVLTreeInfo *tree, void *data);
void *avl_tree_find(AVLTreeInfo *tree, void *target_data);
void avl_tree_walk(AVLTreeInfo *tree, DataOpFunc data_op_func);

#ifdef __cplusplus
}
#endif

#endif
