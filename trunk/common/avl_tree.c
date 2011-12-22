#include "avl_tree.h"

int avl_tree_init(AVLTreeInfo *tree, FreeDataFunc free_data_func, \
	CompareFunc compare_func)
{
	tree->root = NULL;
	tree->count = 0;
	tree->free_data_func = free_data_func;
	tree->compare_func = compare_func;
	return pthread_rwlock_init(&(tree->rwlock), NULL);
}

static void avl_tree_destroy_loop(FreeDataFunc free_data_func, \
		AVLTreeNode *pCurrentNode)
{
	if (pCurrentNode->left != NULL)
	{
		avl_tree_destroy_loop(free_data_func, pCurrentNode->left);
	}
	if (pCurrentNode->right != NULL)
	{
		avl_tree_destroy_loop(free_data_func, pCurrentNode->right);
	}
	
	if (free_data_func != NULL)
	{
		free_data_func(pCurrentNode->data);
	}
	free(pCurrentNode);
}

void avl_tree_destroy(AVLTreeInfo *tree)
{
	if (tree == NULL)
	{
		return;
	}

	if (tree->root != NULL)
	{
		avl_tree_destroy_loop(tree->free_data_func, tree->root);
		tree->root = NULL;
		tree->count = 0;
	}

	pthread_rwlock_destroy(&(tree->rwlock));
}

static AVLTreeNode *createTreeNode(AVLTreeNode *pParentNode, void *target_data)
{
	AVLTreeNode *pNewNode;
	pNewNode = (AVLTreeNode *)malloc(sizeof(AVLTreeNode));
	if (pNewNode == NULL)
	{
		printf("malloc fail!\n");
		return NULL;
	}

	pNewNode->left = pNewNode->right = NULL;
	pNewNode->data = target_data;
	pNewNode->balance = 0;

	return pNewNode;
}

static void avlRotateLeft(AVLTreeNode *pRotateNode, AVLTreeNode **ppRaiseNode)
{
	*ppRaiseNode = pRotateNode->right;
	pRotateNode->right = (*ppRaiseNode)->left;
	(*ppRaiseNode)->left = pRotateNode;
}

static void avlRotateRight(AVLTreeNode *pRotateNode, AVLTreeNode **ppRaiseNode)
{
	*ppRaiseNode = pRotateNode->left;
	pRotateNode->left = (*ppRaiseNode)->right;
	(*ppRaiseNode)->right = pRotateNode;
}

static void avlLeftBalanceWhenInsert(AVLTreeNode **pTreeNode, int *taller)
{
	AVLTreeNode *leftsub;
	AVLTreeNode *rightsub;

	leftsub = (*pTreeNode)->left;
	switch (leftsub->balance)
	{
		case -1 :
			(*pTreeNode)->balance = leftsub->balance = 0;
			avlRotateRight (*pTreeNode, pTreeNode);
			*taller = 0;
			break;
		case 0 :
			printf("树已经平衡化!!!!!!!\n");
			break;
		case 1 :
			rightsub = leftsub->right;
			switch ( rightsub->balance )
			{
				case -1:
					(*pTreeNode)->balance = 1;
					leftsub->balance = 0;
					break;
				case 0 :
					(*pTreeNode)->balance = leftsub->balance = 0;
					break;
				case 1 :
					(*pTreeNode)->balance = 0;
					leftsub->balance = -1;
					break;
			}

			rightsub->balance = 0;
			avlRotateLeft( leftsub, &((*pTreeNode)->left));
			avlRotateRight(*pTreeNode, pTreeNode);
			*taller = 0;
	}
}

static void avlRightBalanceWhenInsert(AVLTreeNode **pTreeNode, int *taller)
{
	AVLTreeNode *rightsub;
	AVLTreeNode *leftsub;

	rightsub = (*pTreeNode)->right;
	switch (rightsub->balance)
	{
		case 1: 
			(*pTreeNode)->balance = rightsub->balance = 0;
			avlRotateLeft(*pTreeNode, pTreeNode);
			*taller = 0;
			break;
		case 0: 
			printf("树已经平衡化!!!!!!!\n");
			break; 
		case -1:
			leftsub = rightsub->left;
			switch (leftsub->balance)
			{ 
				case 1 :
					(*pTreeNode)->balance = -1;
					rightsub->balance = 0;
					break;
				case 0 :
					(*pTreeNode)->balance = rightsub->balance = 0;
					break;
				case -1 :
					(*pTreeNode)->balance = 0;
					rightsub->balance = 1;
					break;
			}

			leftsub->balance = 0;
			avlRotateRight(rightsub, &((*pTreeNode)->right));
			avlRotateLeft(*pTreeNode, pTreeNode);
			*taller = 0;
	}
}

static int avl_tree_insert_loop(CompareFunc compare_func, AVLTreeNode **pCurrentNode, \
		void *target_data, int *taller)
{
	int nCompRes;
	int success;

	if (*pCurrentNode == NULL )
	{
		*pCurrentNode = createTreeNode(*pCurrentNode, target_data);
		if (*pCurrentNode != NULL)
		{
			*taller = 1;
			return 1;
		}
		else
		{
			return 0;
		}
	}

	nCompRes = compare_func((*pCurrentNode)->data, target_data);
	if (nCompRes > 0)
	{
		success = avl_tree_insert_loop(compare_func, \
				&((*pCurrentNode)->left), target_data, taller);
		if (*taller != 0)
		{
			switch ((*pCurrentNode)->balance)
			{
				case -1:
					avlLeftBalanceWhenInsert(pCurrentNode, taller);
					break;
				case 0:
					(*pCurrentNode)->balance = -1;
					break;
				case 1:
					(*pCurrentNode)->balance = 0;
					*taller = 0;
					break;
			}
		}
	}
	else if (nCompRes < 0)
	{
		success = avl_tree_insert_loop(compare_func, \
				&((*pCurrentNode)->right), target_data, taller);
		if (*taller != 0)
		{
			switch ((*pCurrentNode)->balance)
			{
				case -1: 
					(*pCurrentNode)->balance = 0;
					*taller = 0;
					break;
				case 0:
					(*pCurrentNode)->balance = 1; 
					break;
				case 1:
					avlRightBalanceWhenInsert(pCurrentNode, taller);
					break;
			}
		}
	}
	else
	{
		return 0;
	}

	return success;
}

int avl_tree_insert(AVLTreeInfo *tree, void *data)
{
	int taller;
	int success;

	taller = 0;

	pthread_rwlock_wrlock(&(tree->rwlock));
	success = avl_tree_insert_loop(tree->compare_func, &(tree->root), \
					data, &taller);
	if (success)
	{
		tree->count++;
	}
	pthread_rwlock_unlock(&(tree->rwlock));
	return success;
}

static int avl_tree_replace_loop(CompareFunc compare_func, \
		FreeDataFunc free_data_func, AVLTreeNode **pCurrentNode, \
		void *target_data, int *taller)
{
	int nCompRes;
	int success;

	if (*pCurrentNode == NULL )
	{
		*pCurrentNode = createTreeNode(*pCurrentNode, target_data);
		if (*pCurrentNode != NULL)
		{
			*taller = 1;
			return 1;
		}
		else
		{
			return 0;
		}
	}

	nCompRes = compare_func((*pCurrentNode)->data, target_data);
	if (nCompRes > 0)
	{
		success = avl_tree_replace_loop(compare_func, free_data_func, 
				&((*pCurrentNode)->left), target_data, taller);
		if (*taller != 0)
		{
			switch ((*pCurrentNode)->balance)
			{
				case -1:
					avlLeftBalanceWhenInsert(pCurrentNode, taller);
					break;
				case 0:
					(*pCurrentNode)->balance = -1;
					break;
				case 1:
					(*pCurrentNode)->balance = 0;
					*taller = 0;
					break;
			}
		}
	}
	else if (nCompRes < 0)
	{
		success = avl_tree_replace_loop(compare_func, free_data_func, 
				&((*pCurrentNode)->right), target_data, taller);
		if (*taller != 0) 
		{
			switch ((*pCurrentNode)->balance)
			{
				case -1 : 
					(*pCurrentNode)->balance = 0;
					*taller = 0;
					break;
				case 0 :
					(*pCurrentNode)->balance = 1; 
					break;
				case 1 :
					avlRightBalanceWhenInsert(pCurrentNode, taller);
					break;
			}
		}
	}
	else
	{
		if (free_data_func != NULL)
		{
			free_data_func((*pCurrentNode)->data);
		}
		(*pCurrentNode)->data = target_data;
		return 0;
	}

	return success;
}

int avl_tree_replace(AVLTreeInfo *tree, void *data)
{
	int taller;
	int success;

	taller = 0;

	pthread_rwlock_wrlock(&(tree->rwlock));
	success = avl_tree_replace_loop(tree->compare_func, \
			tree->free_data_func, &(tree->root), data, &taller);
	if (success)
	{
		tree->count++;
	}
	pthread_rwlock_unlock(&(tree->rwlock));

	return success;
}

static void *avl_tree_find_loop(CompareFunc compare_func, \
		AVLTreeNode *pCurrentNode, void *target_data)
{
	int nCompRes;
	nCompRes = compare_func(pCurrentNode->data, target_data);
	if (nCompRes > 0)
	{
		if (pCurrentNode->left == NULL)
		{
			return NULL;
		}
		else
		{
			return avl_tree_find_loop(compare_func, \
				pCurrentNode->left, target_data);
		}
	}
	else if (nCompRes < 0)
	{
		if (pCurrentNode->right == NULL)
		{
			return NULL;
		}
		else
		{
			return avl_tree_find_loop(compare_func, \
				pCurrentNode->right, target_data);
		}
	}
	else
	{
		return pCurrentNode->data;
	}
}

void *avl_tree_find(AVLTreeInfo *tree, void *target_data)
{
	void *found;

	pthread_rwlock_rdlock(&(tree->rwlock));
	if (tree->root == NULL)
	{
		found = NULL;
	}
	else
	{
		found = avl_tree_find_loop(tree->compare_func, \
			tree->root, target_data);
	}
	pthread_rwlock_unlock(&(tree->rwlock));

	return found;
}

static void avlLeftBalanceWhenDelete(AVLTreeNode **pTreeNode, int *shorter)
{
	AVLTreeNode *leftsub;
	AVLTreeNode *rightsub;

	leftsub = (*pTreeNode)->left;
	switch (leftsub->balance)
	{
		case -1:
			(*pTreeNode)->balance = leftsub->balance = 0;
			avlRotateRight (*pTreeNode, pTreeNode);
			break;
		case 0:
			leftsub->balance = 1;
			avlRotateRight(*pTreeNode, pTreeNode);
			*shorter = 0;
			break;
		case 1:
			rightsub = leftsub->right;
			switch ( rightsub->balance )
			{
				case -1:
					(*pTreeNode)->balance = 1;
					leftsub->balance = 0;
					break;
				case 0 :
					(*pTreeNode)->balance = leftsub->balance = 0;
					break;
				case 1 :
					(*pTreeNode)->balance = 0;
					leftsub->balance = -1;
					break;
			}

			rightsub->balance = 0;
			avlRotateLeft( leftsub, &((*pTreeNode)->left));
			avlRotateRight(*pTreeNode, pTreeNode);
			break;
	}
}

static void avlRightBalanceWhenDelete(AVLTreeNode **pTreeNode, int *shorter)
{
	AVLTreeNode *rightsub;
	AVLTreeNode *leftsub;

	rightsub = (*pTreeNode)->right;
	switch (rightsub->balance)
	{
		case 1:
			(*pTreeNode)->balance = rightsub->balance = 0;
			avlRotateLeft(*pTreeNode, pTreeNode);
			break;
		case 0:
			rightsub->balance = -1;
			avlRotateLeft(*pTreeNode, pTreeNode);
			*shorter = 0;
			break;
		case -1:
			leftsub = rightsub->left;
			switch (leftsub->balance)
			{
				case 1:
					(*pTreeNode)->balance = -1;
					rightsub->balance = 0;
					break;
				case 0:
					(*pTreeNode)->balance = rightsub->balance = 0;
					break;
				case -1:
					(*pTreeNode)->balance = 0;
					rightsub->balance = 1;
					break;
			}

			leftsub->balance = 0;
			avlRotateRight(rightsub, &((*pTreeNode)->right));
			avlRotateLeft(*pTreeNode, pTreeNode);
			break;
	}
}

static void *avl_tree_delete_loop(CompareFunc compare_func, \
		AVLTreeNode **pCurrentNode, void *target_data, int *shorter, \
		AVLTreeNode *pDeletedDataNode)
{
	int nCompRes;
	void *pResultData;
	AVLTreeNode *leftsub;
	AVLTreeNode *rightsub;

	if (*pCurrentNode == NULL)
	{
		return NULL;
	}

	if (pDeletedDataNode != NULL)
	{
		if ((*pCurrentNode)->right == NULL)
		{
			pResultData = pDeletedDataNode->data;
			pDeletedDataNode->data = (*pCurrentNode)->data;
			leftsub = (*pCurrentNode)->left;

			free(*pCurrentNode);
			*pCurrentNode = leftsub;
			*shorter = 1;
			return pResultData;
		}

		nCompRes = -1;	//在右分支中寻找直接前驱结点
	}
	else
	{
		nCompRes = compare_func((*pCurrentNode)->data, target_data);
	}

	if (nCompRes > 0)
	{
		pResultData = avl_tree_delete_loop(compare_func, \
				&((*pCurrentNode)->left), target_data, \
				shorter, pDeletedDataNode);
		if (*shorter != 0)
		{
			switch ((*pCurrentNode)->balance)
			{
			  case -1:
				(*pCurrentNode)->balance = 0;
				break;
			  case 0:
				(*pCurrentNode)->balance = 1;
				*shorter = 0;
				break;
			  case 1:
				avlRightBalanceWhenDelete(pCurrentNode, shorter);
				break;
			}
		}
		return pResultData;
	}
	else if (nCompRes < 0)
	{
		pResultData = avl_tree_delete_loop(compare_func, \
				&((*pCurrentNode)->right), target_data, \
				shorter, pDeletedDataNode);
		if (*shorter != 0)
		{
		    switch ((*pCurrentNode)->balance)
			{
		        case -1:
		          avlLeftBalanceWhenDelete(pCurrentNode, shorter);
		          break;
		        case 0:
		          (*pCurrentNode)->balance = -1;
				  *shorter = 0;
		          break;
		        case 1:
		          (*pCurrentNode)->balance = 0;
		          break;
		    }
		}
		return pResultData;
	}
	else
	{		
		leftsub= (*pCurrentNode)->left;
		rightsub = (*pCurrentNode)->right;
		pResultData = (*pCurrentNode)->data;
		if (leftsub == NULL)
		{
			free(*pCurrentNode);
			*pCurrentNode = rightsub;
		}
		else if (rightsub == NULL)
		{
			free(*pCurrentNode);
			*pCurrentNode = leftsub;
		}
		else
		{
			//printf("find the prior.\n");
			avl_tree_delete_loop(compare_func, \
				&((*pCurrentNode)->left), target_data, \
				shorter, *pCurrentNode);
			if (*shorter != 0)
			{
				switch ((*pCurrentNode)->balance)
				{
				  case -1:
					(*pCurrentNode)->balance = 0;
					break;
				  case 0:
					(*pCurrentNode)->balance = 1;
					*shorter = 0;
					break;
				  case 1:
					avlRightBalanceWhenDelete(pCurrentNode, shorter);
					break;
				}
			}
			return pResultData;
		}

		*shorter = 1;
		return pResultData;
	}
}

void *avl_tree_delete(AVLTreeInfo *tree, void *data)
{
	void *pResultData;
	int shorter;

	shorter = 0;

	pthread_rwlock_wrlock (&(tree->rwlock));
	pResultData = avl_tree_delete_loop(tree->compare_func, \
			&(tree->root), data, &shorter, NULL);
	if (pResultData != NULL)
	{
		tree->count--;
	}
	pthread_rwlock_unlock(&(tree->rwlock));

	return pResultData;
}

static void avl_tree_walk_loop(DataOpFunc data_op_func, AVLTreeNode *pCurrentNode)
{
	if (pCurrentNode->left != NULL)
	{
		avl_tree_walk_loop(data_op_func, pCurrentNode->left);
	}

	if (pCurrentNode->balance >= -1 && pCurrentNode->balance <= 1)
	{
		data_op_func(pCurrentNode->data);
		//printf("==%d\n", pCurrentNode->balance);
	}
	else
	{
		data_op_func(pCurrentNode->data);
		printf("==bad %d!!!!!!!!!!!!\n", pCurrentNode->balance);
	}

	if (pCurrentNode->right != NULL)
	{
		avl_tree_walk_loop(data_op_func, pCurrentNode->right);
	}
}

void avl_tree_walk(AVLTreeInfo *tree, DataOpFunc data_op_func)
{
	pthread_rwlock_rdlock(&(tree->rwlock));
	if (tree->root == NULL)
	{
		return;
	}
	avl_tree_walk_loop(data_op_func, tree->root);
	pthread_rwlock_unlock(&(tree->rwlock));
}

