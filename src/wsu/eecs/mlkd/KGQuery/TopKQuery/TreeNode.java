package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Node;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;

public class TreeNode<T> {
	private TreeNode<T> leftChild;
	private TreeNode<T> rightChild;
	private TreeNode<T> parent = null;
	public boolean isComputed = false;
	public T data = null;
	public int levelInCalcTree = 0;

	public TreeNode<CalculationNode> copy() {
		TreeNode<CalculationNode> newTreeNode = new TreeNode<CalculationNode>(((CalculationNode) this.data).copy());

		if (this.getChildren() != null) {
			TreeNode<CalculationNode> newRightTreeNode = this.getRightChild().copy();
			TreeNode<CalculationNode> newLeftTreeNode = this.getLeftChild().copy();
			newTreeNode.addChildren(newLeftTreeNode, newRightTreeNode);

			newLeftTreeNode.levelInCalcTree = this.getLeftChild().levelInCalcTree;
			newRightTreeNode.levelInCalcTree = this.getRightChild().levelInCalcTree;
			newLeftTreeNode.data.levelInCalcTree = ((CalculationNode) this.getLeftChild().data).levelInCalcTree;
			newRightTreeNode.data.levelInCalcTree = ((CalculationNode) this.getRightChild().data).levelInCalcTree;
		}
		newTreeNode.levelInCalcTree = ((CalculationNode) this.data).levelInCalcTree;
		return newTreeNode;
	}

	public TreeNode() {

	}

	public TreeNode(T data) {
		this.data = data;
	}

	public TreeNode(T data, TreeNode<T> parent) {
		this.data = data;
		this.parent = parent;
	}

	public List<TreeNode<T>> getChildren() {
		if (this.leftChild == null && this.rightChild == null) {
			return null;
		}
		List<TreeNode<T>> childrenList = new ArrayList<TreeNode<T>>();
		childrenList.add(this.leftChild);
		childrenList.add(this.rightChild);
		return childrenList;
	}

	public TreeNode<T> getLeftChild() {
		return leftChild;
	}

	public TreeNode<T> getRightChild() {
		return rightChild;
	}

	public TreeNode<T> getParent() {
		return parent;
	}

	public void setParent(TreeNode<T> parent) {
		// parent.addChild(this);
		this.parent = parent;
	}

	public void addChildren(T leftData, T rightData) {
		leftChild.setParent(this);
		this.leftChild = new TreeNode<T>(leftData);
		this.rightChild = new TreeNode<T>(rightData);
		this.leftChild.levelInCalcTree = this.levelInCalcTree + 1;
		this.rightChild.levelInCalcTree = this.levelInCalcTree + 1;

	}

	public void addChildren(TreeNode<T> leftNode, TreeNode<T> rightNode) {
		this.leftChild = leftNode;
		this.rightChild = rightNode;
		leftChild.setParent(this);
		rightChild.setParent(this);
		this.leftChild.levelInCalcTree = this.levelInCalcTree + 1;
		this.rightChild.levelInCalcTree = this.levelInCalcTree + 1;
		((CalculationNode) leftNode.data).levelInCalcTree = this.levelInCalcTree + 1;
		((CalculationNode) rightNode.data).levelInCalcTree = this.levelInCalcTree + 1;

	}

	public T getData() {
		return this.data;
	}

	public void setData(T data) {
		this.data = data;
	}

	public void setParentData(T data) {
		this.parent.data = data;
	}

	public boolean isRoot() {
		return (this.parent == null);
	}

	public boolean isLeaf() {
		if (this.leftChild == null && this.rightChild == null)
			return true;
		else
			return false;
	}

	public void removeParent() {
		this.parent = null;
	}
}
