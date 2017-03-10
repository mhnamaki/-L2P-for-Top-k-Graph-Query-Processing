package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TreeExampleForStarJoin {

	public static void main(String[] args) {
		Tree tree = new Tree();
		// Node<Integer> root = new Node<Integer>(0);
		Random rnd = new Random();

		// int ;

		ArrayList<TreeNode<Integer[]>> listOfNodes = new ArrayList<TreeNode<Integer[]>>();

		for (int i = 0; i < 7; i++) {
			TreeNode<Integer[]> n = new TreeNode<Integer[]>();
			Integer[] listOfNumbers = new Integer[3];
			for (int j = 0; j < listOfNumbers.length; j++) {
				listOfNumbers[j] = i;
			}
			n.setData(listOfNumbers);
			listOfNodes.add(n);
		}

		
		listOfNodes.get(3).addChildren(listOfNodes.get(5), listOfNodes.get(6));
		listOfNodes.get(1).addChildren(listOfNodes.get(3), listOfNodes.get(4));
		listOfNodes.get(0).addChildren(listOfNodes.get(1), listOfNodes.get(2));
		
		tree.starJoin(listOfNodes.get(1), listOfNodes.get(2));

		// System.out.println(factoriel(3));
		// myMethod(4);
	}

}

class Tree {
	public void printResult(Integer[] integer){
		for (int i = 0; i < integer.length; i++) {
			System.out.print(integer[i] + ",");
		}
		System.out.println();
	}
	public void starJoin(TreeNode<Integer[]> leftChild, TreeNode<Integer[]> rightChild) {
		if (rightChild.isLeaf() && leftChild.isLeaf()) {
			Integer[] result = starJoinForLeaves(leftChild, rightChild);
			leftChild.setParentData(result);
			printResult(result);
			return;

		}
		starJoin(leftChild.getLeftChild(), leftChild.getRightChild());
		Integer[] result = starJoinForLeaves(leftChild, rightChild);
		leftChild.setParentData(result);
		printResult(result);
	}

	private Integer[] starJoinForLeaves(TreeNode<Integer[]> leftChild, TreeNode<Integer[]> rightChild) {
		leftChild.isComputed = true;
		rightChild.isComputed = true;
		Integer[] integer = new Integer[Math.min(((Integer[]) leftChild.getData()).length,
				((Integer[]) rightChild.getData()).length)];
		for (int i = 0; i < integer.length; i++) {
			integer[i] = leftChild.getData()[i] + rightChild.getData()[i];
		}
		return integer;
		// return Integer.parseInt(leftChild.getData().toString()) +
		// Integer.parseInt(rightChild.getData().toString());

	}

}

