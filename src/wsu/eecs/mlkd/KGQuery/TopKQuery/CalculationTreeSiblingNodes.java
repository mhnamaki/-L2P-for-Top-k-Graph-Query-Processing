package wsu.eecs.mlkd.KGQuery.TopKQuery;

public class CalculationTreeSiblingNodes {
	public TreeNode<CalculationNode> leftNode;
	public TreeNode<CalculationNode> rightNode;

	public CalculationTreeSiblingNodes(TreeNode<CalculationNode> leftNode, TreeNode<CalculationNode> rightNode) {
		this.leftNode = leftNode;
		this.rightNode = rightNode;
	}
}
