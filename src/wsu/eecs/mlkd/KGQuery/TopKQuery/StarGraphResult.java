package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class StarGraphResult implements Comparable<StarGraphResult> {
	public NodeWithValue pivotNode;
	public HashMap<Node, NodeWithValue> neighborsOfPivot;
	public HashSet<NodeWithValue> allStarGraphResutlsNodes;
	public HashMap<Node, Integer> readKnowledgeNodesFromTheseCursorsBasedOnQNode;
	public float value = 0;
	public int depthOfThisAnswerOfStarQuery = 0;

	// For Debug:
	public String allStarGraphResutlsNodesWithTheirValueForDebug = "";

	public StarGraphResult copy() {
		StarGraphResult newSGR = new StarGraphResult(this.pivotNode, this.neighborsOfPivot,
				this.readKnowledgeNodesFromTheseCursorsBasedOnQNode);

		newSGR.depthOfThisAnswerOfStarQuery = this.depthOfThisAnswerOfStarQuery;
		return newSGR;
	}

	public StarGraphResult(NodeWithValue pivotNode, HashMap<Node, NodeWithValue> neighborsOfPivot,
			HashMap<Node, Integer> readKnowledgeNodesFromTheseCursorsBasedOnQNode) {
		this.pivotNode = pivotNode;
		this.neighborsOfPivot = neighborsOfPivot;
		this.readKnowledgeNodesFromTheseCursorsBasedOnQNode = readKnowledgeNodesFromTheseCursorsBasedOnQNode;
		allStarGraphResutlsNodes = new HashSet<NodeWithValue>();
		allStarGraphResutlsNodesWithTheirValueForDebug += "pivotNode: " + pivotNode.node.getId() + "_"
				+ pivotNode.simValue + "| neighbors: ";

		for (Node nn : neighborsOfPivot.keySet()) {
			this.value += neighborsOfPivot.get(nn).simValue;

			// for degbug
			allStarGraphResutlsNodesWithTheirValueForDebug += neighborsOfPivot.get(nn).node.getId() + "_"
					+ neighborsOfPivot.get(nn).simValue + "| ";
		}
		this.value += pivotNode.simValue;

		allStarGraphResutlsNodes.addAll(neighborsOfPivot.values());
		allStarGraphResutlsNodes.add(pivotNode);

	}

	@Override
	public int compareTo(StarGraphResult other) {
		return Float.compare(other.value, this.value);
	}

}

class ArrayListWithCursor {
	ArrayList<NodeWithValue> nodeWithValueInDescOrder;
	int cursor;
	Comparator<NodeWithValue> nodeWithValueComparator = new NodeWithValueComparator();

	// public PriorityQueueWithCursor() {
	// this.nodeWithValueInDescOrder = new PriorityQueue<NodeWithValue>();
	// cursor = 0;
	// }

	public ArrayListWithCursor copy() {
		ArrayListWithCursor newArrListWC = new ArrayListWithCursor();

		for (NodeWithValue nwv : this.nodeWithValueInDescOrder) {
			newArrListWC.nodeWithValueInDescOrder.add(nwv);
		}
		newArrListWC.cursor = this.cursor;
		newArrListWC.nodeWithValueComparator = this.nodeWithValueComparator;

		return newArrListWC;

	}

	public ArrayListWithCursor() {
		this.nodeWithValueInDescOrder = new ArrayList<NodeWithValue>();
		cursor = 0;
	}
}

class StarGraphResultComparator implements Comparator<StarGraphResult> {
	public int compare(StarGraphResult a, StarGraphResult b) {
		if (b.value != a.value) {
			return Float.compare(b.value, a.value);
		} else {
			return Long.compare(b.pivotNode.node.getId(), a.pivotNode.node.getId());
		}
	}
}