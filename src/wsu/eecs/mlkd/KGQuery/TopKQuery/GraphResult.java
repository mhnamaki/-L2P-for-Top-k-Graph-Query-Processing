package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;

import org.neo4j.graphdb.Node;

public class GraphResult implements Comparable<GraphResult> {
	public HashMap<Node, NodeWithValue> assembledResult;
	Set<Node> joinableQueryNodes;
	float totalValue;
	float joinableNodesTotalValue;
	float nonJoinableNodesTotalValue;
	public boolean isVisited;
	public String joinableKnowledgeNodeIds;
	public HashMap<Integer, Integer> starQueryIndexDepthMap;
	public double anyTimeItemValue;

	// for debug
	public String nodesInThisGraphResultForDebug = "";

	public GraphResult copy() {
		HashMap<Node, NodeWithValue> newAssembledResult = new HashMap<Node, NodeWithValue>(); // these
																								// object
																								// won't
																								// be
																								// modified
		for (Node node : this.assembledResult.keySet()) {
			newAssembledResult.put(node, this.assembledResult.get(node));
		}
		GraphResult newGraphResult = new GraphResult(newAssembledResult, this.joinableQueryNodes,
				this.starQueryIndexDepthMap);

		newGraphResult.isVisited = this.isVisited;
		newGraphResult.joinableKnowledgeNodeIds = this.joinableKnowledgeNodeIds;
		newGraphResult.anyTimeItemValue = this.anyTimeItemValue;

		return newGraphResult;

	}

	public float getTotalValue() {
		return totalValue;
	}

	public float getJoinableNodesTotalValue() {
		return joinableNodesTotalValue;
	}

	public float getNonJoinableNodesTotalValue() {
		return nonJoinableNodesTotalValue;
	}

	public GraphResult(HashMap<Node, NodeWithValue> assembledResult, Set<Node> joinableQueryNodes,
			HashMap<Integer, Integer> starQueryIndexDepthMap) {
		this.assembledResult = assembledResult;
		this.joinableQueryNodes = joinableQueryNodes;

		isVisited = false; // when it initializes it means that it isn't visited
							// yet.

		for (Node qNode : assembledResult.keySet()) {
			nodesInThisGraphResultForDebug += assembledResult.get(qNode).node.getId() + "_";

			if (joinableQueryNodes != null && !joinableQueryNodes.contains(qNode))
				nonJoinableNodesTotalValue += assembledResult.get(qNode).simValue;
			else
				joinableNodesTotalValue += assembledResult.get(qNode).simValue;

		}

		totalValue = nonJoinableNodesTotalValue + joinableNodesTotalValue;

		this.starQueryIndexDepthMap = starQueryIndexDepthMap;

	}

	@Override
	public int compareTo(GraphResult other) {
		return Float.compare(other.totalValue, this.totalValue);
	}
}

class GraphResultComparator implements Comparator<GraphResult> {

	@Override
	public int compare(GraphResult o1, GraphResult o2) {
		return Float.compare(o2.totalValue, o1.totalValue);
	}

}

// ascending
class AnytimeResultComparator implements Comparator<GraphResult> {
	public int compare(GraphResult a, GraphResult b) {
		if (a.anyTimeItemValue != b.anyTimeItemValue) {
			return Double.compare(a.anyTimeItemValue, b.anyTimeItemValue);
		} else {
			return Double.compare(a.totalValue, b.totalValue);
		}
	}
}
