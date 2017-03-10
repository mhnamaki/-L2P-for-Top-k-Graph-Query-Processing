package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class StarGraphQuery {
	public Node pivotNode;
	public org.jgrapht.DirectedGraph<Node, Relationship> graphQuery;
	public Set<Node> allStarGraphQueryNodes;
	public String nodeNameLabels = "";
	public float calcTreeHeuristicValue;
	// public HashMap<Long, Integer> potentialCounterMap;
	public int numberOfPAEstimate = 0;
	public int numberOfPossiblePivots = 0;
	public double avgDegreeOfPossiblePivots = 0;
	public int starQueryCalcNodeIndex;
	public int starQueryIndex;

	public StarGraphQuery(Node pivotNode, org.jgrapht.DirectedGraph<Node, Relationship> directedGraph) {
		// potentialCounterMap = new HashMap<Long, Integer>();
		this.pivotNode = pivotNode;
		this.graphQuery = directedGraph;
		allStarGraphQueryNodes = directedGraph.vertexSet();

		// TODO: just for debugging
		// nodeNameLabels += "Pivot: " + pivotNode.getProperty("name");

		for (Node node : allStarGraphQueryNodes) {
			// for training start
			// potentialCounterMap.put(node.getId(), 0);
			// for training end

			if (node.getId() != pivotNode.getId())
				nodeNameLabels += "_" + node.getId();
		}

	}

	// // because it won't be modified
	// public StarGraphQuery copy() {
	//
	// StarGraphQuery newStarGraphQuery = new StarGraphQuery(this.pivotNode,
	// this.graphQuery);
	// return newStarGraphQuery;
	//
	// }
}
