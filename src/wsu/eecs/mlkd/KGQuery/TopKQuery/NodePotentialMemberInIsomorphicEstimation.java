package wsu.eecs.mlkd.KGQuery.TopKQuery;

import org.neo4j.graphdb.Node;

public class NodePotentialMemberInIsomorphicEstimation {
	Node node;
	int value;

	public NodePotentialMemberInIsomorphicEstimation(Node node, int value) {
		this.node = node;
		this.value = value;
	}
}
