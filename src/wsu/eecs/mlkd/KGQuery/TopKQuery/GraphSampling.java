package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.impl.transaction.log.pruning.Threshold;
import org.neo4j.tooling.GlobalGraphOperations;

public class GraphSampling {
	public GraphSampling() {

	}

	public ArrayList<Node> getSampleOfNodes(GraphDatabaseService graph, double sampleRatio) throws Exception {
		if (sampleRatio > 1) {
			throw new Exception("sampleRatio should be between [0,1]");
		}
		ArrayList<Node> sampleOfNodes = new ArrayList<Node>();
		ResourceIterable<Node> allNodes = GlobalGraphOperations.at(graph).getAllNodes();

		for (Node node : allNodes) {

			if (Math.random() <= sampleRatio) {
				sampleOfNodes.add(node);
			}
		}
		return sampleOfNodes;
	}
}
