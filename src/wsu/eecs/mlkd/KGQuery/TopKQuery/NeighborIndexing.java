package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.HashMap;
import java.util.HashSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

public class NeighborIndexing {

	public HashMap<Long, HashSet<Long>> graphInNeighborIndicesMap = new HashMap<Long, HashSet<Long>>();
	public HashMap<Long, HashSet<Long>> graphOutNeighborIndicesMap = new HashMap<Long, HashSet<Long>>();

	public HashMap<Long, HashSet<Long>> queryInNeighborIndicesMap = new HashMap<Long, HashSet<Long>>();
	public HashMap<Long, HashSet<Long>> queryOutNeighborIndicesMap = new HashMap<Long, HashSet<Long>>();

	public HashMap<Long, Integer> graphInDegreeMap = new HashMap<Long, Integer>();
	public HashMap<Long, Integer> graphOutDegreeMap = new HashMap<Long, Integer>();

	public HashMap<Long, Integer> queryInDegreeMap = new HashMap<Long, Integer>();
	public HashMap<Long, Integer> queryOutDegreeMap = new HashMap<Long, Integer>();

	public HashSet<Long> queryNodeIdSet = new HashSet<Long>();
	// public static HashSet<Long> graphNodeIdSet = null;
	public HashMap<Long, String> queryNodeLabelMap = new HashMap<Long, String>();

	public NeighborIndexing() {
	}

	public void clearQueryNeighborIndexer() {
		queryNodeIdSet.clear();
		queryInNeighborIndicesMap.clear();
		queryOutNeighborIndicesMap.clear();
		queryInDegreeMap.clear();
		queryOutDegreeMap.clear();
		queryNodeLabelMap.clear();

		queryNodeIdSet = null;
		queryInNeighborIndicesMap = null;
		queryOutNeighborIndicesMap = null;
		queryInDegreeMap = null;
		queryOutDegreeMap = null;
		queryNodeLabelMap = null;
		//
		queryNodeIdSet = new HashSet<Long>();
		queryInNeighborIndicesMap = new HashMap<Long, HashSet<Long>>();
		queryOutNeighborIndicesMap = new HashMap<Long, HashSet<Long>>();
		queryInDegreeMap = new HashMap<Long, Integer>();
		queryOutDegreeMap = new HashMap<Long, Integer>();
		queryNodeLabelMap = new HashMap<Long, String>();
	}

	private void fillQueryNodeLabelMap(GraphDatabaseService queryGraph) throws Exception {
		queryNodeLabelMap.clear();
		Transaction tx2 = queryGraph.beginTx();
		ResourceIterable<Node> allNodes = GlobalGraphOperations.at(queryGraph).getAllNodes();

		for (Node node : allNodes) {
			queryNodeLabelMap.put(node.getId(), node.getLabels().iterator().next().toString());
		}
		tx2.success();
		tx2.close();

	}

	public void clearGraphNeighborIndexer() {
		// graphNodeIdSet = new HashSet<Long>();
		// graphInNeighborIndicesMap = new HashMap<Long, HashSet<Long>>();
		// graphOutNeighborIndicesMap = new HashMap<Long, HashSet<Long>>();
		// graphInDegreeMap = new HashMap<Long, Integer>();
		// graphOutDegreeMap = new HashMap<Long, Integer>();

		graphInNeighborIndicesMap.clear();
		graphOutNeighborIndicesMap.clear();
		graphInDegreeMap.clear();
		graphOutDegreeMap.clear();
	}

	public void queryNeighborIndexer(GraphDatabaseService queryGraph) throws Exception {
		clearQueryNeighborIndexer();
		fillQueryNodeLabelMap(queryGraph);
		neighborIndexer(queryGraph, queryInNeighborIndicesMap, queryInDegreeMap, queryNodeIdSet, Direction.INCOMING);
		neighborIndexer(queryGraph, queryOutNeighborIndicesMap, queryOutDegreeMap, null, Direction.OUTGOING);
	}

	public void knowledgeGraphNeighborIndexer(GraphDatabaseService knowledgeGraph) throws Exception {
		clearGraphNeighborIndexer();
		neighborIndexer(knowledgeGraph, graphInNeighborIndicesMap, graphInDegreeMap, null, Direction.INCOMING);
		neighborIndexer(knowledgeGraph, graphOutNeighborIndicesMap, graphOutDegreeMap, null, Direction.OUTGOING);
	}

	private void neighborIndexer(GraphDatabaseService graphDatabaseService,
			HashMap<Long, HashSet<Long>> neighborIndicesMap, HashMap<Long, Integer> degreeMap, HashSet<Long> nodeIdSet,
			Direction direction) throws Exception {

		Transaction tx2 = graphDatabaseService.beginTx();
		ResourceIterable<Node> allNodes = GlobalGraphOperations.at(graphDatabaseService).getAllNodes();

		for (Node node : allNodes) {
			if (nodeIdSet != null) {
				nodeIdSet.add(node.getId());
			}
			HashSet<Long> neighborsIdSet = new HashSet<Long>();

			for (Relationship rel : node.getRelationships(direction)) {
				neighborsIdSet.add(rel.getOtherNode(node).getId());

			}
			degreeMap.put(node.getId(), node.getDegree(direction));
			neighborIndicesMap.put(node.getId(), neighborsIdSet);

		}
		tx2.success();
		tx2.close();

	}

}
