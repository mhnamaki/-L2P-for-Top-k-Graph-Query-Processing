package wsu.eecs.mlkd.KGQuery.test;

import java.util.HashSet;
import java.util.Set;

import org.jgrapht.alg.DirectedNeighborIndex;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

public class NeighborIndexTester {

	public static String dbPath = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/newData/dbpedia_infobox_properties_en.db";

	public static void main(String[] args) {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-dbPath")) {
				dbPath = args[++i];

			}
		}
		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		registerShutdownHook(knowledgeGraph);

		try (Transaction tx1 = knowledgeGraph.beginTx()) {
			System.out.println("NeighborIndexTester is started!");
			DirectedNeighborIndex<Node, Relationship> directedNeighborIndex = createDirectedNeighborIndex(
					knowledgeGraph);
			System.out.println("createDirectedNeighborIndex is finished!");
			int progressCnt = 0;
			// for (Node node :
			// GlobalGraphOperations.at(knowledgeGraph).getAllNodes()) {
			// if (node.getDegree(Direction.INCOMING) !=
			// directedNeighborIndex.predecessorListOf(node).size()) {
			// System.out.println("incoming non-equality in nodeId: " +
			// node.getId()
			// + " node.getDegree(Direction.INCOMING): " +
			// node.getDegree(Direction.INCOMING)
			// + " directedNeighborIndex.predecessorListOf(node).size()"
			// + directedNeighborIndex.predecessorListOf(node).size());
			// }
			// if (node.getDegree(Direction.OUTGOING) !=
			// directedNeighborIndex.successorListOf(node).size()) {
			// System.out.println("outgoing non-equality in nodeId: " +
			// node.getId()
			// + " node.getDegree(Direction.OUTGOING): " +
			// node.getDegree(Direction.OUTGOING)
			// + " directedNeighborIndex.successorListOf(node).size()"
			// + directedNeighborIndex.successorListOf(node).size());
			// }
			// progressCnt++;
			// if ((progressCnt % 100000) == 0) {
			// System.out.println("progressCnt: " + progressCnt);
			// }
			// }

			Node node = knowledgeGraph.getNodeById(235);

			// System.out.println(
			// "outgoing non-equality in nodeId: " + node.getId() + "
			// node.getDegree(Direction.OUTGOING): "
			// + node.getDegree(Direction.OUTGOING) + "
			// directedNeighborIndex.successorsOf(node).size()"
			// + directedNeighborIndex.successorsOf(node).size());

			Set<Node> predNodesDirectedIndex = directedNeighborIndex.predecessorsOf(node);

			
			System.out.println(predNodesDirectedIndex.size());

			Iterable<Relationship> incomingRelationsips = node.getRelationships(Direction.INCOMING);
			HashSet<Node> preNodesNeo4j = new HashSet<Node>();
			int relCnt = 0;
			for (Relationship rel : incomingRelationsips) {
				relCnt++;
				preNodesNeo4j.add(rel.getOtherNode(node));
			}
			System.out.println(node.getDegree(Direction.INCOMING));
			System.out.println(relCnt);
			System.out.println(preNodesNeo4j.size());
			
			for (Node neo4jNode : preNodesNeo4j) {
				if (!predNodesDirectedIndex.contains(neo4jNode)) {
					System.out.println("not contain: "+neo4jNode.getId());
				}
			}

			// System.out.println("incoming non-equality in nodeId: " +
			// node.getId()
			// + " node.getDegree(Direction.INCOMING): " +
			// node.getDegree(Direction.INCOMING)
			// + " directedNeighborIndex.predecessorsOf(node).size()"
			// + directedNeighborIndex.predecessorsOf(node).size());

			// for (Relationship rel : node.getRelationships()) {
			// if (rel.getStartNode().getId() == node.getId()) {
			// System.out.println(" outgoing: " +
			// rel.getOtherNode(node).getId());
			// } else {
			// System.out.println(" incoming: " +
			// rel.getOtherNode(node).getId());
			// }
			// }

			tx1.success();
		} catch (Exception exc) {
			knowledgeGraph.shutdown();
		}
		knowledgeGraph.shutdown();

	}

	private static DirectedNeighborIndex<Node, Relationship> createDirectedNeighborIndex(
			GraphDatabaseService graphDatabaseService) {

		org.jgrapht.DirectedGraph<Node, Relationship> directedGraph = new DefaultDirectedGraph<Node, Relationship>(
				Relationship.class);

		ResourceIterable<Node> allNodes = GlobalGraphOperations.at(graphDatabaseService).getAllNodes();
		for (Node queryNodeItem : allNodes) {
			directedGraph.addVertex(queryNodeItem);
		}

		Iterable<Relationship> allRelationship = GlobalGraphOperations.at(graphDatabaseService).getAllRelationships();
		for (Relationship relItem : allRelationship) {
			directedGraph.addEdge(relItem.getStartNode(), relItem.getEndNode(), relItem);
		}
		return new DirectedNeighborIndex<>(directedGraph);

	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

}
