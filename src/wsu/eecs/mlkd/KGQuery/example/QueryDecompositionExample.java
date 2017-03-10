package wsu.eecs.mlkd.KGQuery.example;

import java.util.ArrayList;

import org.jgrapht.DirectedGraph;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.*;

import wsu.eecs.mlkd.KGQuery.TopKQuery.NodePotentialMemberInIsomorphicEstimation;
import wsu.eecs.mlkd.KGQuery.TopKQuery.QueryDecomposition;
import wsu.eecs.mlkd.KGQuery.TopKQuery.StarGraphQuery;
import wsu.eecs.mlkd.KGQuery.algo.VF2.VF2Matcher;

public class QueryDecompositionExample {
	// private static final String MODELGRAPH_DB_PATH =
	// "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/knowledge3Nodes.db";
	// private static final String PATTERNGRAPH_DB_PATH =
	// "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/pattern3Nodes.db";

	// public static void main(String[] args) {
	//
	// QueryDecomposition qd = new QueryDecomposition();
	// GraphDatabaseService knowledgeGraph = new
	// GraphDatabaseFactory().newEmbeddedDatabase(MODELGRAPH_DB_PATH);
	// registerShutdownHook(knowledgeGraph);
	//
	// GraphDatabaseService queryGraph = new
	// GraphDatabaseFactory().newEmbeddedDatabase(PATTERNGRAPH_DB_PATH);
	// registerShutdownHook(queryGraph);
	//
	// try (Transaction tx1 = queryGraph.beginTx()) {
	// try (Transaction tx2 = knowledgeGraph.beginTx()) {
	//
	// ArrayList<org.jgrapht.DirectedGraph<Node, Relationship>> decomposedQs =
	// qd
	// .getStarQueriesFromGraphQueryBasedOnKnowledgeGraphClarksonGreedyAlgorithm(queryGraph,
	// knowledgeGraph);
	//
	// for (DirectedGraph<Node, Relationship> directedGraph : decomposedQs) {
	// for (Relationship rel : directedGraph.edgeSet()) {
	// System.out.print(rel.getStartNode().getLabels().toString() + "_"
	// + rel.getEndNode().getLabels().toString() + " ,");
	//
	// }
	// System.out.println("");
	// }
	//
	// tx2.success();
	// tx1.success();
	// } catch (Exception exc) {
	// exc.printStackTrace();
	// } finally {
	// knowledgeGraph.shutdown();
	// queryGraph.shutdown();
	// }
	//
	// } catch (Exception exc) {
	// exc.printStackTrace();
	// } finally {
	// knowledgeGraph.shutdown();
	// queryGraph.shutdown();
	// }
	//
	// }

	private static final String PATTERNGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/directedExampleForDecomposition.db";

	public static void main(String[] args) {

//		QueryDecomposition qd = new QueryDecomposition();
//
//		GraphDatabaseService queryGraph = new GraphDatabaseFactory().newEmbeddedDatabase(PATTERNGRAPH_DB_PATH);
//		registerShutdownHook(queryGraph);
//
//		try (Transaction tx1 = queryGraph.beginTx()) {
//
//			ArrayList<NodePotentialMemberInIsomorphicEstimation> nodeEstimationList = new ArrayList<NodePotentialMemberInIsomorphicEstimation>();
//
//			nodeEstimationList.add(new NodePotentialMemberInIsomorphicEstimation(queryGraph.getNodeById(0), 100));
//
//			nodeEstimationList.add(new NodePotentialMemberInIsomorphicEstimation(queryGraph.getNodeById(1), 10));// b
//
//			nodeEstimationList.add(new NodePotentialMemberInIsomorphicEstimation(queryGraph.getNodeById(2), 1000));// c
//
//			nodeEstimationList.add(new NodePotentialMemberInIsomorphicEstimation(queryGraph.getNodeById(3), 100));// d
//
//			nodeEstimationList.add(new NodePotentialMemberInIsomorphicEstimation(queryGraph.getNodeById(4), 10));// e
//
//			ArrayList<StarGraphQuery> decomposedQs = qd.getStarQueriesFromGraphQuery(queryGraph, nodeEstimationList);
//
//			for (StarGraphQuery directedGraph : decomposedQs) {
//				System.out.println("Pivot node:" + directedGraph.pivotNode.getProperty("name"));
//				for (Relationship rel : directedGraph.graphQuery.edgeSet()) {
//					System.out.print(rel.getStartNode().getLabels().toString() + "_"
//							+ rel.getEndNode().getLabels().toString() + " ,");
//
//				}
//				System.out.println();
//				System.out.println();
//			}
//
//			tx1.success();
//		} catch (Exception exc) {
//			exc.printStackTrace();
//		} finally {
//			queryGraph.shutdown();
//		}

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
