package wsu.eecs.mlkd.KGQuery.example;

import java.util.ArrayList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import edu.uci.ics.jung.graph.DirectedGraph;
import wsu.eecs.mlkd.KGQuery.TopKQuery.*;
import wsu.eecs.mlkd.KGQuery.algo.VF2.VF2Matcher;

import org.jgrapht.alg.*;

public class TopKQueryExample {
	//private static final String MODELGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/VF2modelgraph.db";
	//private static final String PATTERNGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/VF2patterngraph.db";
//	
	//private static final String MODELGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/knowledge3Nodes.db";
	//private static final String PATTERNGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/pattern3Nodes.db";
	
	private static final String MODELGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/starFrameWorkTestG1Prime.db";
    private static final String PATTERNGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/starFrameWorkTestQ1.db";
	
	
	public static void main(String[] args) {
//		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(MODELGRAPH_DB_PATH);
//		registerShutdownHook(knowledgeGraph);
//
//		GraphDatabaseService queryGraph = new GraphDatabaseFactory().newEmbeddedDatabase(PATTERNGRAPH_DB_PATH);
//		registerShutdownHook(queryGraph);
//
//		int kNumberOfTopAnswers = 3;
//		// resp = GeoLocationService.getLocationByIp(ipAddress);
//
//		try (Transaction tx1 = queryGraph.beginTx()) {
//			try (Transaction tx2 = knowledgeGraph.beginTx()) {
//				GraphTA gt = new GraphTA(queryGraph, knowledgeGraph, kNumberOfTopAnswers);
//				long start_time = System.nanoTime();
//				gt.runGraphTA();
//				long end_time = System.nanoTime();
//				double difference = (end_time - start_time) / 1e6;
//				System.out.println("GraphTA finished in " + difference + "miliseconds!");
//				tx2.success();
//			} catch (Exception exc) {
//				System.out.println("queryGraph Transaction failed");
//				exc.printStackTrace();
//			}
//
//			tx1.success();
//		} catch (Exception exc) {
//			System.out.println("modelGraph Transaction failed");
//			exc.printStackTrace();
//		}
//		finally {
//			queryGraph.shutdown();
//			knowledgeGraph.shutdown();	
//		}
//		

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

//Test Node GraphSampling
	// public static void main(String[] args) {
	// GraphSampling gs = new GraphSampling();
	// GraphDatabaseService knowledgeGraph = new
	// GraphDatabaseFactory().newEmbeddedDatabase(MODELGRAPH_DB_PATH);
	// registerShutdownHook(knowledgeGraph);
	// try {
	// try (Transaction tx1 = knowledgeGraph.beginTx()) {
	// int sumSizes = 0;
	// int numberOfSummation = 0;
	// long start_time = System.nanoTime();
	// for (int i = 0; i < 1000; i++) {
	// ArrayList<Node> sampleOfNodes = gs.getSampleOfNodes(knowledgeGraph, 0.2);
	// sumSizes += sampleOfNodes.size();
	// numberOfSummation++;
	// }
	//
	// long end_time = System.nanoTime();
	// double difference = (end_time - start_time) / 1e6;
	//
	// System.out.println("SIZE: " + (sumSizes / numberOfSummation));
	// System.out.println("Graph sampling finished in " + difference + "
	// miliseconds!");
	// tx1.success();
	// } catch (Exception e) {
	//
	// }
	//
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// knowledgeGraph.shutdown();
	// }

	// public static void main(String[] args) {
	//
	//
	//
	// }
