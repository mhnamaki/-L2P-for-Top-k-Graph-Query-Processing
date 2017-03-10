package wsu.eecs.mlkd.KGQuery.example;

import java.util.HashMap;
import java.util.HashSet;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import wsu.eecs.mlkd.KGQuery.TopKQuery.GraphTA;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;

public class GraphTATester {
//	private static final String MODELGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/knowledge3Nodes.db";
//	private static final String PATTERNGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/pattern3Nodes.db";
	private static final String MODELGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/starFrameWorkTestG1.db";
	private static final String PATTERNGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/starFrameWorkTestQ1.db";

	public static void main(String[] args) {
		DummyProperties.similarityThreshold = 0.9F;
		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(MODELGRAPH_DB_PATH);
		registerShutdownHook(knowledgeGraph);

		GraphDatabaseService queryGraph = new GraphDatabaseFactory().newEmbeddedDatabase(PATTERNGRAPH_DB_PATH);
		registerShutdownHook(queryGraph);

		int kNumberOfTopAnswers = 1;

		HashMap<String, HashSet<Long>> nodeLabelsIndex = PreProcessingLabels.getPrefixLabelsIndex(knowledgeGraph, 3);

		Levenshtein levenshtein = new Levenshtein(nodeLabelsIndex, 3);
		try (Transaction tx1 = queryGraph.beginTx()) {
			try (Transaction tx2 = knowledgeGraph.beginTx()) {
				DummyFunctions
						.createUniqueIdForEachNode(queryGraph/* , knowledgeGraph */);
				DummyProperties.nodeIdByUniqueManualIdMap = new HashMap<GraphDatabaseService, HashMap<Integer, Long>>();
				DummyProperties.uniqueManualIdByNodeIdMap = new HashMap<GraphDatabaseService, HashMap<Long, Integer>>();
				DummyFunctions.populateIdUniqueManualIdMaps(knowledgeGraph);
				DummyFunctions.populateIdUniqueManualIdMaps(queryGraph);

				GraphTA gt = new GraphTA(queryGraph, knowledgeGraph, kNumberOfTopAnswers, levenshtein);
				gt.runGraphTA();
				System.out.println("GraphTA is finished!");
				tx2.success();
			} catch (Exception exc) {
				System.out.println("queryGraph Transaction failed");
				exc.printStackTrace();
			}

			tx1.success();
		} catch (Exception exc) {
			System.out.println("modelGraph Transaction failed");
			exc.printStackTrace();
		}
		queryGraph.shutdown();
		knowledgeGraph.shutdown();

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
