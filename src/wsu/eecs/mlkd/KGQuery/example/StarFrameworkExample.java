package wsu.eecs.mlkd.KGQuery.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import wsu.eecs.mlkd.KGQuery.TopKQuery.GraphResult;
import wsu.eecs.mlkd.KGQuery.TopKQuery.GraphTA;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;
import wsu.eecs.mlkd.KGQuery.TopKQuery.StarFramework2;

public class StarFrameworkExample {

	// private static final String MODELGRAPH_DB_PATH =
	// "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/knowledge3Nodes.db";
	// private static final String PATTERNGRAPH_DB_PATH =
	// "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/pattern3Nodes.db";

	private static final String MODELGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/starFrameWorkTestG1Prime.db";
	private static final String PATTERNGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/starFrameWorkTestQ1.db";
	public static int numberOfPrefixChars = 4;

	public static void main(String[] args) {
		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(MODELGRAPH_DB_PATH);
		registerShutdownHook(knowledgeGraph);

		GraphDatabaseService queryGraph = new GraphDatabaseFactory().newEmbeddedDatabase(PATTERNGRAPH_DB_PATH);
		registerShutdownHook(queryGraph);
		HashMap<String, HashSet<Long>> nodeLabelsIndex = PreProcessingLabels.getPrefixLabelsIndex(knowledgeGraph,
				numberOfPrefixChars);
		Levenshtein levenshtein = new Levenshtein(nodeLabelsIndex, numberOfPrefixChars);
		int kNumberOfTopAnswers = 3;
		float alpha = 0.5F;
		try (Transaction tx1 = queryGraph.beginTx()) {
			try (Transaction tx2 = knowledgeGraph.beginTx()) {
				StarFramework2 starFramework2 = new StarFramework2(queryGraph, knowledgeGraph, kNumberOfTopAnswers,
						alpha, levenshtein);
				long start_time = System.nanoTime();
				ArrayList<GraphResult> finalResults = starFramework2.starRun(queryGraph, knowledgeGraph);
				long end_time = System.nanoTime();
				double difference = (end_time - start_time) / 1e6;

				for (int i = 0; i < finalResults.size(); i++) {
					GraphResult gr = finalResults.get(i);
					String result = "";
					for (Node qNode : gr.assembledResult.keySet()) {
						result += gr.assembledResult.get(qNode).node.getId() + "-";
					}
					result += " -> " + gr.getTotalValue();
					System.out.println(result);
				}

				System.out.println("StarFramework2 finished in " + difference + "miliseconds!");

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
