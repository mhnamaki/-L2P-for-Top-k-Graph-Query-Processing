package wsu.eecs.mlkd.KGQuery.test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;

public class LeveneshteinExperimenter {

	public static void main(String[] args) {
		String GPath = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/newData/dbpedia_infobox_properties_en.db";
		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(GPath);
		registerShutdownHook(knowledgeGraph);

		QueryGenerator queryGenerator = new QueryGenerator(GPath);
		List<QueryFromFile> queriesFromFile = queryGenerator.getQueryFromFile(
				"/Users/mnamaki/Documents/workspace/wsu.eecs.mlkd.KGQuery/src/wsu/eecs/mlkd/KGQuery/test/StoredQueries5.txt");
		GraphDatabaseService queryGraph = queryGenerator.ConstrucQueryGraph(
				"/Users/mnamaki/Documents/workspace/wsu.eecs.mlkd.KGQuery/src/wsu/eecs/mlkd/KGQuery/test/query.db",
				queriesFromFile.get(0));
		registerShutdownHook(queryGraph);
		HashMap<String, HashSet<Long>> nodeLabelsIndex = PreProcessingLabels.getPrefixLabelsIndex(knowledgeGraph, 4);
		Levenshtein levenshtein = new Levenshtein(nodeLabelsIndex, 4);
		try (Transaction tx1 = queryGraph.beginTx()) {
			try (Transaction tx2 = knowledgeGraph.beginTx()) {

				// {
				// System.out.println("Labels----");
				// Iterable<Label> knldglabelIterator =
				// knowledgeGraph.getNodeById(31286).getLabels();
				// for (Label knldgLabel : knldglabelIterator) {
				// System.out.println(knldgLabel.toString());
				// }
				//
				// System.out.println("Proper----");
				//
				// Iterable<String> keys =
				// knowledgeGraph.getNodeById(31286).getPropertyKeys();
				// for (String key : keys) {
				// System.out.println(knowledgeGraph.getNodeById(31286).getProperty(key));
				// }
				// System.out.println("QLabel----");
				// Iterable<Label> qlabelIterator =
				// queryGraph.getNodeById(3).getLabels();
				// for (Label qLabel : qlabelIterator) {
				// System.out.println(qLabel.toString());
				// }
				// }
				//
				// System.out.println(levenshtein.HowMuchTwoNodesAreSimilar(queryGraph.getNodeById(0),
				// knowledgeGraph.getNodeById(3838702)));
				// System.out.println("////");
				// {Iterable<Label> knldglabelIterator =
				// knowledgeGraph.getNodeById(3838703).getLabels();
				// for (Label knldgLabel : knldglabelIterator) {
				// System.out.println(knldgLabel.toString());
				// }
				// System.out.println("----");
				//
				// Iterable<Label> qlabelIterator =
				// queryGraph.getNodeById(1).getLabels();
				// for (Label qLabel : qlabelIterator) {
				// System.out.println(qLabel.toString());
				// }}
				// System.out.println(levenshtein.HowMuchTwoNodesAreSimilar(queryGraph.getNodeById(1),
				// knowledgeGraph.getNodeById(3838703)));

//				System.out.println(levenshtein.HowMuchTwoNodesAreSimilar(knowledgeGraph, queryGraph.getNodeById(0),
//						knowledgeGraph.getNodeById(3838702)));
//				System.out.println(levenshtein.HowMuchTwoNodesAreSimilar(knowledgeGraph, queryGraph.getNodeById(1),
//						knowledgeGraph.getNodeById(3838703)));
//				System.out.println(levenshtein.HowMuchTwoNodesAreSimilar(knowledgeGraph, queryGraph.getNodeById(2),
//						knowledgeGraph.getNodeById(235)));
//				System.out.println(levenshtein.HowMuchTwoNodesAreSimilar(knowledgeGraph, queryGraph.getNodeById(3),
//						knowledgeGraph.getNodeById(31286)));
//				System.out.println(levenshtein.HowMuchTwoNodesAreSimilar(knowledgeGraph, queryGraph.getNodeById(4),
//						knowledgeGraph.getNodeById(2969668)));

				tx2.success();
			} catch (Exception e) {
				queryGraph.shutdown();
				knowledgeGraph.shutdown();
			}
			tx1.success();
		} catch (Exception e) {
			queryGraph.shutdown();
			knowledgeGraph.shutdown();
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
