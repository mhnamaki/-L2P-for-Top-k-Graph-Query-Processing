package wsu.eecs.mlkd.KGQuery.test;

import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;


public class PerformanceTester {
	private static String MODELGRAPH_DB_PATH = "";
	private static String PATTERNGRAPH_DB_PATH = "";

	public static String queryFileName = "";
	public static String queryFileDirectory = "";
	public static int kFrom = 0;
	public static int kTo = 0;
	public static int querySizeFrom = 0;
	public static int querySizeTo = 0;
	public static String GName = ""; // Yago, DBPedia, ...

	public static String queryDBInNeo4j = "query.db";
	public static String GDirectory = "";

	public static void main(String[] args) throws Exception {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-queryFileName")) {
				queryFileName = args[++i];
				queryFileName = queryFileName.replace(".txt", "");
			} else if (args[i].equals("-queryFileDirectory")) {
				queryFileDirectory = args[++i];
				if (!queryFileDirectory.endsWith("/") && !queryFileDirectory.equals("")) {
					queryFileDirectory += "/";
				}
			} else if (args[i].equals("-kFrom")) {
				kFrom = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-kTo")) {
				kTo = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-querySizeFrom")) {
				querySizeFrom = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-querySizeTo")) {
				querySizeTo = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-GName")) {
				GName = args[++i];

			} else if (args[i].equals("-GDirectory")) {
				GDirectory = args[++i];

			}
		}

		if (!GDirectory.endsWith("/")) {
			GDirectory += "/";
		}
		MODELGRAPH_DB_PATH = GDirectory + GName;
		PATTERNGRAPH_DB_PATH = queryFileDirectory + queryDBInNeo4j;

		if (queryFileName.equals("") || GName.equals("") || kFrom == 0 || kTo == 0 || querySizeFrom == 0
				|| querySizeTo == 0) {
			throw new Exception(
					"You should provide all the parameters -queryFileName  -queryFileDirectory -kFrom -kTo -querySizeFrom -querySizeTo -GName");
		}

		QueryGenerator queryGenerator = new QueryGenerator(GDirectory + GName);
		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(MODELGRAPH_DB_PATH);

		registerShutdownHook(knowledgeGraph);

		for (int querySize = querySizeFrom; querySize <= querySizeTo; querySize++) {
			List<QueryFromFile> queriesFromFile = queryGenerator
					.getQueryFromFile(queryFileDirectory + queryFileName + querySize + ".txt");
			for (QueryFromFile queryFromFile : queriesFromFile) {
				DummyFunctions.printIfItIsInDebuggedMode("start ConstrucQueryGraph");
				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH,
						queryFromFile);
				GraphDatabaseService queryGraph = smallGraph;
				registerShutdownHook(queryGraph);
				DummyFunctions.printIfItIsInDebuggedMode("end ConstrucQueryGraph");
				for (int k = kFrom; k <= kTo; k++) {
					try (Transaction tx1 = smallGraph.beginTx()) {
						try (Transaction tx2 = knowledgeGraph.beginTx()) {

							// 0,1 infinity
							long start_time, end_time;
							double difference;
							// for (int i = 0; i < 10; i++) {
							// start_time = System.nanoTime();
							// justIterateAllTheGNodes(knowledgeGraph);
							// end_time = System.nanoTime();
							// difference = (end_time - start_time) / 1e6;
							// DummyFunctions.printIfItIsInDebuggedMode(
							// "justIterateAllTheGNodes " + i + " time: " +
							// difference);
							// }

							// for (int i = 0; i < 10; i++) {
							// start_time = System.nanoTime();
							// iterateAllTheGNodesByGettingProperty(knowledgeGraph);
							// end_time = System.nanoTime();
							// difference = (end_time - start_time) / 1e6;
							// DummyFunctions.printIfItIsInDebuggedMode(
							// "iterateAllTheGNodesByGettingProperty " + i + "
							// time " + difference);
							// }

							// for (int i = 0; i < 10; i++) {
							// start_time = System.nanoTime();
							// iterateAllTheGNodesByGettingLabels(knowledgeGraph);
							// end_time = System.nanoTime();
							// difference = (end_time - start_time) / 1e6;
							// DummyFunctions.printIfItIsInDebuggedMode(
							// "iterateAllTheGNodesByGettingLabels " + i + "
							// time " + difference);
							// }

//							for (int i = 0; i < 10; i++) {
//								start_time = System.nanoTime();
//								iterateAndCheckSimilarity(queryGraph, knowledgeGraph);
//								end_time = System.nanoTime();
//								difference = (end_time - start_time) / 1e6;
//								DummyFunctions.printIfItIsInDebuggedMode(
//										"iterateAndCheckSimilarity " + i + " time " + difference);
//							}

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
				}
				queryGraph.shutdown();
			}
		}
		knowledgeGraph.shutdown();
	}

//	private static void iterateAndCheckSimilarity(GraphDatabaseService queryGraph,
//			GraphDatabaseService knowledgeGraph) {
//		ResourceIterable<Node> allKnowledgeNodes = GlobalGraphOperations.at(knowledgeGraph).getAllNodes();
//		ResourceIterable<Node> allQueryNodes = GlobalGraphOperations.at(queryGraph).getAllNodes();
//		for (Node queryNode : allQueryNodes) {
//			for (Node knldgNode : allKnowledgeNodes) {
//				Iterable<Label> lblGIteratable = knldgNode.getLabels();
//				Iterable<Label> lblQIteratable = queryNode.getLabels();
//				if (lblGIteratable == null || lblQIteratable == null) {
//					continue;
//				}
//				Iterator<Label> lblGIterator = lblGIteratable.iterator();
//				Iterator<Label> lblQIterator = lblQIteratable.iterator();
//				if (lblGIterator == null || lblQIterator == null) {
//					continue;
//				}
//
//				try {
//					Label s1 = lblGIterator.next();
//					Label s2 = lblQIterator.next();
//					if (s1 == null || s2 == null) {
//						continue;
//					}
//					levenshtein.normalizedDistance(s1.toString(), s2.toString());
//				} catch (Exception exc) {
//
//				}
//			}
//		}
//	}

	private static void iterateAllTheGNodesByGettingLabels(GraphDatabaseService knowledgeGraph) {

		ResourceIterable<Node> allKnowledgeNodes = GlobalGraphOperations.at(knowledgeGraph).getAllNodes();
		for (Node knldgNode : allKnowledgeNodes) {
			for (Label label : knldgNode.getLabels()) {
				String val = label.toString();
				val = val.toString();
			}

		}
	}

	private static void iterateAllTheGNodesByGettingProperty(GraphDatabaseService knowledgeGraph) {
		ResourceIterable<Node> allKnowledgeNodes = GlobalGraphOperations.at(knowledgeGraph).getAllNodes();
		for (Node knldgNode : allKnowledgeNodes) {
			for (String key : knldgNode.getPropertyKeys()) {
				String val = knldgNode.getProperty(key).toString();
				val = val.toString();
			}

		}
	}

	private static void justIterateAllTheGNodes(GraphDatabaseService knowledgeGraph) {
		ResourceIterable<Node> allKnowledgeNodes = GlobalGraphOperations.at(knowledgeGraph).getAllNodes();
		for (Node knldgNode : allKnowledgeNodes) {
			long id = knldgNode.getId();
			id++;
		}
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
