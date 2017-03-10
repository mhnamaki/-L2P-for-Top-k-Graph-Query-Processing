package wsu.eecs.mlkd.KGQuery.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.TopKQuery.GraphTA;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CacheServer;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;

public class GraphTAExperimenter {
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
	public static int numberOfPrefixChars = 4;
	public static CacheServer cacheServer;
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

			} else if (args[i].equals("-similarityThreshold")) {
				DummyProperties.similarityThreshold = Float.parseFloat(args[++i]);
			}
		}

		if (!GDirectory.endsWith("/")) {
			GDirectory += "/";
		}
		MODELGRAPH_DB_PATH = GDirectory + GName;
		PATTERNGRAPH_DB_PATH = queryFileDirectory + queryDBInNeo4j;

		ArrayList<Integer> k_s = new ArrayList<Integer>();
		if (kFrom == 0 && kTo == 0) {
			k_s.add(100);
			k_s.add(50);
			k_s.add(20);
			k_s.add(10);
			k_s.add(1);
		} else {
			for (int i = kTo; i >= kFrom; i--) {
				k_s.add(i);
			}
		}

		if (queryFileName.equals("") || GName.equals("") || k_s.isEmpty() || querySizeFrom == 0 || querySizeTo == 0
				|| DummyProperties.similarityThreshold == null) {
			throw new Exception(
					"You should provide all the parameters -queryFileName  -queryFileDirectory -kFrom -kTo -querySizeFrom -querySizeTo -GName -similarityThreshold");
		}

		QueryGenerator queryGenerator = new QueryGenerator(GDirectory + GName);

		// output the results and answers
		File fout = new File(GName + queryFileName + "_answerResults_graphTA.txt");
		FileOutputStream fos = new FileOutputStream(fout);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		// output the
		File foutTime = new File(GName + queryFileName + "_timeResults_graphTA.txt");
		FileOutputStream fosTime = new FileOutputStream(foutTime);

		BufferedWriter bwTime = new BufferedWriter(new OutputStreamWriter(fosTime));

		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(MODELGRAPH_DB_PATH);
		DummyFunctions.printIfItIsInDebuggedMode("after initialization of GraphDatabaseServices");
		registerShutdownHook(knowledgeGraph);

		long start_time, end_time;
		double difference;

		start_time = System.nanoTime();
		HashMap<String, HashSet<Long>> nodeLabelsIndex = PreProcessingLabels.getPrefixLabelsIndex(knowledgeGraph,
				numberOfPrefixChars);

		end_time = System.nanoTime();
		difference = (end_time - start_time) / 1e6;
		DummyFunctions.printIfItIsInDebuggedMode("nodeLabelsIndex in miliseconds: " + difference);
		DummyFunctions.printIfItIsInDebuggedMode("nodeLabelsIndex size: " + nodeLabelsIndex.keySet().size());
		cacheServer = new CacheServer();
		Levenshtein levenshtein = new Levenshtein(nodeLabelsIndex, numberOfPrefixChars);
		for (int querySize = querySizeFrom; querySize <= querySizeTo; querySize++) {
			List<QueryFromFile> queriesFromFile = queryGenerator
					.getQueryFromFile(queryFileDirectory + queryFileName + querySize + ".txt");
			for (QueryFromFile queryFromFile : queriesFromFile) {
				System.gc();
				DummyFunctions.printIfItIsInDebuggedMode("start ConstrucQueryGraph");
				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH,
						queryFromFile);
				DummyFunctions.printIfItIsInDebuggedMode("end ConstrucQueryGraph");

				GraphDatabaseService queryGraph = smallGraph;
				registerShutdownHook(queryGraph);

				for (Integer k : k_s) {

					try (Transaction tx1 = queryGraph.beginTx()) {
						for (Node node : queryGraph.getAllNodes()) {
							Iterator<Label> labelIterator = node.getLabels().iterator();
							while (labelIterator.hasNext()) {
								System.out.println(node.getId() + ", " + labelIterator.next());
							}
						}
						try (Transaction tx2 = knowledgeGraph.beginTx()) {
							DummyFunctions.populatePatternAndModelSize(queryGraph, knowledgeGraph); // for
																									// vf2
																									// improvement
							DummyFunctions
									.createUniqueIdForEachNode(queryGraph/* , knowledgeGraph */);
							start_time = System.nanoTime();
							DummyProperties.nodeIdByUniqueManualIdMap = new HashMap<GraphDatabaseService, HashMap<Integer, Long>>();
							DummyProperties.uniqueManualIdByNodeIdMap = new HashMap<GraphDatabaseService, HashMap<Long, Integer>>();
							DummyFunctions.printIfItIsInDebuggedMode("after creation of uniqueId's maps!");
							DummyFunctions.populateIdUniqueManualIdMaps(knowledgeGraph);
							DummyFunctions.populateIdUniqueManualIdMaps(queryGraph);
							cacheServer.clear();
							end_time = System.nanoTime();
							difference = (end_time - start_time) / 1e6;
							DummyFunctions
									.printIfItIsInDebuggedMode("time for populating the uniqueId maps: " + difference);
							// ResourceIterable<Node> allKnowledgeNodes =
							// GlobalGraphOperations.at(knowledgeGraph)
							// .getAllNodes();
							// for (Node knldgNode : allKnowledgeNodes) {
							// System.out.println(knldgNode.getId());
							// }
							DummyFunctions.printIfItIsInDebuggedMode("after create uniqueIDs  for query nodes");
							GraphTA gt = new GraphTA(queryGraph, knowledgeGraph, k, levenshtein);
							start_time = System.nanoTime();
							List<String> results = gt.runGraphTA();
							end_time = System.nanoTime();
							difference = (end_time - start_time) / 1e6;
							System.out.println("GraphTA finished in " + difference + "miliseconds!");

							// |Q|, GName, k, actualResultsReterned, Alg, time
							String timeLineResult = "";
							timeLineResult += querySize + ";";
							timeLineResult += GName + ";";
							timeLineResult += results.size() + ";";
							timeLineResult += k + ";";
							timeLineResult += "GraphTA;";
							timeLineResult += difference + ";";

							for (String result : results) {
								if (!result.equals("") && !result.contains("Final Result")) {
									bw.write("querySize;" + querySize + "; k;" + k + "; " + result);
									bw.newLine();
								}
							}
							bw.newLine();
							bw.flush();
							bwTime.write(querySize + ";" + GName + ";" + k + ";" + "GraphTA;" + difference);
							bwTime.flush();
							bwTime.newLine();
							tx2.success();
						} catch (Exception exc) {
							System.out.println("queryGraph Transaction failed");
							exc.printStackTrace();
						}

						tx1.success();
					} catch (Exception exc) {
						System.out.println("modelGraph Transaction failed");
						bw.close();
						bwTime.close();
						exc.printStackTrace();
					}

				}
				queryGraph.shutdown();
			}

		}
		bw.close();
		bwTime.close();
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
