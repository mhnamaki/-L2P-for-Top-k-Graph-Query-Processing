package wsu.eecs.mlkd.KGQuery.test;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import wsu.eecs.mlkd.KGQuery.TopKQuery.*;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;

public class QueryGeneratorFromTemplatesFreqLblForJar {

	private static String MODELGRAPH_DB_PATH = "";
	private static String PATTERNGRAPH_DB_PATH = "";

	public static String templateFilePath = "/Users/mnamaki/Documents/Education/PhD/Spring2016/Research/queriesTemplatePruned.txt";
	public static String graphFileDirectory = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.3.1/data/yago.db";
	public static int k = 10;
	private static String outputFileName = "resultsQueries.csv";
	
	public static String queryDBInNeo4j = "query.db";
	public static int numberOfPrefixChars = 12;
	private static List<ArrayList<Object>> allPossibleLabel = new ArrayList<>();
	private static BufferedWriter outputFile = null;
	
	// public static String GDirectory = "";
	public static CacheServer cacheServer;
	public static int startingQueryIndex = 0;
	public static int endingQueryIndex = 10000000;
	public static int startingQueryIndexForLabeling;
	private static String debugMode;

	public static void main(String[] args) throws Exception {
		DummyProperties.similarityThreshold = 0.3F;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-templateFilePath")) {
				templateFilePath = args[++i];
			} else if (args[i].equals("-graphFileDirectory")) {
				graphFileDirectory = args[++i];
			} else if (args[i].equals("-similarityThreshold")) {
				DummyProperties.similarityThreshold = Float.parseFloat(args[++i]);
			} else if (args[i].equals("-k")) {
				k = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-outputFileName")) {
				outputFileName = args[++i];
			} else if (args[i].equals("-startingQueryIndex")) {
				startingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-endingQueryIndex")) {
				endingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-startingQueryIndexLabel")) {
				startingQueryIndexForLabeling = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-debugMode")) {
				debugMode = args[++i];
			}
		}

		if (debugMode != null && debugMode.toLowerCase().equals("true")) {
			DummyProperties.debuggMode = true;
		}

		if (startingQueryIndex > 1 && startingQueryIndexForLabeling == 0) {
			startingQueryIndexForLabeling = k * startingQueryIndex + 1;
		}
		if (!graphFileDirectory.endsWith("/")) {
			graphFileDirectory += "/";
		}
		MODELGRAPH_DB_PATH = graphFileDirectory;
		PATTERNGRAPH_DB_PATH = queryDBInNeo4j;

		QueryGenerator queryGenerator = new QueryGenerator(graphFileDirectory);

		// output the results and answers
		// File fout = new File(GName + queryFileName +
		// "_answerResults_starFramework.txt");
		// FileOutputStream fos = new FileOutputStream(fout);
		//
		// BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		// output the
		// File foutTime = new File(GName + queryFileName +
		// "_timeResults_starFramework.txt");
		// FileOutputStream fosTime = new FileOutputStream(foutTime);
		//
		// BufferedWriter bwTime = new BufferedWriter(new
		// OutputStreamWriter(fosTime));

		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(MODELGRAPH_DB_PATH)
				.setConfig(GraphDatabaseSettings.pagecache_memory, "4g").newGraphDatabase();
		System.out.println("after initialization of GraphDatabaseServices");
		registerShutdownHook(knowledgeGraph);

		long start_time, end_time;
		double difference;

		NeighborIndexing neighborIndexingInstance = new NeighborIndexing();
		start_time = System.nanoTime();
		neighborIndexingInstance.knowledgeGraphNeighborIndexer(knowledgeGraph);
		end_time = System.nanoTime();
		difference = (end_time - start_time) / 1e6;
		System.out.println("knowledgeGraphNeighborIndexer finished in " + difference + "miliseconds!");

		float alpha = 0.5F;

		List<QueryFromFile> queriesFromFile = queryGenerator.getQueryFromFile(templateFilePath);
		int queryIndex = 0;
		for (QueryFromFile queryFromFile : queriesFromFile) {
			System.gc();
			System.out.println("start ConstrucQueryGraph");

			queryIndex = queryFromFile.queryIndex;
			if (queryIndex < startingQueryIndex || queryIndex > endingQueryIndex) {
				continue;
			}

			GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH, queryFromFile);
			System.out.println("end ConstrucQueryGraph");

			GraphDatabaseService queryGraph = smallGraph;
			// registerShutdownHook(queryGraph);

			start_time = System.nanoTime();
			Dummy.DummyProperties.semanticChecking = false;
			neighborIndexingInstance.queryNeighborIndexer(queryGraph);
			end_time = System.nanoTime();
			difference = (end_time - start_time) / 1e6;
			System.out.println("queryNeighborIndexer finished in " + difference + "miliseconds!");

			boolean theQueryHasAnyAnswer = true;
			// for (int exp = 0; exp < numberOfSameExperiment &&
			// theQueryHasAnyAnswer; exp++) {
			// for (Integer k : k_s) {

			// System.out.println("querySize: " + querySize + ", queryIndex: " +
			// queryIndex + " k: " + k);
			HashMap<String, HashSet<Long>> nodeLabelsIndex = PreProcessingLabels.getPrefixLabelsIndex(knowledgeGraph,
					numberOfPrefixChars);
			Levenshtein levenshtein = new Levenshtein(nodeLabelsIndex, numberOfPrefixChars);
			try (Transaction tx1 = queryGraph.beginTx()) {
				try (Transaction tx2 = knowledgeGraph.beginTx()) {

					// CacheServer.clear();

					StarFramework starFramework2 = new StarFramework(queryGraph, knowledgeGraph, k, alpha, null);
					starFramework2.decomposeQuery(queryGraph, knowledgeGraph, neighborIndexingInstance, cacheServer);

					start_time = System.nanoTime();

					ArrayList<GraphResult> finalResults = starFramework2.starRun(queryGraph, knowledgeGraph,
							neighborIndexingInstance, cacheServer);

					end_time = System.nanoTime();
					difference = (end_time - start_time) / 1e6;
					System.out.println("StarFramework finished in " + difference + "miliseconds!");

					starFramework2 = null;
					System.gc();
					System.runFinalization();

					int finalResultSizeTemp = finalResults.size();
					System.out.println("finalResults: " + finalResults.size());
					if (finalResultSizeTemp == 0) {
						System.err.println("No Answer Found for query index! " + queryIndex);
						theQueryHasAnyAnswer = false;
						break;
					} else {

						outputFile = new BufferedWriter(new FileWriter(outputFileName + "_" + queryIndex + ".txt"));
						for (int i = 0; (i < k) && (i < finalResults.size()); i++) {
							GraphResult gr = finalResults.get(i);
							String result = "";
							for (Node qNode : gr.assembledResult.keySet()) {
								result += gr.assembledResult.get(qNode).node.getId() + "-";
							}
							result += " -> " + gr.getTotalValue();

							outputFile.write(result);
							outputFile.newLine();
						}

						finalResults.clear();
						outputFile.close();
						System.out.println();
						System.out.println();
					}

					tx2.success();
					System.out.println("last");
					// DummyFunctions.printIfItIsInDebuggedMode("before
					// tx2 success");

				} catch (Exception exc) {
					System.out.println("queryGraph Transaction failed");
					exc.printStackTrace();
				}
				// DummyFunctions.printIfItIsInDebuggedMode("before
				// tx1
				// success");
				tx1.success();
				// DummyFunctions.printIfItIsInDebuggedMode("after
				// tx1
				// success");
			} catch (Exception exc) {
				System.out.println("modelGraph Transaction failed");
				exc.printStackTrace();
			}

			queryGraph.shutdown();
		}

		// }
		// bw.close();
		// bwTime.close();
		knowledgeGraph.shutdown();

	}

	/* remove labels from a node */
	private static void RemoveLabels(Node node) {
		Iterable<Label> nodeLabel = node.getLabels();
		Iterator<Label> labelIterator = nodeLabel.iterator();
		while (labelIterator.hasNext()) {
			Label label = labelIterator.next();
			node.removeLabel(label);
		}
	}

	private static void printCombinations(String[][] sets, int n, String prefix) {
		if (n >= sets.length) {
			// System.out.println("{"+prefix.substring(0,prefix.length()-1)+"}");
			String[] labels = prefix.split(",");
			int cntSize = labels.length;
			int allPossibleSize = allPossibleLabel.size();
			allPossibleLabel.add(allPossibleSize, new ArrayList<>());
			for (int cnt = 0; cnt < cntSize; ++cnt) {
				allPossibleLabel.get(allPossibleSize).add(labels[cnt]);
			}
			return;
		}
		for (String s : sets[n]) {
			s = s.replaceAll(",", ""); // some labels got , inside so I am
										// removing them.
			printCombinations(sets, n + 1, prefix + s + ",");
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

	class FinalResultForLabelSelect {
		HashMap<Long, TreeMap<Integer, String>> frequentlabelsQNodeMap = new HashMap<Long, TreeMap<Integer, String>>();

		public FinalResultForLabelSelect(HashMap<Long, TreeMap<Integer, String>> frequentlabelsQNodeMap) {
			this.frequentlabelsQNodeMap = frequentlabelsQNodeMap;
		}

	}
}
