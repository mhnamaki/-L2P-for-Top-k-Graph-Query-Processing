package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import wsu.eecs.mlkd.KGQuery.TopKQuery.CacheServer;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CalculationNode;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CalculationTreeSiblingNodes;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.NeighborIndexing;
import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;
import wsu.eecs.mlkd.KGQuery.TopKQuery.StarFramework;
import wsu.eecs.mlkd.KGQuery.TopKQuery.TreeNode;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;
import wsu.eecs.mlkd.KGQuery.test.QueryFromFile;
import wsu.eecs.mlkd.KGQuery.test.QueryGenerator;

public class QueryIndexer {
	private static String MODELGRAPH_DB_PATH = "";
	private static String PATTERNGRAPH_DB_PATH = "";

	public static String queryFileName = "";
	public static String queryFileDirectory = "";
	public static int k = 0;
	public StarFramework starFramework2;
	public static String GName = ""; // Yago, DBPedia, ...

	public static String queryDBInNeo4j = "query";
	public static String GDirectory = "";

	public GraphDatabaseService queryGraph;
	public GraphDatabaseService knowledgeGraph;
	public float alpha = 0.5F;
	public Levenshtein levenshtein;
	public CacheServer cacheServer;
	public HashMap<Long, Integer> nodeEstimationMap;
	// public int startingQueryIndex = 0;
	public NeighborIndexing neighborIndexingInstance;
	// private int endingQueryIndex;
	private int startingIndex;

	public static void main(String[] args) throws Exception {
		QueryIndexer foq = new QueryIndexer();
		foq.initialize(args);
	}

	public void initialize(String[] args) throws Exception {

		cacheServer = new CacheServer();

		DummyProperties.debuggMode = false;

		int numberOfPrefixChars = 0;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-queryFileName")) {
				queryFileName = args[++i];
				queryFileName = queryFileName.replace(".txt", "");
			} else if (args[i].equals("-queryFileDirectory")) {
				queryFileDirectory = args[++i];
				if (!queryFileDirectory.endsWith("/") && !queryFileDirectory.equals("")) {
					queryFileDirectory += "/";
				}
			} else if (args[i].equals("-k")) {
				k = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-GName")) {
				GName = args[++i];
			} else if (args[i].equals("-GDirectory")) {
				GDirectory = args[++i];

			} else if (args[i].equals("-similarityThreshold")) {
				DummyProperties.similarityThreshold = Float.parseFloat(args[++i]);
			} else if (args[i].equals("-numberOfPrefixChars")) {
				numberOfPrefixChars = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-startingIndex")) {
				startingIndex = Integer.parseInt(args[++i]);
			}

		}

		if (numberOfPrefixChars > 0) {
			DummyProperties.numberOfPrefixChars = numberOfPrefixChars;
		}
		if (!GDirectory.endsWith("/")) {
			GDirectory += "/";
		}
		MODELGRAPH_DB_PATH = GDirectory + GName;
		PATTERNGRAPH_DB_PATH = queryFileDirectory + queryDBInNeo4j + startingIndex + ".db";

		QueryGenerator queryGenerator = new QueryGenerator(GDirectory + GName);

		// knowledgeGraph = new
		// GraphDatabaseFactory().newEmbeddedDatabaseBuilder(MODELGRAPH_DB_PATH)
		// .setConfig(GraphDatabaseSettings.pagecache_memory,
		// "500M").newGraphDatabase();
		//
		// DummyFunctions.printIfItIsInDebuggedMode("after initialization of
		// GraphDatabaseServices");
		// registerShutdownHook(knowledgeGraph);

		long start_time, end_time;
		double difference;

		// HashMap<String, HashSet<Long>> nodeLabelsIndex =
		// PreProcessingLabels.getPrefixLabelsIndex(knowledgeGraph,
		// Dummy.DummyProperties.numberOfPrefixChars);

		neighborIndexingInstance = new NeighborIndexing();
		// start_time = System.nanoTime();
		// neighborIndexingInstance.knowledgeGraphNeighborIndexer(knowledgeGraph);
		// end_time = System.nanoTime();
		// difference = (end_time - start_time) / 1e6;
		// System.out.println("knowledgeGraphNeighborIndexer finished in " +
		// difference + "miliseconds!");

		// float alpha = 0.5F;
		// levenshtein = new Levenshtein(nodeLabelsIndex,
		// Dummy.DummyProperties.numberOfPrefixChars);

		int queryIndex = startingIndex;
		// it reads query files
		for (File file : fileInTheDirfinder(queryFileDirectory)) {
			queryFileName = file.getName();
		
			System.out.println("file: " + queryFileName + ", " + queryIndex);

			List<QueryFromFile> queriesFromFile = queryGenerator.getQueryFromFile(queryFileDirectory + queryFileName);

			// it reads queries from a file.
			for (QueryFromFile queryFromFile : queriesFromFile) {

				queryIndex++;

				// if (queryIndex < startingQueryIndex || queryIndex >
				// endingQueryIndex) {
				// continue;
				// }

				System.out.println(queryFileName + " queryIndex: " + queryIndex);
				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH,
						queryFromFile);

				queryGraph = smallGraph;

				neighborIndexingInstance.queryNeighborIndexer(queryGraph);

				// for each query, it opens two nested transactions
				// try (Transaction txG = knowledgeGraph.beginTx()) {
				try (Transaction txQ = queryGraph.beginTx()) {

					HashSet<Long> allNodesForSaving = new HashSet<Long>();
					for (Long qNodeId : neighborIndexingInstance.queryNodeIdSet) {
						allNodesForSaving.add(qNodeId);
					}

					int offset = queryIndex / 50000;
					// saving the query in another file with some new
					// information
					queryGenerator.SaveQueryInfo(queryIndex, allNodesForSaving, queryFileName, null, null, null, null,
							neighborIndexingInstance, 0, offset);

					txQ.success();
					txQ.close();
				}

				catch (Exception exc) {
					System.out.println("queryGraph Transaction failed");
					exc.printStackTrace();
					System.gc();
					System.runFinalization();
				}

				// txG.success();
				// txG.close();
				// }
				// catch (Exception exc) {
				// System.out.println("modelGraph Transaction failed");
				// exc.printStackTrace();
				// System.gc();
				// System.runFinalization();
				// }

				queryGraph.shutdown();
				queryGraph = null;
				smallGraph = null;
				System.gc();
				System.runFinalization();
			}
			// startingQueryIndex = 0;
			queriesFromFile = null;
			System.gc();
			System.runFinalization();
		}

		// knowledgeGraph.shutdown();
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	public static File[] fileInTheDirfinder(String dirName) {
		File dir = new File(dirName);

		File[] files = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String filename) {
				return filename.endsWith(".txt");
			}
		});

		Arrays.sort(files);

		for (int i = 0; i < files.length; i++) {
			System.out.println("catched file " + i + "; " + files[i].getName());
		}
		return files;

	}

	// public StarFramework getNewStarFrameworkInstance(GraphDatabaseService
	// queryGraph,
	// GraphDatabaseService knowledgeGraph, int k2, float alpha, Levenshtein
	// levenshtein,
	// NeighborIndexing neighborIndexingInstance) {
	// this.starFramework2 = null;
	// this.nodeEstimationMap = null;
	//
	// cacheServer.clear();
	// this.starFramework2 = new StarFramework(queryGraph, knowledgeGraph, k,
	// alpha, levenshtein);
	// nodeEstimationMap = starFramework2.decomposeQuery(queryGraph,
	// knowledgeGraph, neighborIndexingInstance,
	// cacheServer);
	//
	// return starFramework2;
	// }

	// public StarFramework getNewStarFrameworkInstance() {
	// return getNewStarFrameworkInstance(queryGraph, knowledgeGraph, k, alpha,
	// levenshtein, neighborIndexingInstance);
	// }
}
