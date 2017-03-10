package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import wsu.eecs.mlkd.KGQuery.TopKQuery.AnyTimeStarFramework;
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

public class AnyTimeFilterOutQuery {
	private static String MODELGRAPH_DB_PATH = "";
	private static String PATTERNGRAPH_DB_PATH = "";

	public static String queryFileName = "";
	public static String queryFileDirectory = "";
	public static int k = 0;
	public AnyTimeStarFramework anyTimeStarFramework;
	public static String GName = ""; // Yago, DBPedia, ...

	public static String queryDBInNeo4j = "query.db";
	public static String GDirectory = "";
	public static int numberOfSameExperiment = 5;

	public GraphDatabaseService queryGraph;
	public GraphDatabaseService knowledgeGraph;
	public float alpha = 0.5F;
	public Levenshtein levenshtein;
	public CacheServer cacheServer;
	public HashMap<Integer, TreeNode<CalculationNode>> calcTreeNodeMap;
	public HashMap<Integer, CalculationTreeSiblingNodes> joinLevelSiblingNodesMap;
	public HashMap<Long, Integer> nodeEstimationMap;
	public int startingQueryIndex = 0;
	public NeighborIndexing neighborIndexingInstance;

	public static void main(String[] args) throws Exception {
		AnyTimeFilterOutQuery foq = new AnyTimeFilterOutQuery();
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
			} else if (args[i].equals("-startingQueryIndex")) {
				startingQueryIndex = Integer.parseInt(args[++i]);
			}

		}

		if (numberOfPrefixChars > 0) {
			DummyProperties.numberOfPrefixChars = numberOfPrefixChars;
		}
		if (!GDirectory.endsWith("/")) {
			GDirectory += "/";
		}
		MODELGRAPH_DB_PATH = GDirectory + GName;
		PATTERNGRAPH_DB_PATH = queryFileDirectory + queryDBInNeo4j;

		QueryGenerator queryGenerator = new QueryGenerator(GDirectory + GName);

		knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(MODELGRAPH_DB_PATH)
				.setConfig(GraphDatabaseSettings.pagecache_memory, "500M").newGraphDatabase();

		DummyFunctions.printIfItIsInDebuggedMode("after initialization of GraphDatabaseServices");
		registerShutdownHook(knowledgeGraph);

		long start_time, end_time;
		double difference;

		HashMap<String, HashSet<Long>> nodeLabelsIndex = PreProcessingLabels.getPrefixLabelsIndex(knowledgeGraph,
				Dummy.DummyProperties.numberOfPrefixChars);

		neighborIndexingInstance = new NeighborIndexing();
		start_time = System.nanoTime();
		neighborIndexingInstance.knowledgeGraphNeighborIndexer(knowledgeGraph);
		end_time = System.nanoTime();
		difference = (end_time - start_time) / 1e6;
		System.out.println("knowledgeGraphNeighborIndexer finished in " + difference + "miliseconds!");

		float alpha = 0.5F;
		levenshtein = new Levenshtein(nodeLabelsIndex, Dummy.DummyProperties.numberOfPrefixChars);

		// String nodeEstimation = "";
		// String queryInfo = "";
		// String joinableNodes = "";

		for (File file : fileInTheDirfinder(queryFileDirectory)) {
			queryFileName = file.getName();

			// output the queries
			// File fout = new File(queryFileName + "_filtered.txt");
			// FileOutputStream fos = new FileOutputStream(fout);
			//
			// BufferedWriter bwFilteredQueries = new BufferedWriter(new
			// OutputStreamWriter(fos));

			List<QueryFromFile> queriesFromFile = queryGenerator.getQueryFromFile(queryFileDirectory + queryFileName);
			int queryIndex = 0;
			for (QueryFromFile queryFromFile : queriesFromFile) {

				queryIndex++;
				if (queryIndex < startingQueryIndex) {
					continue;
				}
				// DummyFunctions.printIfItIsInDebuggedMode("start
				// ConstrucQueryGraph");
				System.out.println("queryIndex: " + queryIndex);
				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH,
						queryFromFile);
				// DummyFunctions.printIfItIsInDebuggedMode("end
				// ConstrucQueryGraph");

				queryGraph = smallGraph;
				//registerShutdownHook(queryGraph);

				start_time = System.nanoTime();
				neighborIndexingInstance.queryNeighborIndexer(queryGraph);
				end_time = System.nanoTime();
				difference = (end_time - start_time) / 1e6;
				// Dummy.DummyFunctions
				// .printIfItIsInDebuggedMode("queryNeighborIndexer finished in
				// " + difference + "miliseconds!");
				try (Transaction txG = knowledgeGraph.beginTx()) {
					try (Transaction txQ = queryGraph.beginTx()) {

						int numberOfQNodes = neighborIndexingInstance.queryNodeIdSet.size();

						boolean dontRunThisQuery = false;
						for (Long qNodeId : neighborIndexingInstance.queryNodeLabelMap.keySet()) {
							if (neighborIndexingInstance.queryNodeLabelMap.get(qNodeId)
									.length() < DummyProperties.numberOfPrefixChars) {
								dontRunThisQuery = true;
							}
						}
						if (dontRunThisQuery) {
							txQ.success();
							txQ.close();
							txG.success();
							txG.close();
							queryGraph.shutdown();
							queryGraph = null;

							System.out.println();
							// System.gc();
							// System.runFinalization();
							continue;
						}

						boolean timeOutOccurred = false;
						Double diffTime = 0d;
						int finalDifferenceTime = 0;
						ArrayList<Integer> differenceTimes = new ArrayList<Integer>();

						for (int exp = 0; exp < numberOfSameExperiment; exp++) {
							// if (exp == 0) {
							// Dummy.DummyProperties.debuggMode = true;
							// } else {
							// Dummy.DummyProperties.debuggMode = false;
							// }
							anyTimeStarFramework = getNewStarFrameworkInstance();

							start_time = System.nanoTime();
							anyTimeStarFramework.starRoundRobinRun(queryGraph, knowledgeGraph, neighborIndexingInstance, cacheServer);
							if (anyTimeStarFramework.timeOut) {
								timeOutOccurred = true;
								break;
							}
							end_time = System.nanoTime();
							diffTime = (end_time - start_time) / 1e6;
							// System.out.println("StarFramework exp: " + exp +
							// " is finished in " + diffTime.intValue()
							// + " miliseconds!");

							differenceTimes.add(diffTime.intValue());

							if (exp != (numberOfSameExperiment - 1)) {
								anyTimeStarFramework = null;
							}
						}
						// System.gc();
						// System.runFinalization();

						if (timeOutOccurred) {
							System.err.println("No Answer Found for query index! " + queryIndex);
							txQ.success();
							txQ.close();
							txG.success();
							txG.close();
							queryGraph.shutdown();
							queryGraph = null;
							System.out.println();
							anyTimeStarFramework = null;
							continue;
						}

						finalDifferenceTime = DummyFunctions.computeNonOutlierAverage(differenceTimes,
								numberOfSameExperiment);
						differenceTimes = null;
						System.out.println("StarFramework avg is finished in " + finalDifferenceTime + " miliseconds!");

						HashSet<Long> allNodesForSaving = new HashSet<Long>();
						for (Long qNodeId : neighborIndexingInstance.queryNodeIdSet) {
							allNodesForSaving.add(qNodeId);
						}

						queryGenerator.SaveQueryInfo(queryIndex, allNodesForSaving, queryFileName, null, null, null,
								null, neighborIndexingInstance);

						System.out.println();
						// writing in another file.

						// allNodesForSaving = null;
						// nodeEstimation = null;
						// queryInfo = null;
						anyTimeStarFramework = null;
						txQ.success();
						txQ.close();
					}

					catch (Exception exc) {
						System.out.println("queryGraph Transaction failed");
						exc.printStackTrace();
						// System.gc();
						// System.runFinalization();
					}

					txG.success();
					txG.close();
				} catch (Exception exc) {
					System.out.println("modelGraph Transaction failed");
					exc.printStackTrace();
					// System.gc();
					// System.runFinalization();
				}

				queryGraph.shutdown();
				queryGraph = null;
				smallGraph = null;
				// System.gc();
				// System.runFinalization();
			}
			startingQueryIndex = 0;
			queriesFromFile = null;
			// System.gc();
			// System.runFinalization();
			// bwFilteredQueries.close();
		}

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

	public static File[] fileInTheDirfinder(String dirName) {
		File dir = new File(dirName);

		return dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String filename) {
				return filename.endsWith(".txt");
			}
		});

	}

	public AnyTimeStarFramework getNewStarFrameworkInstance(GraphDatabaseService queryGraph,
			GraphDatabaseService knowledgeGraph, int k2, float alpha, Levenshtein levenshtein,
			NeighborIndexing neighborIndexingInstance) {
		this.anyTimeStarFramework = null;
		this.nodeEstimationMap = null;

		cacheServer.clear();
		this.anyTimeStarFramework = new AnyTimeStarFramework(queryGraph, knowledgeGraph, k, alpha, levenshtein);
		nodeEstimationMap = anyTimeStarFramework.decomposeQuery(queryGraph, knowledgeGraph, neighborIndexingInstance,
				cacheServer);

		return anyTimeStarFramework;
	}

	public AnyTimeStarFramework getNewStarFrameworkInstance() {
		return getNewStarFrameworkInstance(queryGraph, knowledgeGraph, k, alpha, levenshtein, neighborIndexingInstance);
	}
}
