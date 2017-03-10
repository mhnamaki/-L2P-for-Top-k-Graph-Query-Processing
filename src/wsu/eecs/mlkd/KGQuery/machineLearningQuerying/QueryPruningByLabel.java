package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import wsu.eecs.mlkd.KGQuery.TopKQuery.CacheServer;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.NeighborIndexing;
import wsu.eecs.mlkd.KGQuery.TopKQuery.StarFramework;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;
import wsu.eecs.mlkd.KGQuery.test.QueryFromFile;
import wsu.eecs.mlkd.KGQuery.test.QueryGenerator;

public class QueryPruningByLabel {

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
		QueryPruningByLabel foq = new QueryPruningByLabel();
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

		long start_time, end_time;
		double difference;

		neighborIndexingInstance = new NeighborIndexing();

		int queryIndex = startingIndex;
		// it reads query files
		for (File file : fileInTheDirfinder(queryFileDirectory)) {
			queryFileName = file.getName();

			List<QueryFromFile> queriesFromFile = queryGenerator.getQueryFromFile(queryFileDirectory + queryFileName);

			// it reads queries from a file.
			for (QueryFromFile queryFromFile : queriesFromFile) {

				queryIndex++;

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
					boolean badLabel = false;
					for (Long nodeId : neighborIndexingInstance.queryNodeLabelMap.keySet()) {
						if (neighborIndexingInstance.queryNodeLabelMap.get(nodeId).length() < 13) {
							badLabel = true;
							break;
						}
					}
					if (!badLabel) {
						for (Long nodeId : neighborIndexingInstance.queryNodeLabelMap.keySet()) {
							if (neighborIndexingInstance.queryNodeLabelMap.get(nodeId).toLowerCase()
									.contains("base.type_ontology.non_agent")
									|| neighborIndexingInstance.queryNodeLabelMap.get(nodeId).toLowerCase()
											.contains("base.type_ontology.abstract")
									|| neighborIndexingInstance.queryNodeLabelMap.get(nodeId).toLowerCase()
											.contains("base.type_ontology.agent")
									|| neighborIndexingInstance.queryNodeLabelMap.get(nodeId).toLowerCase()
											.contains("base.type_ontology.inanimate")) {
								badLabel = true;
							}
						}
					}
					if (badLabel) {
						txQ.success();
						txQ.close();
						queryGraph.shutdown();
						queryGraph = null;
						continue;
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

}
