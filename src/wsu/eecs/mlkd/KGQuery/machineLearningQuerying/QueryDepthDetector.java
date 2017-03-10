package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import javax.management.Query;

import org.neo4j.cypher.internal.compiler.v2_2.perty.print.condense;
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
import wsu.eecs.mlkd.KGQuery.TopKQuery.GraphResult;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.NeighborIndexing;
import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;
import wsu.eecs.mlkd.KGQuery.TopKQuery.StarFramework;
import wsu.eecs.mlkd.KGQuery.TopKQuery.TreeNode;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;
import wsu.eecs.mlkd.KGQuery.test.QueryFromFile;
import wsu.eecs.mlkd.KGQuery.test.QueryGenerator;

public class QueryDepthDetector {
	private static String MODELGRAPH_DB_PATH = "";
	private static String PATTERNGRAPH_DB_PATH = "";

	public static String queryFileName = "";
	public static String queryFileDirectory = "";
	public static int k = 0;
	public AnyTimeStarFramework anyTimeStarFramework;
	public static String GName = ""; // Yago, DBPedia, ...

	public static String queryDBInNeo4j = "query";
	public static String GDirectory = "";
	public static int numberOfSameExperiment = 2;

	public GraphDatabaseService queryGraph;
	public GraphDatabaseService knowledgeGraph;
	public float alpha = 0.5F;
	public Levenshtein levenshtein;
	public CacheServer cacheServer;
	public HashMap<Integer, TreeNode<CalculationNode>> calcTreeNodeMap;
	public HashMap<Integer, CalculationTreeSiblingNodes> joinLevelSiblingNodesMap;
	public HashMap<Long, Integer> nodeEstimationMap;
	public int startingQueryIndex = 0;
	public int endingQueryIndex = 0;

	public NeighborIndexing neighborIndexingInstance;
	public String previousQueryInfoAddress;

	public static void main(String[] args) throws Exception {
		QueryDepthDetector foq = new QueryDepthDetector();
		foq.initialize(args);
	}

	public void initialize(String[] args) throws Exception {

		File foutQueryInfo = new File("allQueryInfo.txt");
		FileOutputStream fosQueryInfo = new FileOutputStream(foutQueryInfo, true);

		BufferedWriter bwQueryInfo = new BufferedWriter(new OutputStreamWriter(fosQueryInfo));

		cacheServer = new CacheServer();

		DummyProperties.debuggMode = false;

		int numberOfPrefixChars = 0;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-queryFileName")) {
				queryFileName = args[++i];
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

			} else if (args[i].equals("-previousQueryInfoAddress")) {
				previousQueryInfoAddress = args[++i];
			} else if (args[i].equals("-similarityThreshold")) {
				DummyProperties.similarityThreshold = Float.parseFloat(args[++i]);
			} else if (args[i].equals("-numberOfPrefixChars")) {
				numberOfPrefixChars = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-startingQueryIndex")) {
				startingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-numberOfSameExperiment")) {
				numberOfSameExperiment = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-endingQueryIndex")) {
				endingQueryIndex = Integer.parseInt(args[++i]);
			}

		}

		if (numberOfPrefixChars > 0) {
			DummyProperties.numberOfPrefixChars = numberOfPrefixChars;
		}
		if (!GDirectory.endsWith("/")) {
			GDirectory += "/";
		}
		MODELGRAPH_DB_PATH = GDirectory + GName;
		PATTERNGRAPH_DB_PATH = queryFileDirectory + queryDBInNeo4j + startingQueryIndex + ".db";

		QueryGenerator queryGenerator = new QueryGenerator(GDirectory + GName);

		knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(MODELGRAPH_DB_PATH).newGraphDatabase();

		System.out.println("after initialization of GraphDatabaseServices");
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

		int queryIndex = 0;

		// temp lines
		HashSet<Integer> registeredQueriesSet = new HashSet<Integer>();
		if (previousQueryInfoAddress != null) {
			ArrayList<QueryInfo> queryInfos = QueryInfo.queryInfoRead(previousQueryInfoAddress);
			for (QueryInfo qi : queryInfos) {
				registeredQueriesSet.add(qi.queryId);
			}
		}
		// it reads query files
		for (File file : fileInTheDirfinder(queryFileDirectory)) {
			queryFileName = file.getName();
			System.out.println("query file is changed to: " + queryFileName);

			List<QueryFromFile> queriesFromFile = queryGenerator.getQueryFromFile(queryFileDirectory + queryFileName);

			// it reads queries from a file.
			for (QueryFromFile queryFromFile : queriesFromFile) {

				queryIndex = queryFromFile.queryIndex;

				if (queryIndex < startingQueryIndex || queryIndex > endingQueryIndex) {
					continue;
				}
				// TODO: may be we should change this line based on the
				// requirements
				// of experiment
				if (previousQueryInfoAddress != null && !registeredQueriesSet.contains(queryIndex)) {
					continue;
				}

				System.out.println(queryFileName + " queryIndex: " + queryIndex);
				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH,
						queryFromFile);

				queryGraph = smallGraph;

				// registerShutdownHook(queryGraph);

				start_time = System.nanoTime();
				neighborIndexingInstance.queryNeighborIndexer(queryGraph);
				end_time = System.nanoTime();
				difference = (end_time - start_time) / 1e6;

				// for each query, it opens two nested transactions
				try (Transaction txG = knowledgeGraph.beginTx()) {
					try (Transaction txQ = queryGraph.beginTx()) {

						// if there are some conditions don't run this query
						boolean dontRunThisQuery = false;
						for (Long qNodeId : neighborIndexingInstance.queryNodeLabelMap.keySet()) {
							if (neighborIndexingInstance.queryNodeLabelMap.get(qNodeId)
									.length() < DummyProperties.numberOfPrefixChars) {
								dontRunThisQuery = true;
							}
						}
						if (dontRunThisQuery) {
							System.err.println("dontRunThisQuery");
							txQ.success();
							txQ.close();
							txG.success();
							txG.close();
							queryGraph.shutdown();
							queryGraph = null;
							System.out.println();
							System.gc();
							System.runFinalization();
							continue;
						}

						Double diffTime = 0d;
						Double finalDifferenceTime = 0d;
						ArrayList<Double> differenceTimes = new ArrayList<Double>();

						// run the query
						boolean timeOutOccurrred = false;
						boolean tooManyPA = false;
						for (int exp = 0; exp < numberOfSameExperiment; exp++) {
							// Dummy.DummyProperties.debuggMode= true;
							anyTimeStarFramework = getNewStarFrameworkInstance();

							for (Long nodeId : nodeEstimationMap.keySet()) {
								if (nodeEstimationMap.get(nodeId) > 400000) {
									System.out.println(
											nodeId + " ->" + neighborIndexingInstance.queryNodeLabelMap.get(nodeId));
									tooManyPA = true;
								}
							}
							if (tooManyPA) {
								break;
							}
							// if (exp == 0) {
							// Dummy.DummyProperties.debuggMode = true;
							// }
							// else {
							// Dummy.DummyProperties.debuggMode = false;
							// }
							start_time = System.nanoTime();

							anyTimeStarFramework.starRoundRobinRun(queryGraph, knowledgeGraph, neighborIndexingInstance,
									cacheServer);

							end_time = System.nanoTime();
							diffTime = (end_time - start_time) / 1e6;
							System.out.println("StarFramework exp: " + exp + " is finished in " + diffTime.intValue()
									+ " miliseconds!");

							differenceTimes.add(diffTime);

							if (anyTimeStarFramework.timeOut) {
								timeOutOccurrred = true;
								break;
							}
							if (exp != (numberOfSameExperiment - 1)) {
								anyTimeStarFramework = null;
							}
						}

						if (tooManyPA) {
							System.err.println("tooManyPA Occurrred");
							txQ.success();
							txQ.close();
							txG.success();
							txG.close();
							queryGraph.shutdown();
							queryGraph = null;
							continue;
						}
						if (timeOutOccurrred) {
							System.err.println("timeOutOccurrred");
							txQ.success();
							txQ.close();
							txG.success();
							txG.close();
							queryGraph.shutdown();
							queryGraph = null;
							continue;
						}
						finalDifferenceTime = Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimes,
								numberOfSameExperiment);
						System.out.println("StarFramework avg is finished in " + finalDifferenceTime + " miliseconds!");

						finalizeMethod(queryIndex, anyTimeStarFramework, finalDifferenceTime, bwQueryInfo);

						anyTimeStarFramework = null;
						txQ.success();
						txQ.close();
					}

					catch (Exception exc) {
						System.out.println("queryGraph Transaction failed");
						exc.printStackTrace();
						System.gc();
						System.runFinalization();
					}

					txG.success();
					txG.close();
				} catch (Exception exc) {
					System.out.println("modelGraph Transaction failed");
					exc.printStackTrace();
					System.gc();
					System.runFinalization();
				}

				queryGraph.shutdown();
				queryGraph = null;
				System.gc();
				System.runFinalization();
			}
			// startingQueryIndex = 0;
			queriesFromFile = null;
			System.gc();
			System.runFinalization();
		}

		knowledgeGraph.shutdown();
		bwQueryInfo.close();
	}

	public void finalizeMethod(int queryIndex, AnyTimeStarFramework anyTimeStarFramework, Double time,
			BufferedWriter bwQueryInfo) throws Exception {
		TreeNode<CalculationNode> tempNode = anyTimeStarFramework.rootTreeNode;
		HashMap<Integer, Integer> calcTreeNodeStarQueryMaxDepthMap = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> maxAnyTimeAnswerDepthStarQueryMap = new HashMap<Integer, Integer>();

		if (tempNode.getData().isStarQuery) {
			calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getData().nodeIndex, tempNode.getData().depthOfDigging);
			maxAnyTimeAnswerDepthStarQueryMap.put(tempNode.getData().nodeIndex, 0);
		}

		while (tempNode != null) {
			if (tempNode.getRightChild() != null && tempNode.getRightChild().getData().isStarQuery) {
				calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getRightChild().getData().nodeIndex,
						tempNode.getRightChild().getData().depthOfDigging);
				maxAnyTimeAnswerDepthStarQueryMap.put(tempNode.getRightChild().getData().nodeIndex, 0);
			}
			if (tempNode.getLeftChild() != null && tempNode.getLeftChild().getData().isStarQuery) {
				calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getLeftChild().getData().nodeIndex,
						tempNode.getLeftChild().getData().depthOfDigging);
				maxAnyTimeAnswerDepthStarQueryMap.put(tempNode.getLeftChild().getData().nodeIndex, 0);
			}
			tempNode = tempNode.getLeftChild();
		}

		for (GraphResult gr : anyTimeStarFramework.anyTimeResults) {
			for (Integer starQueryIndex : gr.starQueryIndexDepthMap.keySet()) {
				if (gr.starQueryIndexDepthMap.get(starQueryIndex) > maxAnyTimeAnswerDepthStarQueryMap
						.get(starQueryIndex)) {
					maxAnyTimeAnswerDepthStarQueryMap.put(starQueryIndex,
							gr.starQueryIndexDepthMap.get(starQueryIndex));
				}
			}
		}
		int totalAnswersDepth = 0;
		int totalSFDepth = 0;
		int totalFirstPQItems = 0;
		int totalPAEstimates = 0;
		int numberOfStars = anyTimeStarFramework.starQueries.size();
		String postFix = "";
		for (Integer starQueryIndex : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			totalSFDepth += calcTreeNodeStarQueryMaxDepthMap.get(starQueryIndex);
			postFix += "(" + starQueryIndex + ":" + calcTreeNodeStarQueryMaxDepthMap.get(starQueryIndex) + "),";
		}
		postFix += ";";

		for (Integer starQueryIndex : maxAnyTimeAnswerDepthStarQueryMap.keySet()) {
			totalAnswersDepth += maxAnyTimeAnswerDepthStarQueryMap.get(starQueryIndex);
			postFix += "(" + starQueryIndex + ":" + maxAnyTimeAnswerDepthStarQueryMap.get(starQueryIndex) + "),";
		}

		for (Integer starQueryIndex : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			totalFirstPQItems += anyTimeStarFramework.calcTreeNodeMap.get(starQueryIndex).data.firstPQItemSize;
		}
		for (Integer starQueryIndex : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			totalPAEstimates += anyTimeStarFramework.calcTreeNodeMap
					.get(starQueryIndex).data.starQuery.numberOfPAEstimate;
		}

		bwQueryInfo.write(queryIndex + ";" + numberOfStars + ";" + time + ";" + totalSFDepth + ";" + totalAnswersDepth
				+ ";" + totalFirstPQItems + ";" + totalPAEstimates + ";" + postFix);
		bwQueryInfo.newLine();
		bwQueryInfo.flush();

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
