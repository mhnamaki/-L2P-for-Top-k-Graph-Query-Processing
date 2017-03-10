package wsu.eecs.mlkd.KGQuery.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import wsu.eecs.mlkd.KGQuery.TopKQuery.GraphResult;
import wsu.eecs.mlkd.KGQuery.TopKQuery.InfoHolder;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.NeighborIndexing;
import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;
import wsu.eecs.mlkd.KGQuery.TopKQuery.TreeNode;
import wsu.eecs.mlkd.KGQuery.TopKQuery.AnyTimeStarFramework;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CacheServer;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CalculationNode;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;
import wsu.eecs.mlkd.KGQuery.machineLearningQuerying.QueryInfo;

public class AnyTimeStarFrameworkExperimenter {

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
	public static int numberOfSameExperiment = 5;
	public static NeighborIndexing neighborIndexingInstance;
	public static CacheServer cacheServer;
	private static int startingQueryIndex;
	private static int endingQueryIndex;

	public static void main(String[] args) throws Exception {

		int numberOfPrefixChars = 0;
		String shouldCheckQueries = null;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-debugMode")) {
				Dummy.DummyProperties.debuggMode = Boolean.parseBoolean(args[++i]);
			}
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
				// }
				// else if (args[i].equals("-querySizeFrom")) {
				// querySizeFrom = Integer.parseInt(args[++i]);
				// } else if (args[i].equals("-querySizeTo")) {
				// querySizeTo = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-GName")) {
				GName = args[++i];

			} else if (args[i].equals("-GDirectory")) {
				GDirectory = args[++i];

			} else if (args[i].equals("-similarityThreshold")) {
				DummyProperties.similarityThreshold = Float.parseFloat(args[++i]);
			} else if (args[i].equals("-numberOfSameExperiment")) {
				numberOfSameExperiment = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-numberOfPrefixChars")) {
				numberOfPrefixChars = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-startingQueryIndex")) {
				startingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-endingQueryIndex")) {
				endingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-shouldCheckQueries")) {
				shouldCheckQueries = args[++i];
			}
		}

		if (numberOfPrefixChars >= 0) {
			DummyProperties.numberOfPrefixChars = numberOfPrefixChars;
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

		if (queryFileDirectory.equals("") || GName.equals("") || k_s.isEmpty()
				|| DummyProperties.similarityThreshold == null || shouldCheckQueries == null) {
			throw new Exception(
					"You should provide all the parameters -queryFileName  -queryFileDirectory -kFrom -kTo -querySizeFrom -querySizeTo -GName -similarityThreshold");
		}

		String totalParams = "";
		for (String arg : args) {
			totalParams += arg + ", ";
		}
		DummyFunctions.printIfItIsInDebuggedMode(totalParams);

		QueryGenerator queryGenerator = new QueryGenerator(GDirectory + GName);

		// output the results and answers
		File fout = new File(GName + queryFileName + "_answerResults_starFramework.txt");
		FileOutputStream fos = new FileOutputStream(fout);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		// output the results and anyTime answers
		File foutAnyTimeAnswers = new File(GName + queryFileName + "_anytime_answerResults_starFramework.txt");
		FileOutputStream fosAnyTimeAnswers = new FileOutputStream(foutAnyTimeAnswers);
		BufferedWriter bwAnyTimeAnswers = new BufferedWriter(new OutputStreamWriter(fosAnyTimeAnswers));

		// output the
		File foutTime = new File(GName + queryFileName + "_timeResults_starFramework.txt");
		FileOutputStream fosTime = new FileOutputStream(foutTime);

		BufferedWriter bwTime = new BufferedWriter(new OutputStreamWriter(fosTime));

		File foutTime2 = new File(GName + queryFileName + "dataScienceTimeResults.txt");
		FileOutputStream fosTime2 = new FileOutputStream(foutTime2);

		BufferedWriter bwDSTime = new BufferedWriter(new OutputStreamWriter(fosTime2));

		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(MODELGRAPH_DB_PATH);
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
		Levenshtein levenshtein = new Levenshtein(nodeLabelsIndex, Dummy.DummyProperties.numberOfPrefixChars);
		cacheServer = new CacheServer();

		HashSet<Integer> queriesShouldBeChecked = new HashSet<Integer>();
		if (shouldCheckQueries != null && !shouldCheckQueries.equals("")) {
			ArrayList<Integer> queriesInfo = shouldCheckRead(shouldCheckQueries);
			queriesShouldBeChecked = new HashSet<Integer>();
			for (Integer qi : queriesInfo) {
				queriesShouldBeChecked.add(qi);
			}

		}

		for (File file : fileInTheDirfinder(queryFileDirectory)) {
			queryFileName = file.getName();

			List<QueryFromFile> queriesFromFile = queryGenerator.getQueryFromFile(file.getAbsolutePath());

			int queryIndex = 0;
			for (QueryFromFile queryFromFile : queriesFromFile) {

				DummyFunctions.printIfItIsInDebuggedMode("start ConstrucQueryGraph");

				queryIndex = queryFromFile.queryIndex;

				// System.gc();

				if (!queriesShouldBeChecked.contains(queryIndex)) {
					System.out.println("query is skipped!" + queryIndex);
					continue;
				}

				if (queryIndex < startingQueryIndex) {
					continue;
				}
				if (queryIndex > endingQueryIndex) {
					break;
				}
				DummyFunctions.printIfItIsInDebuggedMode("start ConstrucQueryGraph");
				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH,
						queryFromFile);
				DummyFunctions.printIfItIsInDebuggedMode("end ConstrucQueryGraph");

				GraphDatabaseService queryGraph = smallGraph;
				// registerShutdownHook(queryGraph);

				start_time = System.nanoTime();
				neighborIndexingInstance.queryNeighborIndexer(queryGraph);
				end_time = System.nanoTime();
				difference = (end_time - start_time) / 1e6;
				Dummy.DummyFunctions
						.printIfItIsInDebuggedMode("queryNeighborIndexer finished in " + difference + "miliseconds!");

				boolean theQueryHasAnyAnswer = true;
				Double diffTime = 0d;
				Double finalDifferenceTime = 0d;
				ArrayList<Double> differenceTimes = new ArrayList<Double>();
				// DummyProperties.debuggMode = true;
				for (Integer k : k_s) {

					System.out.println("queryIndex: " + queryIndex + " k: " + k);

					try (Transaction tx1 = queryGraph.beginTx()) {
						try (Transaction tx2 = knowledgeGraph.beginTx()) {

							for (int exp = 0; exp < numberOfSameExperiment && theQueryHasAnyAnswer; exp++) {
								System.out.println("experiment" + exp);

								cacheServer.clear();
								// Dummy.DummyProperties.debuggMode = true;

								AnyTimeStarFramework anyTimeSF = new AnyTimeStarFramework(queryGraph, knowledgeGraph, k,
										alpha, levenshtein);

								anyTimeSF.decomposeQuery(queryGraph, knowledgeGraph, neighborIndexingInstance,
										cacheServer);

								// StarFramework sf3 =
								// starFramework2.copy(queryGraph,
								// knowledgeGraph);

								InfoHolder infoHolder = new InfoHolder();

								start_time = System.nanoTime();
								// ArrayList<GraphResult> finalResults =
								// sf3.starRun(queryGraph, knowledgeGraph);
								// ArrayList<GraphResult> finalResults =
								// starFramework2.starRun(queryGraph,
								// knowledgeGraph);

								ArrayList<GraphResult> finalResults = anyTimeSF.starRoundRobinRun(queryGraph,
										knowledgeGraph, neighborIndexingInstance, cacheServer, infoHolder);

								end_time = System.nanoTime();
								diffTime = (end_time - start_time) / 1e6;

								differenceTimes.add(diffTime);

								for (Integer calcNodeId : anyTimeSF.calcTreeNodeMap.keySet()) {
									if (!anyTimeSF.calcTreeNodeMap.get(calcNodeId).data.isStarQuery) {
										for (GraphResult gr : anyTimeSF.calcTreeNodeMap
												.get(calcNodeId).data.graphResult) {
											System.out.println(calcNodeId + ";" + gr.joinableKnowledgeNodeIds);
										}
									}
									System.out.println();
								}
								System.out.println("starRun finished in " + diffTime + " miliseconds!");
								int anyTimeResultsSizeTemp = anyTimeSF.anyTimeResults.size();
								// printResults(queryIndex, 0,
								// finalResults.size(), k, diffTime, exp, bw,
								// bwTime,
								// finalResults);
								finalizeMethod(anyTimeSF);
								// writeAnyTimeResult(bwAnyTimeAnswers,
								// anyTimeSF.anyTimeResults);

								bwTime.newLine();
								bwTime.newLine();
								bw.newLine();
								bw.newLine();
								System.err.println("new line");
								System.err.println();
								// start_time = System.nanoTime();
								// // ArrayList<GraphResult> finalResults2 =
								// // starFramework2.starRun(queryGraph,
								// // knowledgeGraph);
								// ArrayList<GraphResult> finalResults2 =
								// sf3.starRun(queryGraph,
								// knowledgeGraph);
								// end_time = System.nanoTime();
								// difference = (end_time - start_time) / 1e6;
								// System.out.println("StarFramework finished in
								// " + difference + "miliseconds!");
								// printResults(queryIndex, querySize,
								// finalResults2.size(), k, difference, exp, bw,
								// bwTime, finalResults2);
								// finalizeMethod(sf3);

								if (anyTimeResultsSizeTemp == 0) {
									System.err.println("No Answer Found for query index! " + queryIndex);
									theQueryHasAnyAnswer = false;
									break;
								} else {

									// finalizeMethod(toBeRun);
									// DummyFunctions.printIfItIsInDebuggedMode("before
									// tx2 success");
								}
								tx2.success();
								// DummyFunctions.printIfItIsInDebuggedMode("before
								// tx2 success");
							}
							finalDifferenceTime = Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimes,
									numberOfSameExperiment);
							System.out.println(
									"StarFramework avg is finished in " + finalDifferenceTime + " miliseconds!");

							bwDSTime.write(queryIndex + ";" + finalDifferenceTime + "\n");
							bwDSTime.flush();

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
				}

				queryGraph.shutdown();
			}

			// }

		}
		bw.close();
		bwTime.close();
		bwAnyTimeAnswers.close();
		bwDSTime.close();
		knowledgeGraph.shutdown();
	}

	private static ArrayList<Integer> shouldCheckRead(String shouldCheckQueries) throws Exception {
		ArrayList<Integer> shouldCheckQueriesList = new ArrayList<Integer>();

		FileInputStream fis = new FileInputStream(shouldCheckQueries);

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		while ((line = br.readLine()) != null) {
			String[] queryInfos = line.split(";");
			shouldCheckQueriesList.add(Integer.parseInt(queryInfos[0]));
		}

		br.close();

		return shouldCheckQueriesList;
	}

	private static void writeAnyTimeResult(BufferedWriter bwAnyTimeAnswers, PriorityQueue<GraphResult> anyTimeResults)
			throws Exception {

		while (anyTimeResults.size() > 0) {
			GraphResult gr = anyTimeResults.poll();
			String result = "";
			for (Node qNode : gr.assembledResult.keySet()) {
				result += qNode.getId() + ":" + qNode.getLabels().iterator().next() + " => "
						+ gr.assembledResult.get(qNode).node.getId() + " - ";

				result += " {";
				for (Label albl : gr.assembledResult.get(qNode).node.getLabels()) {
					result += albl + ",";
				}
				result += "} ";

				result += " [";
				HashMap<String, Object> propMap = (HashMap<String, Object>) gr.assembledResult.get(qNode).node
						.getAllProperties();
				for (String key : propMap.keySet()) {
					if (key.toLowerCase().contains("__uri__"))
						result += "(" + propMap.get(key) + ")";

				}

				result += "] \n";

			}
			result += " -> anyTimeItemValue:" + gr.anyTimeItemValue + " | tValue:" + gr.getTotalValue();

			bwAnyTimeAnswers.write(result);
			bwAnyTimeAnswers.newLine();
		}
		bwAnyTimeAnswers.newLine();
		bwAnyTimeAnswers.flush();

	}

	public static void printResults(int queryIndex, int querySize, int finalResultSizeTemp, int k, double difference,
			int experiement, BufferedWriter bw, BufferedWriter bwTime, ArrayList<GraphResult> finalResults)
			throws Exception {
		for (int i = 0; i < finalResults.size(); i++) {
			GraphResult gr = finalResults.get(i);
			String result = "";
			for (Node qNode : gr.assembledResult.keySet()) {
				result += gr.assembledResult.get(qNode).node.getId() + "-";
			}
			result += " -> " + gr.getTotalValue();
			// System.out.println(result);
			bw.write(result);
			bw.newLine();
		}
		// System.out.println();
		// System.out.println();
		// |Q|, GName, k, actualResultsReterned,
		// Alg,
		// time
		String timeLineResult = "";
		timeLineResult += queryIndex + ";";
		timeLineResult += querySize + ";";
		timeLineResult += GName + ";";
		timeLineResult += finalResultSizeTemp + ";";
		timeLineResult += k + ";";
		timeLineResult += "StarFramework;";
		timeLineResult += difference + ";";

		// for (GraphResult result : finalResults) {
		// bw.write(result);
		// bw.newLine();
		// }
		bw.newLine();
		bw.flush();
		if (experiement != 0) {
			bwTime.write(timeLineResult);
			bwTime.newLine();
			bwTime.flush();
		}
	}

	public static void finalizeMethod(AnyTimeStarFramework starFramework2) {
		TreeNode<CalculationNode> tempNode = starFramework2.rootTreeNode;
		HashMap<Integer, Integer> calcTreeNodeStarQueryMaxDepthMap = new HashMap<Integer, Integer>();
		// HashMap<Integer, Integer> maxAnswerDepthStarQueryMap = new
		// HashMap<Integer, Integer>();
		HashMap<Integer, Integer> maxAnyTimeAnswerDepthStarQueryMap = new HashMap<Integer, Integer>();

		if (tempNode.getData().isStarQuery) {
			calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getData().nodeIndex, tempNode.getData().depthOfDigging);
			System.out.println("In SF: depthOfDigging for starQuery with nodeIndex: " + tempNode.getData().nodeIndex
					+ " is " + tempNode.getData().depthOfDigging);
			// maxAnswerDepthStarQueryMap.put(tempNode.getData().nodeIndex, 0);
			maxAnyTimeAnswerDepthStarQueryMap.put(tempNode.getData().nodeIndex, 0);
		}

		while (tempNode != null) {
			if (tempNode.getRightChild() != null && tempNode.getRightChild().getData().isStarQuery) {
				calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getRightChild().getData().nodeIndex,
						tempNode.getRightChild().getData().depthOfDigging);
				// maxAnswerDepthStarQueryMap.put(tempNode.getRightChild().getData().nodeIndex,
				// 0);
				maxAnyTimeAnswerDepthStarQueryMap.put(tempNode.getRightChild().getData().nodeIndex, 0);

				System.out.println("In SF: depthOfDigging for starQuery with nodeIndex: "
						+ tempNode.getRightChild().getData().nodeIndex + " is "
						+ tempNode.getRightChild().getData().depthOfDigging);
			}
			if (tempNode.getLeftChild() != null && tempNode.getLeftChild().getData().isStarQuery) {
				calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getLeftChild().getData().nodeIndex,
						tempNode.getLeftChild().getData().depthOfDigging);
				// maxAnswerDepthStarQueryMap.put(tempNode.getLeftChild().getData().nodeIndex,
				// 0);
				maxAnyTimeAnswerDepthStarQueryMap.put(tempNode.getLeftChild().getData().nodeIndex, 0);

				System.out.println("In SF: depthOfDigging for starQuery with nodeIndex: "
						+ tempNode.getLeftChild().getData().nodeIndex + " is "
						+ tempNode.getLeftChild().getData().depthOfDigging);
			}
			tempNode = tempNode.getLeftChild();
		}

		for (GraphResult gr : starFramework2.anyTimeResults) {
			for (Integer starQueryIndex : gr.starQueryIndexDepthMap.keySet()) {
				if (gr.starQueryIndexDepthMap.get(starQueryIndex) > maxAnyTimeAnswerDepthStarQueryMap
						.get(starQueryIndex)) {
					maxAnyTimeAnswerDepthStarQueryMap.put(starQueryIndex,
							gr.starQueryIndexDepthMap.get(starQueryIndex));
				}
			}
		}

		//
		for (Integer starQueryIndex : maxAnyTimeAnswerDepthStarQueryMap.keySet()) {
			System.out.println("In AnyTime Answers: depthOfDigging for starQuery with nodeIndex: " + starQueryIndex
					+ " is " + maxAnyTimeAnswerDepthStarQueryMap.get(starQueryIndex));
		}

		String prefix = "";
		for (Integer starQueryIndex : maxAnyTimeAnswerDepthStarQueryMap.keySet()) {
			prefix += "(" + starQueryIndex + ":" + maxAnyTimeAnswerDepthStarQueryMap.get(starQueryIndex) + ")";
		}
		prefix += ";  ";

		for (Integer starQueryIndex : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			prefix += "(" + starQueryIndex + ":" + calcTreeNodeStarQueryMaxDepthMap.get(starQueryIndex) + ")";
		}
		prefix += ";  ";

		for (Integer starQueryIndex : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			prefix += "(" + starQueryIndex + ":"
					+ starFramework2.calcTreeStarQueriesNodeMap.get(starQueryIndex).getData().firstPQItemSize + ")";
		}
		prefix += ";  ";

		prefix += "\n";

		prefix += "partialAnswersEstimate: ";
		for (Integer starQueryIndex : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			prefix += "(" + starQueryIndex + " : " + starFramework2.calcTreeStarQueriesNodeMap.get(starQueryIndex)
					.getData().starQuery.numberOfPAEstimate + ") ";
		}
		prefix += "\n";

		prefix += "number of nodes in SQ: ";
		for (Integer starQueryIndex : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			prefix += "(" + starQueryIndex + " : " + starFramework2.calcTreeStarQueriesNodeMap.get(starQueryIndex)
					.getData().starQuery.allStarGraphQueryNodes.size() + ") ";
		}

		prefix += "\n";

		System.out.println(prefix);

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
