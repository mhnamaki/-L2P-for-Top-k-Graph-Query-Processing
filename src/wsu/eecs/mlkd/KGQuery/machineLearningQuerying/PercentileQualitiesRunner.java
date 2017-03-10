package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.tooling.GlobalGraphOperations;

import biz.k11i.xgboost.Predictor;
import wsu.eecs.mlkd.KGQuery.TopKQuery.AnyTimeStarFramework;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CacheServer;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CalculationNode;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CalculationTreeSiblingNodes;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy;
import wsu.eecs.mlkd.KGQuery.TopKQuery.GraphResult;
import wsu.eecs.mlkd.KGQuery.TopKQuery.InfoHolder;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.NeighborIndexing;
import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;
import wsu.eecs.mlkd.KGQuery.TopKQuery.TreeNode;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;
import wsu.eecs.mlkd.KGQuery.test.QueryFromFile;
import wsu.eecs.mlkd.KGQuery.test.QueryGenerator;

public class PercentileQualitiesRunner {
	private static String MODELGRAPH_DB_PATH = "";
	private static String PATTERNGRAPH_DB_PATH = "";

	public static String queryFileName = "";
	public static String queryFileDirectory = "";
	public static int kFrom = 0;
	public static int kTo = 0;
	public static int querySizeFrom = 0;
	public static int querySizeTo = 0;
	public static String GName = ""; // Yago, DBPedia, ...

	public static String queryDBInNeo4j = "query";
	public static String GDirectory = "";
	public int numberOfSameExperiment = 1;
	public AnyTimeStarFramework sf;
	public AnyTimeStarFramework oracleSF;
	private AnyTimeStarFramework mlSF;
	public static int teta = 0;
	public static File foutTime;
	public int k = 0;
	public GraphDatabaseService queryGraph;
	public GraphDatabaseService knowledgeGraph;
	public float alpha = 0.5F;
	public Levenshtein levenshtein;
	public HashMap<Integer, TreeNode<CalculationNode>> calcTreeNodeMap;
	public HashMap<Integer, CalculationTreeSiblingNodes> joinLevelSiblingNodesMap;
	public NeighborIndexing neighborIndexingInstance;
	public CacheServer cacheServer;
	public int startingQueryIndex = 0;
	public int endingQueryIndex = 0;
	public String queryInfoAddress;
	int totalAnswersDepth = 0;
	int totalSFDepth = 0;
	private String queriesFoldPath;
	private int foldStartFrom = -1;
	private int foldEndTo = -1;

	public String oracleSequenceFile;

	final int SELECTION_FEATURES_SIZE = 108;
	final int EXPANSION_FEATURES_SIZE = 49;

	float[] selectionNormalizationFeaturesVector;
	float[] expansionNormalizationFeaturesVector;

	public CommonFunctions commonFunctions = new CommonFunctions();

	public enum WhichOracle {
		microSingleAction, macroSingleAction, macroMultiAction
	};

	public enum MinObjectiveFunction {
		DiffTime, TotalTime, JustDepth
	};

	public void initialize(String[] args) throws Exception {
		int numberOfPrefixChars = 0;
		// int delta = 0;
		// int beamSize = 0;
		WhichOracle oracle = WhichOracle.macroSingleAction;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-queryFileName")) {
				queryFileName = args[++i];
				queryFileName = queryFileName.replace(".txt", "");
			} else if (args[i].equals("-queryFileDirectory")) {
				queryFileDirectory = args[++i];
				// if (!queryFileDirectory.endsWith("/") &&
				// !queryFileDirectory.equals("")) {
				// queryFileDirectory += "/";
				// }
			} else if (args[i].equals("-GName")) {
				GName = args[++i];
			} else if (args[i].equals("-GDirectory")) {
				GDirectory = args[++i];
			} else if (args[i].equals("-similarityThreshold")) {
				DummyProperties.similarityThreshold = Float.parseFloat(args[++i]);
			} else if (args[i].equals("-k")) {
				k = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-numberOfPrefixChars")) {
				numberOfPrefixChars = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-startingQueryIndex")) {
				startingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-endingQueryIndex")) {
				endingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-foldStartFrom")) {
				foldStartFrom = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-foldEndTo")) {
				foldEndTo = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-queriesFoldPath")) {
				queriesFoldPath = args[++i];
			} else if (args[i].equals("-oracleSequenceFile")) {
				oracleSequenceFile = args[++i];
			}

		}

		System.out.println("foldStartFrom: " + foldStartFrom);
		System.out.println("foldEndTo: " + foldEndTo);
		System.out.println(" startingQueryIndex: " + startingQueryIndex);
		System.out.println("endingQueryIndex: " + endingQueryIndex);
		System.out.println("oracleSequenceFile : " + oracleSequenceFile);

		cacheServer = new CacheServer();

		if (numberOfPrefixChars > 0) {
			DummyProperties.numberOfPrefixChars = numberOfPrefixChars;
		}
		// if (!GDirectory.endsWith("/")) {
		// GDirectory += "/";
		// }

		MODELGRAPH_DB_PATH = GDirectory + GName;
		PATTERNGRAPH_DB_PATH = queryFileDirectory + queryDBInNeo4j + "_" + startingQueryIndex + ".db";

		// if (queryFileName.equals("") || GName.equals("") || k_s.isEmpty() ||
		// querySizeFrom == 0 || querySizeTo == 0
		// || DummyProperties.similarityThreshold == null) {
		// throw new Exception(
		// "You should provide all the parameters -queryFileName
		// -queryFileDirectory -kFrom -kTo -querySizeFrom -querySizeTo -GName
		// -similarityThreshold");
		// }

		String totalParams = "";
		for (String arg : args) {
			totalParams += arg + ", ";
		}
		DummyFunctions.printIfItIsInDebuggedMode(totalParams);

		QueryGenerator queryGenerator = new QueryGenerator(GDirectory + GName);

		System.out.println("GDirectory + GName: " + GDirectory + GName);
		System.out.println("numberOfPrefixChars: " + numberOfPrefixChars);

		CommonFunctions cfunction = new CommonFunctions();

		HashSet<Integer> queriesShouldBeChecked = new HashSet<Integer>();
		if (queryInfoAddress != null && !queryInfoAddress.equals("")) {
			ArrayList<QueryInfo> queriesInfo = QueryInfo.queryInfoRead(queryInfoAddress);
			queriesShouldBeChecked = new HashSet<Integer>();
			for (QueryInfo qi : queriesInfo) {
				queriesShouldBeChecked.add(qi.queryId);
			}

		} else if (foldEndTo != -1) {
			queriesShouldBeChecked = cfunction.readQueryIndexBasedOnFolds(queriesFoldPath, foldStartFrom, foldEndTo);
		}

		knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(MODELGRAPH_DB_PATH)
				.setConfig(GraphDatabaseSettings.pagecache_memory, "6g").newGraphDatabase();
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

		levenshtein = new Levenshtein(nodeLabelsIndex, Dummy.DummyProperties.numberOfPrefixChars);

		System.out.println("queriesShouldBeChecked.size(): " + queriesShouldBeChecked.size());

		File[] percentileSeqFout = new File[10];
		FileOutputStream[] percentileSeqFos = new FileOutputStream[10];
		BufferedWriter[] bwPercentileSeqFos = new BufferedWriter[10];

		for (int i = 0; i < 100; i += 10) {
			percentileSeqFout[i / 10] = new File(GName + "_Sequence_For_" + (i + 10) + ".txt");
			percentileSeqFos[i / 10] = new FileOutputStream(percentileSeqFout[i / 10]);
			bwPercentileSeqFos[i / 10] = new BufferedWriter(new OutputStreamWriter(percentileSeqFos[i / 10]));
		}

		boolean isFirstQuery = true;
		for (File file : fileInTheDirfinder(queryFileDirectory)) {
			queryFileName = file.getName();

			List<QueryFromFile> queriesFromFile = queryGenerator.getQueryFromFile(file.getAbsolutePath());

			int queryIndex = 0;
			for (QueryFromFile queryFromFile : queriesFromFile) {

				DummyFunctions.printIfItIsInDebuggedMode("start ConstrucQueryGraph");

				queryIndex = queryFromFile.queryIndex;
				if (queryIndex < startingQueryIndex || queryIndex > endingQueryIndex) {
					continue;
				}

				if (queriesShouldBeChecked.size() > 0 && !queriesShouldBeChecked.contains(queryIndex)) {
					// System.out.println("query " + queryIndex + " skipped!");
					continue;
				}

				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH,
						queryFromFile);
				DummyFunctions.printIfItIsInDebuggedMode("end ConstrucQueryGraph");

				queryGraph = smallGraph;
				// registerShutdownHook(queryGraph);

				start_time = System.nanoTime();
				neighborIndexingInstance.queryNeighborIndexer(queryGraph);
				end_time = System.nanoTime();
				difference = (end_time - start_time) / 1e6;
				Dummy.DummyFunctions
						.printIfItIsInDebuggedMode("queryNeighborIndexer finished in " + difference + "miliseconds!");
				// boolean theQueryHasAnyAnswer = true;

				System.out.println("queryfileName: " + queryFileName + ", queryIndex: " + queryIndex + " k: " + k);

				try (Transaction tx1 = queryGraph.beginTx()) {
					try (Transaction tx2 = knowledgeGraph.beginTx()) {
						int numberOfQNodes = neighborIndexingInstance.queryNodeIdSet.size();
						int numberOfQRelationships = 0;
						for (Relationship rel : GlobalGraphOperations.at(queryGraph).getAllRelationships()) {
							numberOfQRelationships++;
						}
						boolean dontRunThisQuery = false;
						for (Long qNodeId : neighborIndexingInstance.queryNodeLabelMap.keySet()) {
							if (neighborIndexingInstance.queryNodeLabelMap.get(qNodeId)
									.length() < DummyProperties.numberOfPrefixChars) {
								dontRunThisQuery = true;
							}
						}
						if (dontRunThisQuery) {
							tx2.success();
							tx2.close();
							queryGraph.shutdown();
							queryGraph = null;
							System.out.println();
							continue;
						}
						Double diffTime = 0d;
						Double finalSFDifferenceTime = 0d;
						ArrayList<Double> differenceTimes = new ArrayList<Double>();

						InfoHolder[] infoHolders = new InfoHolder[numberOfSameExperiment];
						for (int i = 0; i < infoHolders.length; i++) {
							infoHolders[i] = new InfoHolder();
						}

						// ArrayList<GraphResult> finalResults = null;
						for (int exp = 0; exp < numberOfSameExperiment; exp++) {
							sf = getNewStarFrameworkInstance(sf);
							infoHolders[exp].queryIndex = queryIndex;
							start_time = System.nanoTime();
							sf.starRoundRobinRun(queryGraph, knowledgeGraph, neighborIndexingInstance, cacheServer,
									infoHolders[exp]);

							end_time = System.nanoTime();
							diffTime = (end_time - start_time) / 1e6;
							System.out.println("StarFramework exp: " + exp + " is finished in " + diffTime.intValue()
									+ " miliseconds!");

							differenceTimes.add(diffTime);

							if (exp != (numberOfSameExperiment - 1)) {
								sf = null;
							}
						}

						System.gc();
						System.runFinalization();

						int anyTimeResultsSizeTemp = sf.anyTimeResults.size();

						double trueRank = DummyFunctions.getRank(sf.anyTimeResults);
						System.out.println("queryIndex: " + queryIndex + " trueRank:" + trueRank);

						if (anyTimeResultsSizeTemp == 0) {
							System.err.println("No Answer Found for query index! " + queryIndex);
							tx2.success();
							tx2.close();
							queryGraph.shutdown();
							queryGraph = null;
							System.out.println();
							continue;
						} else {
							totalAnswersDepth = 0;
							totalSFDepth = 0;

							differenceTimes.clear();

							for (int exp = 0; exp < numberOfSameExperiment; exp++) {
								oracleSF = getNewStarFrameworkInstance(oracleSF);

								int numberOfStars = oracleSF.starQueries.size();

								// reading the oracle sequences
								ArrayList<HashMap<Integer, Integer>> fetchSeqByStarQueryIndex = readOracleSequence(
										queryIndex, oracleSequenceFile);

								ArrayList<BufferedWriter> foutSeqBWs = new ArrayList<BufferedWriter>();
								for (int i = 0; i < bwPercentileSeqFos.length; i++) {
									foutSeqBWs.add(bwPercentileSeqFos[i]);
								}

								for (BufferedWriter bwSeqFile : foutSeqBWs) {
									bwSeqFile.write(queryIndex + ";");
									bwSeqFile.flush();
								}

								if (fetchSeqByStarQueryIndex.size() < numberOfStars) {
									tx2.success();
									tx2.close();
									queryGraph.shutdown();
									queryGraph = null;
									System.err.println(fetchSeqByStarQueryIndex.size() + " < " + numberOfStars);
									continue;
								}

								start_time = System.nanoTime();

								double sFetchTime, sJoinTime;
								for (int level = 0; level < fetchSeqByStarQueryIndex.size(); level++) {
									sFetchTime = System.nanoTime();
									HashMap<Integer, Integer> fetchByQueryIndex = fetchSeqByStarQueryIndex.get(level);
									int depthJoinLevel = 0;
									int selection = 0;
									int expansion = 0;
									for (Integer queryIndexToBeFetched : fetchByQueryIndex.keySet()) {

										TreeNode<CalculationNode> thisCalcNode = oracleSF.calcTreeStarQueriesNodeMapBySQMLIndex
												.get(queryIndexToBeFetched);
										selection = queryIndexToBeFetched;
										expansion = fetchByQueryIndex.get(queryIndexToBeFetched);
										thisCalcNode.getData().numberOfPartialAnswersShouldBeFetched = expansion;
										infoHolders[exp].oracleFetchesCalls++;
										infoHolders[exp].oracleTotalRequestForFetches += expansion;
										oracleSF.anyTimeStarkForLeaf(knowledgeGraph, thisCalcNode,
												neighborIndexingInstance, cacheServer);
										// System.out.println(
										// "f:" + starkOperation + " :" +
										// (System.nanoTime() - stime) / 1e6);
										depthJoinLevel = thisCalcNode.levelInCalcTree - 1;

									}
									infoHolders[exp].oracleFetchesTime += ((System.nanoTime() - sFetchTime) / 1e6);

									sJoinTime = System.nanoTime();
									for (; depthJoinLevel >= 0; depthJoinLevel--) {
										infoHolders[exp].oracleJoinCalls++;
										CalculationTreeSiblingNodes calculationTreeSiblingNodes = oracleSF.joinLevelSiblingNodesMap
												.get(depthJoinLevel);
										oracleSF.anyTimeTwoWayHashJoin(calculationTreeSiblingNodes.leftNode,
												calculationTreeSiblingNodes.rightNode, oracleSF.k);
									}
									infoHolders[exp].oracleJoinTime += ((System.nanoTime() - sJoinTime) / 1e6);
									// System.out.println("j:" + joinOperation +
									// " :" + (System.nanoTime() - stime) /
									// 1e6);
									depthJoinLevel = 0;

									if (level > numberOfStars) {
										for (int q = 0; q < 100; q += 10) {
											double currentRank = Dummy.DummyFunctions.getRank(oracleSF.anyTimeResults);
											if (((currentRank / trueRank) * 100) > (q + 10)) {

												System.out.println("queryIndex:" + queryIndex + " > " + (q / 10)
														+ " is removed from fileWriters. currentRank is:" + currentRank
														+ ", trueRank is:" + trueRank + " fileWriter:"
														+ percentileSeqFout[(q / 10)].getName());

												foutSeqBWs.remove(bwPercentileSeqFos[q / 10]);
											}
										}
									}

									for (BufferedWriter bwSeqFile : foutSeqBWs) {
										bwSeqFile.write("< " + selection + ": " + expansion + " >, ");
										bwSeqFile.flush();
									}

								}

								end_time = System.nanoTime();

								diffTime = (end_time - start_time) / 1e6;
								System.out.println("oracle sequence exp: " + exp + " is finished in "
										+ diffTime.intValue() + " miliseconds!");
								differenceTimes.add(diffTime);

								if (exp != (numberOfSameExperiment - 1)) {
									oracleSF = null;
								}

								for (BufferedWriter bwSeqFile : bwPercentileSeqFos) {
									bwSeqFile.newLine();
								}
							}

							oracleSF = null;
							sf = null;
							mlSF = null;

							System.gc();
							System.runFinalization();

							isFirstQuery = false;

						}
						System.out.println();
						tx2.success();
						tx2.close();

					} catch (Exception exc) {
						System.out.println("queryGraph Transaction failed");
						exc.printStackTrace();
					}

					tx1.success();
					tx1.close();

				} catch (Exception exc) {
					System.out.println("modelGraph Transaction failed");
					exc.printStackTrace();
				}
				// }
				queryGraph.shutdown();
				queryGraph = null;
				System.gc();
				System.runFinalization();
			}

		}

		knowledgeGraph.shutdown();

		System.out.println("program is finished properly!");

	}

	public static void main(String[] args) throws Exception {
		PercentileQualitiesRunner beamSearchRunner = new PercentileQualitiesRunner();
		beamSearchRunner.initialize(args);
	}

	private AnyTimeStarFramework getNewStarFrameworkInstance(AnyTimeStarFramework anySF,
			GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph, int k2, float alpha,
			Levenshtein levenshtein) {
		cacheServer.clear();
		anySF = new AnyTimeStarFramework(queryGraph, knowledgeGraph, k, alpha, levenshtein);
		anySF.decomposeQuery(queryGraph, knowledgeGraph, neighborIndexingInstance, cacheServer);

		TreeNode<CalculationNode> tempNode = anySF.rootTreeNode;
		calcTreeNodeMap = new HashMap<Integer, TreeNode<CalculationNode>>();
		joinLevelSiblingNodesMap = new HashMap<Integer, CalculationTreeSiblingNodes>();

		calcTreeNodeMap.put(tempNode.getData().nodeIndex, tempNode);
		while (tempNode != null) {

			if (tempNode.getRightChild() != null)
				calcTreeNodeMap.put(tempNode.getRightChild().getData().nodeIndex, tempNode.getRightChild());

			if (tempNode.getLeftChild() != null)
				calcTreeNodeMap.put(tempNode.getLeftChild().getData().nodeIndex, tempNode.getLeftChild());

			if (tempNode.getLeftChild() != null && tempNode.getRightChild() != null) {
				joinLevelSiblingNodesMap.put(tempNode.levelInCalcTree,
						new CalculationTreeSiblingNodes(tempNode.getLeftChild(), tempNode.getRightChild()));
			}

			tempNode = tempNode.getLeftChild();
		}

		return anySF;
	}

	public AnyTimeStarFramework getNewStarFrameworkInstance(AnyTimeStarFramework anySF) {
		return getNewStarFrameworkInstance(anySF, queryGraph, knowledgeGraph, k, alpha, levenshtein);
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	// public AnyTimeStarFramework copy() {
	// return this.starFramework2.copy(queryGraph, knowledgeGraph);
	//
	// }

	public void finalizeMethod(AnyTimeStarFramework starFramework2, BufferedWriter bwTime, String prefix,
			HashMap<Integer, Integer> maxAnyTimeAnswerDepthStarQueryMap) throws Exception {

		TreeNode<CalculationNode> tempNode = starFramework2.rootTreeNode;
		HashMap<Integer, Integer> calcTreeNodeStarQueryMaxDepthMap = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> maxAnswerDepthStarQueryMap = new HashMap<Integer, Integer>();

		if (tempNode.getData().isStarQuery) {
			calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getData().nodeIndex, tempNode.getData().depthOfDigging);
			System.out.println("In SF: depthOfDigging for starQuery with nodeIndex: " + tempNode.getData().nodeIndex
					+ " is " + tempNode.getData().depthOfDigging);
			maxAnswerDepthStarQueryMap.put(tempNode.getData().nodeIndex, 0);
			maxAnyTimeAnswerDepthStarQueryMap.put(tempNode.getData().nodeIndex, 0);
		}

		while (tempNode != null) {
			if (tempNode.getRightChild() != null && tempNode.getRightChild().getData().isStarQuery) {
				calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getRightChild().getData().nodeIndex,
						tempNode.getRightChild().getData().depthOfDigging);
				maxAnswerDepthStarQueryMap.put(tempNode.getRightChild().getData().nodeIndex, 0);
				maxAnyTimeAnswerDepthStarQueryMap.put(tempNode.getRightChild().getData().nodeIndex, 0);

				System.out.println("In SF: depthOfDigging for starQuery with nodeIndex: "
						+ tempNode.getRightChild().getData().nodeIndex + " is "
						+ tempNode.getRightChild().getData().depthOfDigging);
			}
			if (tempNode.getLeftChild() != null && tempNode.getLeftChild().getData().isStarQuery) {
				calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getLeftChild().getData().nodeIndex,
						tempNode.getLeftChild().getData().depthOfDigging);
				maxAnswerDepthStarQueryMap.put(tempNode.getLeftChild().getData().nodeIndex, 0);
				maxAnyTimeAnswerDepthStarQueryMap.put(tempNode.getLeftChild().getData().nodeIndex, 0);

				System.out.println("In SF: depthOfDigging for starQuery with nodeIndex: "
						+ tempNode.getLeftChild().getData().nodeIndex + " is "
						+ tempNode.getLeftChild().getData().depthOfDigging);
			}
			tempNode = tempNode.getLeftChild();
		}

		// for (int index = 0; index < starFramework2.k; index++) {
		// GraphResult gr = starFramework2.finalResultsArrayList.get(index);
		// for (Integer starQueryIndex : gr.starQueryIndexDepthMap.keySet()) {
		// if (gr.starQueryIndexDepthMap.get(starQueryIndex) >
		// maxAnswerDepthStarQueryMap.get(starQueryIndex)) {
		// maxAnswerDepthStarQueryMap.put(starQueryIndex,
		// gr.starQueryIndexDepthMap.get(starQueryIndex));
		// }
		// }
		// }

		//

		for (GraphResult gr : starFramework2.anyTimeResults) {
			for (Integer starQueryIndex : gr.starQueryIndexDepthMap.keySet()) {
				if (gr.starQueryIndexDepthMap.get(starQueryIndex) > maxAnyTimeAnswerDepthStarQueryMap
						.get(starQueryIndex)) {
					maxAnyTimeAnswerDepthStarQueryMap.put(starQueryIndex,
							gr.starQueryIndexDepthMap.get(starQueryIndex));
				}
			}
		}

		for (Integer starQueryIndex : maxAnyTimeAnswerDepthStarQueryMap.keySet()) {
			prefix += "(" + starQueryIndex + ":" + maxAnyTimeAnswerDepthStarQueryMap.get(starQueryIndex) + ")";
		}
		prefix += ";";

		for (Integer starQueryIndex : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			prefix += "(" + starQueryIndex + ":" + calcTreeNodeStarQueryMaxDepthMap.get(starQueryIndex) + ")";
		}
		prefix += ";";

		for (Integer starQueryIndex : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			prefix += "(" + starQueryIndex + ":"
					+ starFramework2.calcTreeStarQueriesNodeMap.get(starQueryIndex).getData().firstPQItemSize + ")";
		}
		prefix += ";";

		for (Integer starQueryIndex : maxAnyTimeAnswerDepthStarQueryMap.keySet()) {
			System.out.println("In Anytime Answers: depthOfDigging for starQuery with nodeIndex: " + starQueryIndex
					+ " is " + maxAnyTimeAnswerDepthStarQueryMap.get(starQueryIndex));
		}

		for (Integer starQueryIndex : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			totalSFDepth += calcTreeNodeStarQueryMaxDepthMap.get(starQueryIndex);
		}
		prefix += totalSFDepth + ";";

		for (Integer starQueryIndex : maxAnyTimeAnswerDepthStarQueryMap.keySet()) {
			totalAnswersDepth += maxAnyTimeAnswerDepthStarQueryMap.get(starQueryIndex);
		}
		prefix += totalAnswersDepth + ";";

		if (bwTime != null) {
			bwTime.write(prefix);
			bwTime.newLine();
			bwTime.flush();
		}

	}

	public void printBeamResult(String GName, int queryIndex, String queryFileName, int numberOfNodes,
			int numberOfRelationships, int beamSize, int delta, int k, double difference, BufferedWriter bwTime,
			BeamSearchNode beamSearchNode, double oracleTime, String rankingFunction, InfoHolder[] infoHolders)
			throws Exception {

		String prefix = "BeamSearch" + ";" + GName + ";" + queryIndex + ";" + queryFileName + ";" + numberOfNodes + ";"
				+ numberOfRelationships + ";" + beamSize + ";" + delta + ";" + 0 + ";" + k + ";" + difference + ";" + 0
				+ ";" + oracleTime + ";" + rankingFunction + ";";

		// int totalOracleFetched = 0;
		// for (Integer index :
		// beamSearchNode.resultPartialAnswersShouldBeByNodeIndex.keySet()) {
		// prefix += " (" + index + ": " +
		// beamSearchNode.resultPartialAnswersShouldBeByNodeIndex.get(index) +
		// ") ";
		// totalOracleFetched +=
		// beamSearchNode.resultPartialAnswersShouldBeByNodeIndex.get(index);
		// }

		prefix += ";" + totalSFDepth + ";" + totalAnswersDepth + ";" + infoHolders[0].oracleTotalRequestForFetches;

		ArrayList<Double> avgFetchTimeArr = new ArrayList<>();
		ArrayList<Double> avgJoinTimeArr = new ArrayList<>();
		ArrayList<Double> avgFinishCheckingTimeArr = new ArrayList<>();
		for (int l = 0; l < infoHolders.length; l++) {
			avgFetchTimeArr.add(infoHolders[l].sfFetchesTime);
			avgJoinTimeArr.add(infoHolders[l].sfJoinTime);
			avgFinishCheckingTimeArr.add(infoHolders[l].sfCheckShouldFinishTime);
		}
		double avgSFFetchTime = DummyFunctions.computeNonOutlierAverage(avgFetchTimeArr, numberOfSameExperiment);
		double avgSFJoinTime = DummyFunctions.computeNonOutlierAverage(avgJoinTimeArr, numberOfSameExperiment);
		double avgSFFinishCheckingTime = DummyFunctions.computeNonOutlierAverage(avgFinishCheckingTimeArr,
				numberOfSameExperiment);

		// avgFetchTimeArr.clear();
		// avgJoinTimeArr.clear();
		// avgFinishCheckingTimeArr.clear();
		// for (int l = 0; l < infoHolders.length; l++) {
		// avgFetchTimeArr.add(infoHolders[l].oracleFetchesTime);
		// avgJoinTimeArr.add(infoHolders[l].oracleJoinTime);
		// avgFinishCheckingTimeArr.add(infoHolders[l].oracleCheckShouldFinishTime);
		// }
		// double avgOracleFetchTime =
		// DummyFunctions.computeNonOutlierAverage(avgFetchTimeArr,
		// numberOfSameExperiment);
		// double avgOracleJoinTime =
		// DummyFunctions.computeNonOutlierAverage(avgJoinTimeArr,
		// numberOfSameExperiment);
		// double avgOracleFinishCheckingTime =
		// DummyFunctions.computeNonOutlierAverage(avgFinishCheckingTimeArr,
		// numberOfSameExperiment);

		prefix += ";" + avgSFFetchTime + ";" + avgSFJoinTime + ";" + avgSFFinishCheckingTime + ";"
				+ infoHolders[0].oracleFetchesTime + ";" + infoHolders[0].oracleJoinTime + ";"
				+ infoHolders[0].oracleCheckShouldFinishTime;

		prefix += ";" + infoHolders[0].sfFetchesCalls + ";" + infoHolders[0].sfJoinCalls + ";"
				+ infoHolders[0].sfTotalRequestForFetches;

		prefix += ";" + infoHolders[0].oracleFetchesCalls + ";" + infoHolders[0].oracleJoinCalls + ";"
				+ infoHolders[0].oracleTotalRequestForFetches;

		// prefix += ";" + beamSearchNode.sequenceForDebug + ";";

		bwTime.write(prefix);
		bwTime.newLine();
		bwTime.flush();
	}

	public static void printAnswerResult(BufferedWriter bw, int queryIndex, AnyTimeStarFramework sf) throws Exception {
		bw.write(queryIndex + ";" + sf.anyTimeResults.size());
		bw.newLine();
		while (sf.anyTimeResults.size() > 0) {
			GraphResult gr = sf.anyTimeResults.poll();
			String result = "";
			for (Node qNode : gr.assembledResult.keySet()) {
				result += qNode.getId() + ":" + gr.assembledResult.get(qNode).node.getId() + "-";
			}
			result += " -> anyValue: " + gr.anyTimeItemValue + " -> tValue:" + gr.getTotalValue();
			// System.out.println(result);
			bw.write(result);
			bw.newLine();

		}
		bw.newLine();
		bw.flush();

	}

	private void saveTheResults(int queryIndex, BufferedWriter bwMLTime, AnyTimeStarFramework sf,
			Double sfDifferenceTime, AnyTimeStarFramework mlSF, Double mlDifferenceTime,
			Double featuresComputationalTime, Double classifierInferenceTime, Double regressionInferenceTime,
			Double extraMLComputationalTime, int numberOfClassifierCalls, ArrayList<Integer> expansions,
			InfoHolder[] infoHolders) throws Exception {

		HashMap<Integer, Integer> maxAnswersDepth = new HashMap<Integer, Integer>();
		String posfix = "";

		int totalSFDepth = 0;
		// SF Depth
		for (Integer sqNodeId : sf.calcTreeStarQueriesNodeMap.keySet()) {
			totalSFDepth += sf.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging;
			posfix += "(" + sqNodeId + ":" + sf.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging + "),";

			// init with zeros
			maxAnswersDepth.put(sqNodeId, 0);
		}
		posfix += ";";

		// finding the answers depth
		for (Integer sqNodeId : sf.calcTreeStarQueriesNodeMap.keySet()) {
			for (GraphResult gr : sf.anyTimeResults) {
				if (gr.starQueryIndexDepthMap.get(sqNodeId) != null
						&& (maxAnswersDepth.get(sqNodeId) < gr.starQueryIndexDepthMap.get(sqNodeId))) {
					maxAnswersDepth.put(sqNodeId, gr.starQueryIndexDepthMap.get(sqNodeId));
				}
			}
		}

		double sfQuality = 0d;
		for (GraphResult gr : sf.anyTimeResults) {
			sfQuality += gr.anyTimeItemValue;
		}

		int totalMLSFDepth = 0;
		for (Integer sqNodeId : mlSF.calcTreeStarQueriesNodeMap.keySet()) {
			totalMLSFDepth += mlSF.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging;
			posfix += "(" + sqNodeId + ":" + mlSF.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging + "),";
		}
		posfix += ";";

		double mlSFQuality = 0d;
		for (GraphResult gr : mlSF.anyTimeResults) {
			mlSFQuality += gr.anyTimeItemValue;
		}

		int totalAnswersDepth = 0;
		for (Integer sqNodeId : maxAnswersDepth.keySet()) {
			totalAnswersDepth += maxAnswersDepth.get(sqNodeId);
			posfix += "(" + sqNodeId + ":" + maxAnswersDepth.get(sqNodeId) + "),";
		}

		double mlEarlyStoppingError = 0d;
		double mlFurtherStoppingError = 0d;
		double mlStoppingError = 0d;
		double sfStoppingError = 0d;
		int maxDepthML_Answers = 0;
		int maxDepthSF_Answers = 0;

		for (Integer sqNodeId : maxAnswersDepth.keySet()) {
			mlStoppingError += Math.abs(
					maxAnswersDepth.get(sqNodeId) - mlSF.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging);

			// maxDepthML_Answers += Math.max(maxAnswersDepth.get(sqNodeId),
			// mlSF.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging);

			sfStoppingError += Math.abs(
					maxAnswersDepth.get(sqNodeId) - sf.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging);

			int tempStoppingError = Math.abs(
					maxAnswersDepth.get(sqNodeId) - mlSF.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging);

			if (maxAnswersDepth.get(sqNodeId) > mlSF.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging) {
				mlEarlyStoppingError += tempStoppingError;
			} else {
				mlFurtherStoppingError += tempStoppingError;
			}

			// maxDepthSF_Answers += Math.max(maxAnswersDepth.get(sqNodeId),
			// sf.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging);
		}

		// mlStoppingError = 1 - mlStoppingError;
		// sfStoppingError = 1 - sfStoppingError;

		posfix += ";";

		double avgExpansion = 0.0d;
		double expansionCnt = 0;
		if (expansions.size() > 0) {
			for (Integer expansion : expansions) {
				avgExpansion += expansion;
				expansionCnt++;
			}
			avgExpansion = avgExpansion / expansionCnt;
		}

		ArrayList<Double> avgFetchTimeArr = new ArrayList<>();
		ArrayList<Double> avgJoinTimeArr = new ArrayList<>();
		ArrayList<Double> avgFinishCheckingTimeArr = new ArrayList<>();

		for (int l = 0; l < infoHolders.length; l++) {
			avgFetchTimeArr.add(infoHolders[l].mlFetchesTime);
			avgJoinTimeArr.add(infoHolders[l].mlJoinTime);
			avgFinishCheckingTimeArr.add(infoHolders[l].mlCheckShouldFinishTime);

		}
		double totalMLFetchTime = DummyFunctions.computeNonOutlierAverage(avgFetchTimeArr, infoHolders.length);
		double totalMLJoinTime = DummyFunctions.computeNonOutlierAverage(avgJoinTimeArr, infoHolders.length);
		double totalMLFinishCheckingTime = DummyFunctions.computeNonOutlierAverage(avgFinishCheckingTimeArr,
				infoHolders.length);

		bwMLTime.write(queryIndex + ";" + sfDifferenceTime + ";" + mlDifferenceTime + ";" + totalSFDepth + ";"
				+ totalMLSFDepth + ";" + totalAnswersDepth + ";" + sfQuality + ";" + mlSFQuality + ";"
				+ featuresComputationalTime + ";" + mlStoppingError + ";" + sfStoppingError + ";" + mlEarlyStoppingError
				+ ";" + mlFurtherStoppingError + ";" + classifierInferenceTime + ";" + regressionInferenceTime + ";"
				+ extraMLComputationalTime + ";" + posfix + numberOfClassifierCalls + ";" + avgExpansion + ";");

		bwMLTime.write(totalMLFetchTime + ";" + totalMLJoinTime + ";" + totalMLFinishCheckingTime + ";"
				+ infoHolders[0].mlTotalRequestForFetches + ";" + infoHolders[0].mlFetchesCalls + ";"
				+ infoHolders[0].mlJoinCalls + ";" + infoHolders[0].mlFeaturesComputationalTime + ";"
				+ infoHolders[0].mlClsInferenceComputationalTime + ";" + infoHolders[0].mlRegInferenceComputationalTime
				+ ";");

		bwMLTime.newLine();
		bwMLTime.flush();

	}

	private ExpansionFeatures computeExpansionFeatures(int queryIndex, AnyTimeStarFramework starFramework, int level,
			int numberOfStars, int numberOfCalcNodes, BaseFeatures baseFeatures, SelectionFeatures selectionFeatures,
			int paSelected, Features baseStaticFeatures) throws Exception {

		int sqIndex = paSelected - 1;
		int currentPQ = selectionFeatures.pqCurrent[sqIndex];
		double currentThisLB = selectionFeatures.lbCurrent;
		double currentThisUB = selectionFeatures.ubCurrent[paSelected];
		// parent in calcNode;
		double currentParentUB = starFramework.calcTreeNodeMap.get(paSelected).getParent().getData().anytimeUpperBound;
		// int pqDiffThisFromParent =
		// selectionFeatures.pqDiffThisFromParent[sqIndex];
		// int pqDiffThisFromRoot =
		// selectionFeatures.pqDiffThisFromRoot[sqIndex];
		// int generateNextBestMatchQueued =
		// selectionFeatures.generateNextBestMatchQueued[sqIndex];

		double ubDifferenceFromCurrentLB = selectionFeatures.ubDifferenceFromCurrentLB[paSelected];
		double ubDifferenceFromParentUB = selectionFeatures.ubDifferenceFromParentUB[paSelected];
		double ubDifferenceFromRootUB = selectionFeatures.ubDifferenceFromRootUB[paSelected];

		double lbDifferenceFromRootLB = selectionFeatures.lbDifferenceFromRootLB;
		double lbDifferenceFromParentLB = selectionFeatures.lbDifferenceFromParentLB;
		int previousPASelected = selectionFeatures.previousPASelected;

		// int howManyTimesSelectedBefore =
		// selectionFeatures.howManyTimesSelectedBefore[sqIndex];
		/// int contributionToCurrentAnswer = 0;//
		// TODO:????
		// int sqCalcTreeDepth =
		// selectionFeatures.sqCalcTreeDepth[sqIndex];
		int currentDepth = selectionFeatures.currentDepth[sqIndex];
		boolean isStarkIsEnough = selectionFeatures.isStarkIsEnough[sqIndex];
		int remainingPA = starFramework.calcTreeNodeMap.get(paSelected).data.firstPQItemSize
				- selectionFeatures.currentDepth[sqIndex];

		int searchLevel = level;
		// double maxUB = 0d;
		// for (int i = 0; i < numberOfCalcNodes; i++) {
		// maxUB = Math.max(selectionFeatures.ubCurrent[i], maxUB);
		// }

		// getting the (maximum k*ub) - sum(current answers scores)

		// double diffMaxPossibleRankCurrentRank = starFramework.k * maxUB;
		// double currentRank = 0d;

		// for (GraphResult gr : starFramework.anyTimeResults) {
		// diffMaxPossibleRankCurrentRank -= gr.anyTimeItemValue;
		// currentRank += gr.anyTimeItemValue;
		// }

		boolean isPreviouslySelected = false;
		if (selectionFeatures.previousPASelected == paSelected) {
			isPreviouslySelected = true;
		}

		int previousExpansionValue = baseFeatures != null ? baseFeatures.paParentExpansion : 0;

		ExpansionFeatures expFeatures = new ExpansionFeatures(queryIndex, currentPQ, currentThisLB, currentThisUB,
				currentParentUB, 0, 0, 0, ubDifferenceFromCurrentLB, ubDifferenceFromParentUB, ubDifferenceFromRootUB,
				lbDifferenceFromRootLB, lbDifferenceFromParentLB, previousPASelected, 0, currentDepth, isStarkIsEnough,
				remainingPA, searchLevel, 0, isPreviouslySelected, 0, 0, previousExpansionValue, -1);

		// ExpansionFeatures expFeatures = new ExpansionFeatures(queryIndex,
		// currentThisLB, currentThisUB,
		// lbDifferenceFromParentLB, searchLevel, currentRank,
		// previousExpansionValue, -1);

		// ExpansionFeatures expFeatures = new ExpansionFeatures(queryIndex,
		// currentPQ, currentThisLB, currentThisUB,
		// ubDifferenceFromCurrentLB, lbDifferenceFromParentLB, currentDepth,
		// searchLevel,
		// diffMaxPossibleRankCurrentRank, isPreviouslySelected, currentRank,
		// paSelected, previousExpansionValue,
		// -1);

		return expFeatures;
	}

	private SelectionFeatures computeSelectionFeatures(int queryIndex, AnyTimeStarFramework starFramework, int level,
			int numberOfStars, int numberOfCalcNodes, BaseFeatures baseFeatures, Features baseStaticFeatures,
			int maxNumberOfStars, int maxNumberOfCalcNodes) throws Exception {

		int[] pqCurrent = new int[maxNumberOfStars];
		double[] ubCurrent = new double[maxNumberOfCalcNodes];
		double lbCurrent;

		int[] pqDiffThisFromParent = new int[maxNumberOfStars];
		int[] pqDiffThisFromRoot = new int[maxNumberOfStars];
		int[] generateNextBestMatchQueued = new int[maxNumberOfStars];
		double[] ubDifferenceFromCurrentLB = new double[maxNumberOfCalcNodes];
		double[] ubDifferenceFromParentUB = new double[maxNumberOfCalcNodes];
		double[] ubDifferenceFromRootUB = new double[maxNumberOfCalcNodes];
		double lbDifferenceFromRootLB = 0d;
		double lbDifferenceFromParentLB = 0d;

		int[] currentDepth = new int[maxNumberOfStars];
		int[] remainingPA = new int[maxNumberOfStars];
		boolean[] isStarkIsEnough = new boolean[maxNumberOfStars];
		int[] howManySelectedBefore = new int[maxNumberOfStars];
		int previousPASelected = 0;

		lbCurrent = starFramework.leastAnyTimeValueResult;

		if (baseFeatures != null) {
			lbDifferenceFromRootLB = starFramework.leastAnyTimeValueResult - baseFeatures.lbRoot;
			lbDifferenceFromParentLB = starFramework.leastAnyTimeValueResult - baseFeatures.lbParent;
			previousPASelected = baseFeatures.paParentSelected;
		}

		for (int i = 0; i < numberOfStars; i++) {

			// for (NodeWithValue nwv :
			// starFramework.currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
			// .get(starFramework.starQueries.get(i)).keySet()) {
			// generateNextBestMatchQueued[i] +=
			// starFramework.currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
			// .get(starFramework.starQueries.get(i)).get(nwv).size();
			// }

			CalculationNode data = starFramework.calcTreeStarQueriesNodeMap
					.get(starFramework.starQueries.get(i).starQueryCalcNodeIndex).data;

			currentDepth[i] = data.depthOfDigging;

			isStarkIsEnough[i] = data.callStarKIsEnough;

			pqCurrent[i] = starFramework.starkForLeafPQResults.get(starFramework.starQueries.get(i)).size();

			howManySelectedBefore[i] = data.howManyTimesSelectedBefore;

			remainingPA[i] = starFramework.starQueries.get(i).numberOfPAEstimate - currentDepth[i];

			if (baseFeatures != null) {
				// in Diff always current - parent/root.
				pqDiffThisFromParent[i] = pqCurrent[i] - baseFeatures.pqParent[i];
				pqDiffThisFromRoot[i] = pqCurrent[i] - baseFeatures.pqRoot[i];
			}

			// contributionToCurrentAnswer = TODO:???

		}

		for (Integer nodeId : starFramework.calcTreeNodeMap.keySet()) {
			ubCurrent[nodeId] = starFramework.calcTreeNodeMap.get(nodeId).data.anytimeUpperBound;
			ubDifferenceFromCurrentLB[nodeId] = ubCurrent[nodeId] - lbCurrent;
			if (baseFeatures != null) {
				ubDifferenceFromParentUB[nodeId] = ubCurrent[nodeId] - baseFeatures.ubParent[nodeId];
				ubDifferenceFromRootUB[nodeId] = ubCurrent[nodeId] - baseFeatures.ubRoot[nodeId];

			}
		}

		SelectionFeatures sfeatures = new SelectionFeatures(queryIndex, pqCurrent, ubCurrent, lbCurrent,
				pqDiffThisFromParent, pqDiffThisFromRoot, generateNextBestMatchQueued, ubDifferenceFromCurrentLB,
				ubDifferenceFromParentUB, ubDifferenceFromRootUB, lbDifferenceFromRootLB, lbDifferenceFromParentLB,
				previousPASelected, howManySelectedBefore, currentDepth, isStarkIsEnough, remainingPA, -1);

		for (int i = numberOfStars; i < maxNumberOfStars; i++) {
			isStarkIsEnough[i] = true;
		}
		// SelectionFeatures sfeatures = new SelectionFeatures(queryIndex,
		// pqCurrent, ubCurrent, lbCurrent,
		// ubDifferenceFromCurrentLB, ubDifferenceFromParentUB,
		// lbDifferenceFromParentLB, currentDepth,
		// isStarkIsEnough, previousPASelected, -1);

		// sfeatures.print(baseStaticFeatures, bwPASelectionFeatures);

		return sfeatures;
	}

	public BaseFeatures baseFeatureFiller(int queryIndex, SelectionFeatures selectionFeatures,
			AnyTimeStarFramework starFramework, int numberOfStars, int numberOfCalcNodes,
			BaseFeatures previousBaseFeatures, int paSelected, int paExpansion) {

		int[] pqParent = new int[numberOfStars];
		int[] pqRoot = new int[numberOfStars];
		double[] ubParent = new double[numberOfCalcNodes];
		double[] ubRoot = new double[numberOfCalcNodes];

		double lbRoot = 0;
		double lbParent = 0;

		for (int i = 0; i < numberOfStars; i++) {
			pqParent[i] = starFramework.starkForLeafPQResults.get(starFramework.starQueries.get(i)).size();
		}

		for (Integer nodeId : starFramework.calcTreeNodeMap.keySet()) {
			ubParent[nodeId] = starFramework.calcTreeNodeMap.get(nodeId).getData().anytimeUpperBound;
		}

		lbParent = starFramework.leastAnyTimeValueResult;

		if (previousBaseFeatures != null) {
			pqRoot = previousBaseFeatures.pqRoot;
			ubRoot = previousBaseFeatures.ubRoot;
			lbRoot = previousBaseFeatures.lbRoot;
		} else {
			pqRoot = pqParent;
			ubRoot = ubParent;
			lbRoot = lbParent;
		}

		return new BaseFeatures(queryIndex, pqParent, pqRoot, ubParent, ubRoot, lbRoot, lbParent, paSelected,
				paExpansion);

		// return new BaseFeatures(queryIndex, pqParent, pqRoot, lbRoot,
		// lbParent, paSelected, paExpansion);

		// return new BaseFeatures(queryIndex, lbRoot, lbParent, ubRoot,
		// ubParent, paSelected, paExpansion);
	}

	private Features initStaticFeatures(int queryIndex, AnyTimeStarFramework starFramework, int numberOfQNodes,
			int numberOfQRelationships, int numberOfStars, int maxNumberOfStars, int maxNumberOfCalcNodes)
			throws Exception {
		// sumJoinableNodes
		int[] joinableNodes = new int[maxNumberOfStars];

		for (int i = 0; i < numberOfStars; i++) {
			int intersectionSize = 0;
			for (int j = 0; j < numberOfStars; j++) {
				if (i != j) {
					Set<Node> intersections = new HashSet<Node>(
							starFramework.starQueries.get(i).allStarGraphQueryNodes);
					intersections.retainAll(starFramework.starQueries.get(j).allStarGraphQueryNodes);
					intersectionSize += intersections.size();
				}
			}
			joinableNodes[i] = intersectionSize;
		}

		int[] nodesInStar = new int[maxNumberOfStars];
		double[] avgPivotDegreeInDataGraph = new double[maxNumberOfStars];
		int[] estimatedPA = new int[maxNumberOfStars];
		int[] possiblePivots = new int[maxNumberOfStars];
		int[] firstPQItemSize = new int[maxNumberOfStars];

		for (int i = 0; i < numberOfStars; i++) {
			nodesInStar[i] = starFramework.starQueries.get(i).allStarGraphQueryNodes.size();
			avgPivotDegreeInDataGraph[i] = starFramework.starQueries.get(i).avgDegreeOfPossiblePivots;
			estimatedPA[i] = starFramework.starQueries.get(i).numberOfPAEstimate;
			possiblePivots[i] = starFramework.starQueries.get(i).numberOfPossiblePivots;
			firstPQItemSize[i] = starFramework.calcTreeNodeMap
					.get(starFramework.starQueries.get(i).starQueryCalcNodeIndex).getData().firstPQItemSize;
		}

		for (int i = numberOfStars; i < maxNumberOfStars; i++) {
			joinableNodes[i] = 0;
			nodesInStar[i] = 0;
			avgPivotDegreeInDataGraph[i] = 0d;
			estimatedPA[i] = 0;
			possiblePivots[i] = 0;
			firstPQItemSize[i] = 0;
		}

		return new Features(numberOfQNodes, numberOfQRelationships, numberOfStars, nodesInStar,
				avgPivotDegreeInDataGraph, estimatedPA, firstPQItemSize, possiblePivots, joinableNodes);

		// return new Features(queryIndex, numberOfQRelationships,
		// numberOfStars, nodesInStar, estimatedPA,
		// firstPQItemSize, joinableNodes);

	}

	private void fillNormVectorsIfTheyAreExist() throws Exception {
		File f = new File("normalizationVectors.txt");
		if (!f.exists()) {
			System.out.println("normalizationVectors.txt doesn't exist");
			return;
		}

		selectionNormalizationFeaturesVector = new float[SELECTION_FEATURES_SIZE];
		expansionNormalizationFeaturesVector = new float[EXPANSION_FEATURES_SIZE];

		FileInputStream fis = new FileInputStream(f.getAbsolutePath());
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String selectionNormalizationLine = null;
		String expansionNormalizationLine = null;

		selectionNormalizationLine = br.readLine();
		String[] splitedSelNorm = selectionNormalizationLine.split(",");
		for (int i = 0; i < SELECTION_FEATURES_SIZE; i++) {
			selectionNormalizationFeaturesVector[i] = Float.parseFloat(splitedSelNorm[i]);
		}

		expansionNormalizationLine = br.readLine();
		String[] splitedRegNorm = expansionNormalizationLine.split(",");
		for (int i = 0; i < EXPANSION_FEATURES_SIZE; i++) {
			expansionNormalizationFeaturesVector[i] = Float.parseFloat(splitedRegNorm[i]);
		}

		br.close();
	}

	private int boosterPredictInstance(Predictor regressor, Features baseStaticFeatures, int paSelected,
			ExpansionFeatures expansionFeatures, int maxNumberOfStars, float[] expansionNormalizationFeaturesVector)
			throws Exception {

		double[] predictsReg = regressor.predict(commonFunctions.createRegressionBoosterInstance(baseStaticFeatures,
				expansionFeatures, maxNumberOfStars, expansionNormalizationFeaturesVector));

		// regPredicted.add(predictsReg[0][0]);

		return Math.abs((int) Math.ceil(predictsReg[0]));

	}

	private int boosterClassifyInstance(Predictor classifier, Features baseStaticFeatures,
			SelectionFeatures selectionFeatures, int maxNumberOfStars, float[] selectionNormalizationFeaturesVector)
			throws Exception {

		double[] predicts = classifier.predict(commonFunctions.createClassificationBoosterInstance(baseStaticFeatures,
				selectionFeatures, maxNumberOfStars, selectionNormalizationFeaturesVector));
		// clsPredicted.add(predicts[0][0]);
		return (int) predicts[0];
	}

	private ArrayList<HashMap<Integer, Integer>> readOracleSequence(int queryIndex, String oracleSequenceFile)
			throws Exception {

		ArrayList<HashMap<Integer, Integer>> fetchSeqByStarQueryIndex = new ArrayList<HashMap<Integer, Integer>>();

		FileInputStream fis = new FileInputStream(oracleSequenceFile);

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		while ((line = br.readLine()) != null) {
			String[] tokenized = line.split(";");
			if (tokenized.length == 0) {
				continue;
			}

			if (queryIndex == Integer.parseInt(tokenized[0])) {
				String[] sequence = tokenized[1].replace(">", "").replace("<", "").split(",");
				for (int s = 0; s < sequence.length; s++) {
					sequence[s] = sequence[s].trim();
				}
				for (String queryFetchNumberPair : sequence) {
					if (queryFetchNumberPair.length() > 0) {
						HashMap<Integer, Integer> action = new HashMap<Integer, Integer>();
						String[] actionStr = queryFetchNumberPair.split(":");
						action.put(Integer.parseInt(actionStr[0].trim()), Integer.parseInt(actionStr[1].trim()));
						fetchSeqByStarQueryIndex.add(action);
					}
				}
				break;
			}
		}

		br.close();

		return fetchSeqByStarQueryIndex;

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
