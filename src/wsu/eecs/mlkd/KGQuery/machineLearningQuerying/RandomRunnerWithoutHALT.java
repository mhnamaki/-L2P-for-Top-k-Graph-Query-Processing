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
import java.util.Random;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.tooling.GlobalGraphOperations;

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

public class RandomRunnerWithoutHALT {
	private static String MODELGRAPH_DB_PATH = "";
	private static String PATTERNGRAPH_DB_PATH = "";

	public static String queryFileName = "";
	public static String queryFileDirectory = "";

	public static String GName = ""; // Yago, DBPedia, ...

	public static String queryDBInNeo4j = "query";
	public static String GDirectory = "";
	public int numberOfSameExperiment = 1;
	public AnyTimeStarFramework sf;
	private AnyTimeStarFramework rndSF;

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

	public CommonFunctions commonFunctions = new CommonFunctions();

	public enum WhichOracle {
		microSingleAction, macroSingleAction, macroMultiAction
	};

	public enum MinObjectiveFunction {
		DiffTime, TotalTime, JustDepth
	};

	public void initialize(String[] args) throws Exception {
		int numberOfPrefixChars = 0;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-queryFileName")) {
				queryFileName = args[++i];
				queryFileName = queryFileName.replace(".txt", "");
			} else if (args[i].equals("-queryFileDirectory")) {
				queryFileDirectory = args[++i];
			} else if (args[i].equals("-GName")) {
				GName = args[++i];
			} else if (args[i].equals("-GDirectory")) {
				GDirectory = args[++i];
			} else if (args[i].equals("-queryInfoAddress")) {
				queryInfoAddress = args[++i];
			} else if (args[i].equals("-similarityThreshold")) {
				DummyProperties.similarityThreshold = Float.parseFloat(args[++i]);
			} else if (args[i].equals("-numberOfSameExperiment")) {
				numberOfSameExperiment = Integer.parseInt(args[++i]);
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
			}
		}

		System.out.println("foldStartFrom: " + foldStartFrom);
		System.out.println("foldEndTo: " + foldEndTo);
		System.out.println(" startingQueryIndex: " + startingQueryIndex);
		System.out.println("endingQueryIndex: " + endingQueryIndex);
		System.out.println("numberOfSameExperiment : " + numberOfSameExperiment);

		cacheServer = new CacheServer();

		if (numberOfPrefixChars > 0) {
			DummyProperties.numberOfPrefixChars = numberOfPrefixChars;
		}

		MODELGRAPH_DB_PATH = GDirectory + GName;
		PATTERNGRAPH_DB_PATH = queryFileDirectory + queryDBInNeo4j + "_" + startingQueryIndex + ".db";

		String totalParams = "";
		for (String arg : args) {
			totalParams += arg + ", ";
		}
		DummyFunctions.printIfItIsInDebuggedMode(totalParams);

		QueryGenerator queryGenerator = new QueryGenerator(GDirectory + GName);

		System.out.println("GDirectory + GName: " + GDirectory + GName);
		System.out.println("numberOfPrefixChars: " + numberOfPrefixChars);

		// output the SF versions
		File foutTimeSF = new File(queryFileName + "_" + GName + "_timeResults_SF.txt");
		FileOutputStream fosTimeSF = new FileOutputStream(foutTimeSF, true);

		BufferedWriter bwTimeSF = new BufferedWriter(new OutputStreamWriter(fosTimeSF));

		// output beamSearch with different beamSizes and delta's
		File foutTimeBeamSearch = new File(queryFileName + "_" + GName + "_timeResults_BeamSearch.txt");
		FileOutputStream fosTimeBeamSearch = new FileOutputStream(foutTimeBeamSearch, true);

		BufferedWriter bwTimeBeamSearch = new BufferedWriter(new OutputStreamWriter(fosTimeBeamSearch));

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

		if (startingQueryIndex == 0) {
			bwTimeSF.write(
					"OurSF;queryFileName;queryIndex;numberOfQNodes;numberOfQRelationships;GName;k;difference;finalResultSizeTemp;numberOfStarQueries;depthAnswer;depthSF;firstPQItemSize;totalSFDepth;totalAnswersDepth");
			bwTimeSF.newLine();

			bwTimeBeamSearch.write(
					"BeamSearch;GName;queryIndex;queryFileName;numberOfNodes;numberOfRelationships;beamSize;delta;sizeOfAllReturnedAnswers;k;difference;beamSearchNode.level;oracleSearchTime;type;depth;totalSFDepth;totalAnswersDepth;totalOracleDepth;");

			bwTimeBeamSearch.write(
					"avgSFFetchTime;avgSFJoinTime;avgSFFinishCheckingTime;avgOracleFetchTime;avgOracleJoinTime;avgOracleFinishCheckingTime;sfFetchesCalls;sfJoinCalls;sfTotalRequestForFetches;oracleFetchesCalls;oracleJoinCalls;oracleTotalRequestForFetches;sequence;");
			bwTimeBeamSearch.newLine();
		}

		knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(MODELGRAPH_DB_PATH)
				.setConfig(GraphDatabaseSettings.pagecache_memory, "6g")
				.setConfig(GraphDatabaseSettings.allow_store_upgrade, "true").newGraphDatabase();
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

						InfoHolder[] infoHolders = new InfoHolder[numberOfSameExperiment];
						for (int i = 0; i < infoHolders.length; i++) {
							infoHolders[i] = new InfoHolder();
						}

						totalAnswersDepth = 0;
						totalSFDepth = 0;

						Random rnd = new Random();
						int[] expansionOptions = new int[] { 10, 20, 60, 100, 140, 180, 200 };

						File foutTime = new File(GName + "_randomAlgorithmWithoutHALT.txt");
						FileOutputStream fosTime = new FileOutputStream(foutTime, true);
						BufferedWriter bwMLTime = new BufferedWriter(new OutputStreamWriter(fosTime));
						if (isFirstQuery) {
							bwMLTime.write(
									"queryIndex; sfDifferenceTime;mlDifferenceTime; totalSFDepth;totalrndSFDepth;totalAnswersDepth;sfQuality;rndSFQuality;featuresComputationalTime;mlStoppingError;sfStoppingError;mlEarlyStoppingError;mlFurtherStoppingError;classifierInferenceTime;regressionInferenceTime;extraMLComputationalTime;SFDepth; rndSF Depth;Answers Depth;numberOfClassificationCalls; expansionAvg;");
							bwMLTime.write(
									"totalMLFetchTime; totalMLJoinTime;totalMLFinishCheckingTime;mlTotalRequestForFetches; mlFetchesCalls;mlJoinCalls; mlFeaturesComputationalTime;mlClsInferenceComputationalTime;mlRegInferenceComputationalTime;");
							bwMLTime.newLine();
						}

						ArrayList<Integer> expansions = new ArrayList<Integer>();
						int numberOfClassifierCalls = 0;

						Double diffMLTime = 0d;
						Double finalMLDifferenceTime = 0d;
						ArrayList<Double> differenceTimesML = new ArrayList<Double>();

						for (int p = 0; p < infoHolders.length; p++) {
							infoHolders[p].mlFetchesCalls = 0;
							infoHolders[p].mlJoinCalls = 0;
							infoHolders[p].mlFetchesTime = 0;
							infoHolders[p].mlJoinTime = 0;
							infoHolders[p].mlCheckShouldFinishTime = 0;
							infoHolders[p].mlClsInferenceComputationalTime = 0;
							infoHolders[p].mlFeaturesComputationalTime = 0;
							infoHolders[p].mlRegInferenceComputationalTime = 0;
							infoHolders[p].mlTotalRequestForFetches = 0;
						}

						double sJoinTime, sFetchTime;
						for (int exp = 0; exp < numberOfSameExperiment; exp++) {
							rndSF = getNewStarFrameworkInstance(rndSF);

							int numberOfStars = rndSF.starQueries.size();

							int[] selectionOptions = new int[numberOfStars]; // +HALT
																				// action

							int depthJoinLevel = 0;

							start_time = System.nanoTime();
							for (Integer starQNode : rndSF.calcTreeStarQueriesNodeMap.keySet()) {
								sFetchTime = System.nanoTime();

								rndSF.calcTreeStarQueriesNodeMap.get(starQNode)
										.getData().numberOfPartialAnswersShouldBeFetched = 1;

								infoHolders[exp].mlFetchesCalls++;
								infoHolders[exp].mlTotalRequestForFetches++;

								rndSF.anyTimeStarkForLeaf(knowledgeGraph,
										rndSF.calcTreeStarQueriesNodeMap.get(starQNode), neighborIndexingInstance,
										cacheServer);

								infoHolders[exp].mlFetchesTime += ((System.nanoTime() - sFetchTime) / 1e6);

								sJoinTime = System.nanoTime();

								depthJoinLevel = rndSF.calcTreeStarQueriesNodeMap.get(starQNode).levelInCalcTree - 1;

								for (; depthJoinLevel >= 0; depthJoinLevel--) {
									CalculationTreeSiblingNodes calculationTreeSiblingNodes = rndSF.joinLevelSiblingNodesMap
											.get(depthJoinLevel);
									infoHolders[exp].mlJoinCalls++;
									rndSF.anyTimeTwoWayHashJoin(calculationTreeSiblingNodes.leftNode,
											calculationTreeSiblingNodes.rightNode, rndSF.k);

								}

								infoHolders[exp].mlJoinTime += ((System.nanoTime() - sJoinTime) / 1e6);
							}

							depthJoinLevel = 0;
							int paExpansion = 1;

							boolean shouldFinish = false;

							while (true) {
								shouldFinish = rndSF.anyTimeAlgorithmShouldFinish();

								if (shouldFinish) {
									break;
								}

								int paSelected = 0;

								paSelected = rnd.nextInt(selectionOptions.length) + 1;

								numberOfClassifierCalls++;

								// if (paSelected < 1) {
								// break;
								// }
								// reaching to the queries time-bound
								// if ((System.nanoTime() - start_time) / 1e6 >
								// 60000) {
								// break;
								// }

								// if selected SQ stark is enough?

								while (rndSF.calcTreeStarQueriesNodeMapBySQMLIndex.get(paSelected) == null
										|| rndSF.calcTreeStarQueriesNodeMapBySQMLIndex
												.get(paSelected).data.callStarKIsEnough) {
									paSelected = rnd.nextInt(selectionOptions.length) + 1;
								}

								paExpansion = expansionOptions[rnd.nextInt(expansionOptions.length)];

								expansions.add(paExpansion);

								sFetchTime = System.nanoTime();

								TreeNode<CalculationNode> thisCalcNode = rndSF.calcTreeStarQueriesNodeMapBySQMLIndex
										.get(paSelected);
								thisCalcNode.getData().numberOfPartialAnswersShouldBeFetched = paExpansion;
								rndSF.anyTimeStarkForLeaf(knowledgeGraph, thisCalcNode, neighborIndexingInstance,
										cacheServer);
								infoHolders[exp].mlFetchesCalls++;
								infoHolders[exp].mlTotalRequestForFetches += paExpansion;

								infoHolders[exp].mlFetchesTime += ((System.nanoTime() - sFetchTime) / 1e6);

								sJoinTime = System.nanoTime();
								depthJoinLevel = thisCalcNode.levelInCalcTree - 1;

								for (; depthJoinLevel >= 0; depthJoinLevel--) {
									CalculationTreeSiblingNodes calculationTreeSiblingNodes = rndSF.joinLevelSiblingNodesMap
											.get(depthJoinLevel);
									infoHolders[exp].mlJoinCalls++;
									rndSF.anyTimeTwoWayHashJoin(calculationTreeSiblingNodes.leftNode,
											calculationTreeSiblingNodes.rightNode, rndSF.k);
								}
								depthJoinLevel = 0;

								infoHolders[exp].mlJoinTime += ((System.nanoTime() - sJoinTime) / 1e6);

							}

							end_time = System.nanoTime();

							diffMLTime = (end_time - start_time) / 1e6;
							System.out.println("ML StarFramework exp: " + exp + " is finished in "
									+ diffMLTime.intValue() + " miliseconds!");

							differenceTimesML.add(diffMLTime);

							if (exp != (numberOfSameExperiment - 1)) {
								rndSF = null;
							} else {
								finalMLDifferenceTime = Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimesML,
										numberOfSameExperiment);

								System.out.println("ML StarFramework avg is finished in " + finalMLDifferenceTime
										+ " miliseconds!");
							}
						}

						Double diffSFTime = 0d;
						Double finalSFDifferenceTime = 0d;
						ArrayList<Double> differenceTimesSF = new ArrayList<Double>();
						for (int exp = 0; exp < numberOfSameExperiment; exp++) {
							sf = getNewStarFrameworkInstance(sf);
							infoHolders[exp].queryIndex = queryIndex;
							start_time = System.nanoTime();
							sf.starRoundRobinRun(queryGraph, knowledgeGraph, neighborIndexingInstance, cacheServer,
									infoHolders[exp]);

							end_time = System.nanoTime();
							diffSFTime = (end_time - start_time) / 1e6;
							System.out.println("StarFramework exp: " + exp + " is finished in " + diffSFTime.intValue()
									+ " miliseconds!");

							differenceTimesSF.add(diffSFTime);

							if (exp != (numberOfSameExperiment - 1)) {
								sf = null;
							}
						}
						System.gc();
						System.runFinalization();

						finalSFDifferenceTime = Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimesSF,
								numberOfSameExperiment);

						System.out
								.println("StarFramework avg is finished in " + finalSFDifferenceTime + " miliseconds!");

						int anyTimeResultsSizeTemp = sf.anyTimeResults.size();
						String prefix = "OurSF;" + queryFileName + ";" + queryIndex + ";" + numberOfQNodes + ";"
								+ numberOfQRelationships + ";" + GName + ";" + k + ";" + finalSFDifferenceTime + ";"
								+ anyTimeResultsSizeTemp + ";" + sf.calcTreeStarQueriesNodeMap.keySet().size() + ";";

						HashMap<Integer, Integer> maxAnyTimeAnswerDepthStarQueryMap = new HashMap<Integer, Integer>();
						finalizeMethod(sf, bwTimeSF, prefix, maxAnyTimeAnswerDepthStarQueryMap);
						bwTimeSF.flush();
						saveTheResults(queryIndex, bwMLTime, sf, finalSFDifferenceTime, rndSF, finalMLDifferenceTime,
								0d, 0d, 0d, 0d, numberOfClassifierCalls, expansions, infoHolders);
						sf = null;
						rndSF = null;

						System.gc();
						System.runFinalization();

						isFirstQuery = false;

						System.out.println();
						tx2.success();
						tx2.close();

						differenceTimesSF.clear();
						differenceTimesML.clear();

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
				queryGraph.shutdown();
				queryGraph = null;
				System.gc();
				System.runFinalization();
			}

			bwTimeBeamSearch.flush();
			bwTimeSF.flush();
		}

		bwTimeBeamSearch.close();
		bwTimeSF.close();

		knowledgeGraph.shutdown();

		System.out.println("program is finished properly!");

	}

	public static void main(String[] args) throws Exception {
		RandomRunnerWithoutHALT beamSearchRunner = new RandomRunnerWithoutHALT();
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
			Double sfDifferenceTime, AnyTimeStarFramework rndSF, Double mlDifferenceTime,
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

		int totalrndSFDepth = 0;
		for (Integer sqNodeId : rndSF.calcTreeStarQueriesNodeMap.keySet()) {
			totalrndSFDepth += rndSF.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging;
			posfix += "(" + sqNodeId + ":" + rndSF.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging + "),";
		}
		posfix += ";";

		double rndSFQuality = 0d;
		for (GraphResult gr : rndSF.anyTimeResults) {
			rndSFQuality += gr.anyTimeItemValue;
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
		// int maxDepthML_Answers = 0;
		// int maxDepthSF_Answers = 0;

		for (Integer sqNodeId : maxAnswersDepth.keySet()) {
			mlStoppingError += Math.abs(
					maxAnswersDepth.get(sqNodeId) - rndSF.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging);

			// maxDepthML_Answers += Math.max(maxAnswersDepth.get(sqNodeId),
			// rndSF.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging);

			sfStoppingError += Math.abs(
					maxAnswersDepth.get(sqNodeId) - sf.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging);

			int tempStoppingError = Math.abs(
					maxAnswersDepth.get(sqNodeId) - rndSF.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging);

			if (maxAnswersDepth.get(sqNodeId) > rndSF.calcTreeStarQueriesNodeMap.get(sqNodeId).data.depthOfDigging) {
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
				+ totalrndSFDepth + ";" + totalAnswersDepth + ";" + sfQuality + ";" + rndSFQuality + ";"
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
