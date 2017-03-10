package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
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

import wsu.eecs.mlkd.KGQuery.TopKQuery.AnyTimeStarFramework;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CacheServer;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CalculationNode;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CalculationTreeSiblingNodes;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy;
import wsu.eecs.mlkd.KGQuery.TopKQuery.GraphResult;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.NeighborIndexing;
import wsu.eecs.mlkd.KGQuery.TopKQuery.NodeWithValue;
import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;
import wsu.eecs.mlkd.KGQuery.TopKQuery.TreeNode;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;
import wsu.eecs.mlkd.KGQuery.test.QueryFromFile;
import wsu.eecs.mlkd.KGQuery.test.QueryGenerator;

public class SequenceRunnerForAllSQs {
	private static String MODELGRAPH_DB_PATH = "";
	private static String PATTERNGRAPH_DB_PATH = "";

	public static String queryFileName = "";
	public static String queryFileDirectory = "";

	public static String GName = ""; // Yago, DBPedia, ...

	public static String queryDBInNeo4j = "query.db";
	public static String GDirectory = "";
	public int numberOfSameExperiment = 1;
	public AnyTimeStarFramework starFramework2;
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
	public int startingQueryIndex;
	private String oracleSequenceFile;
	private int endingQueryIndex = 1000000;

	public enum WhichOracle {
		microSingleAction, macroSingleAction, macroMultiAction
	};

	public void initialize(String[] args) throws Exception {
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
			} else if (args[i].equals("-GName")) {
				GName = args[++i];

			} else if (args[i].equals("-GDirectory")) {
				GDirectory = args[++i];

			} else if (args[i].equals("-similarityThreshold")) {
				DummyProperties.similarityThreshold = Float.parseFloat(args[++i]);
			} else if (args[i].equals("-numberOfSameExperiment")) {
				numberOfSameExperiment = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-k")) {
				k = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-teta")) {
				teta = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-numberOfPrefixChars")) {
				numberOfPrefixChars = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-startingQueryIndex")) {
				startingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-endingQueryIndex")) {
				endingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-oracleSequenceFile")) {
				oracleSequenceFile = args[++i];
			}
		}

		cacheServer = new CacheServer();

		if (numberOfPrefixChars > 0) {
			DummyProperties.numberOfPrefixChars = numberOfPrefixChars;
		}
		if (!GDirectory.endsWith("/")) {
			GDirectory += "/";
		}
		MODELGRAPH_DB_PATH = GDirectory + GName;
		PATTERNGRAPH_DB_PATH = queryFileDirectory + queryDBInNeo4j;

		String totalParams = "";
		for (String arg : args) {
			totalParams += arg + ", ";
		}
		DummyFunctions.printIfItIsInDebuggedMode(totalParams);

		QueryGenerator queryGenerator = new QueryGenerator(GDirectory + GName);

		// output the SF versions
		File foutTimeSF = new File(queryFileName + "_" + GName + "_timeResults.txt");
		FileOutputStream fosTimeSF = new FileOutputStream(foutTimeSF);
		BufferedWriter bwTimeSF = new BufferedWriter(new OutputStreamWriter(fosTimeSF));

		String firstLine = "";

		int maxNumberOfSQ = 5;
		int maxNumberOfCalcNodes = 2 * maxNumberOfSQ - 1;

		File foutStaticFeatures = new File("staticFeaturesForAll.txt");
		FileOutputStream fosStaticFeatures = new FileOutputStream(foutStaticFeatures, true);
		BufferedWriter bwStaticFeatures = new BufferedWriter(new OutputStreamWriter(fosStaticFeatures));

		if (startingQueryIndex == 0) {
			firstLine = "queryIndex,nodes,edges,stars,";
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "nodesInStar" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "avgPivotDegreeInDataGraph" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "estimatedPA" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "firstPQItemSize" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "possiblePivots" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "joinableNodes" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "anyTimeDepth" + m + ",";
			}

			bwStaticFeatures.write(firstLine);
			bwStaticFeatures.newLine();

		}

		File foutPASelectionFeatures = new File("paSelectionFeaturesForAll.txt");
		FileOutputStream fosPASelectionFeatures = new FileOutputStream(foutPASelectionFeatures, true);
		BufferedWriter bwPASelectionFeatures = new BufferedWriter(new OutputStreamWriter(fosPASelectionFeatures));

		File foutStoppingFeatures = new File("stoppingFeaturesForAll.txt");
		FileOutputStream fosStoppingFeatures = new FileOutputStream(foutStoppingFeatures, true);
		BufferedWriter bwStoppingFeatures = new BufferedWriter(new OutputStreamWriter(fosStoppingFeatures));

		if (startingQueryIndex == 0) {
			firstLine = "queryIndex,nodes,edges,stars,";
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "nodesInStar" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "avgPivotDegreeInDataGraph" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "estimatedPA" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "firstPQItemSize" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "possiblePivots" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "joinableNodes" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "pqCurrent" + m + ",";
			}

			for (int m = 1; m <= maxNumberOfCalcNodes; m++) {
				firstLine += "ubCurrent" + m + ",";
			}
			firstLine += "lbCurrent,";

			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "pqDiffThisFromParent" + m + ",";
			}

			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "pqDiffThisFromRoot" + m + ",";
			}

			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "generateNextBestMatchQueued" + m + ",";
			}

			for (int m = 1; m <= maxNumberOfCalcNodes; m++) {
				firstLine += "ubDifferenceFromCurrentLB" + m + ",";
			}

			for (int m = 1; m <= maxNumberOfCalcNodes; m++) {
				firstLine += "ubDifferenceFromParentUB" + m + ",";
			}

			for (int m = 1; m <= maxNumberOfCalcNodes; m++) {
				firstLine += "ubDifferenceFromRootUB" + m + ",";
			}

			firstLine += "lbDifferenceFromRootLB,";
			firstLine += "lbDifferenceFromParentLB,";

			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "howManyTimesSelectedBefore" + m + ",";
			}

			// for (int m = 1; m <= maxNumberOfSQ; m++) {
			// firstLine += "contributionToCurrentAnswer" + m + ",";
			// }
			//
			// for (int m = 1; m <= maxNumberOfSQ; m++) {
			// firstLine += "sqCalcTreeDepth" + m + ",";
			// }

			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "currentDepth" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "isStarkIsEnough" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "remainingPA" + m + ",";
			}

			firstLine += "previousPASelected" + ",";

			firstLine += "starQuerySelectedIndex" + ",";

			bwPASelectionFeatures.write(firstLine);
			bwPASelectionFeatures.newLine();

			bwStoppingFeatures.write(firstLine);
			bwStoppingFeatures.newLine();

		}

		File foutPAExpansionFeatures = new File("paExpansionFeaturesForAll.txt");
		FileOutputStream fosPAExpansionFeatures = new FileOutputStream(foutPAExpansionFeatures, true);
		BufferedWriter bwPAExpansionFeatures = new BufferedWriter(new OutputStreamWriter(fosPAExpansionFeatures));

		if (startingQueryIndex == 0) {
			firstLine = "queryIndex,nodes,edges,stars,";
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "nodesInStar" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "avgPivotDegreeInDataGraph" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "estimatedPA" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "firstPQItemSize" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "possiblePivots" + m + ",";
			}
			for (int m = 1; m <= maxNumberOfSQ; m++) {
				firstLine += "joinableNodes" + m + ",";
			}
			firstLine += "currentPQ,currentThisLB,currentThisUB,currentParentUB,pqDiffThisFromParent,pqDiffThisFromRoot,"
					+ "generateNextBestMatchQueued,ubDifferenceFromCurrentLB,ubDifferenceFromParentUB,ubDifferenceFromRootUB,"
					+ "lbDifferenceFromRootLB,lbDifferenceFromParentLB,howManyTimesSelectedBefore,"
					+ "currentDepth,isStarkIsEnough,remainingPA,searchLevel,diffMaxPossibleRankCurrentRank,"
					+ "isPreviouslySelected,maxUB,currentRank,previousPASelected,paSelected,previousExpansionValue,expandValue";
		}

		bwPAExpansionFeatures.write(firstLine);
		bwPAExpansionFeatures.newLine();
		// }

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

		HashSet<Integer> queriesSet = readQueryIndexToBeChecked();

		for (File file : CommonFunctions.fileInTheDirfinder(queryFileDirectory)) {
			queryFileName = file.getName();

			List<QueryFromFile> queriesFromFile = queryGenerator.getQueryFromFile(file.getAbsolutePath());

			int queryIndex = 0;
			for (QueryFromFile queryFromFile : queriesFromFile) {

				DummyFunctions.printIfItIsInDebuggedMode("start ConstrucQueryGraph");

				queryIndex = queryFromFile.queryIndex;
				if (queryIndex < startingQueryIndex || queryIndex > endingQueryIndex) {
					continue;
				}

				if (!queriesSet.contains(queryIndex)) {
					continue;
				}

				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH,
						queryFromFile);
				DummyFunctions.printIfItIsInDebuggedMode("end ConstrucQueryGraph");

				queryGraph = smallGraph;

				start_time = System.nanoTime();
				neighborIndexingInstance.queryNeighborIndexer(queryGraph);
				end_time = System.nanoTime();
				difference = (end_time - start_time) / 1e6;
				Dummy.DummyFunctions
						.printIfItIsInDebuggedMode("queryNeighborIndexer finished in " + difference + "miliseconds!");

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

						starFramework2 = getNewStarFrameworkInstance();

						int numberOfStars = starFramework2.starQueries.size();
						int numberOfCalcNodes = starFramework2.calcTreeNodeMap.size();

						Features baseStaticFeatures = null;

						// reading the oracle sequences
						ArrayList<HashMap<Integer, Integer>> fetchSeqByStarQueryIndex = readOracleSequence(queryIndex,
								starFramework2.starQueries.size());

						if (fetchSeqByStarQueryIndex.size() < numberOfStars) {
							tx2.success();
							tx2.close();
							queryGraph.shutdown();
							queryGraph = null;
							System.err.println(fetchSeqByStarQueryIndex.size() + " < " + numberOfStars);
							continue;
						}
						// boolean shouldFinish = false;

						start_time = System.nanoTime();
						BaseFeatures baseFeatures = null;
						SelectionFeatures selectionFeatures = null;
						ExpansionFeatures expansionFeatures = null;
						SelectionFeatures stoppingFeatures = null;
						int paExpansion = 1;
						for (int level = 0; level < fetchSeqByStarQueryIndex.size(); level++) {
							HashMap<Integer, Integer> fetchByQueryIndex = fetchSeqByStarQueryIndex.get(level);
							int depthJoinLevel = 0;
							int paSelected = 0;

							for (Integer queryIndexToBeFetched : fetchByQueryIndex.keySet()) {
								paSelected = queryIndexToBeFetched;
							}

							if (level >= numberOfStars) {

								selectionFeatures = computeSelectionFeatures(queryIndex, starFramework2, level,
										numberOfStars, numberOfCalcNodes, baseFeatures, paSelected, baseStaticFeatures,
										bwPASelectionFeatures, maxNumberOfSQ, maxNumberOfCalcNodes, true);

								expansionFeatures = computeExpansionFeatures(queryIndex, starFramework2, level,
										numberOfStars, numberOfCalcNodes, baseFeatures, selectionFeatures, paSelected,
										fetchByQueryIndex.get(paSelected), baseStaticFeatures, bwPAExpansionFeatures,
										maxNumberOfSQ, maxNumberOfCalcNodes, true);

							}

							// TODO: for multi-action?
							if (level >= numberOfStars) {
								baseFeatures = baseFeatureFiller(queryIndex, selectionFeatures, starFramework2,
										numberOfStars, numberOfCalcNodes, baseFeatures, paSelected,
										fetchByQueryIndex.get(paSelected), maxNumberOfSQ, maxNumberOfCalcNodes);
							}
							for (Integer queryIndexToBeFetched : fetchByQueryIndex.keySet()) {

								TreeNode<CalculationNode> thisCalcNode = starFramework2.calcTreeStarQueriesNodeMapBySQMLIndex
										.get(queryIndexToBeFetched); // sq1,
																		// sq2,
																		// sq3,
																		// ...
								thisCalcNode.getData().numberOfPartialAnswersShouldBeFetched = fetchByQueryIndex
										.get(queryIndexToBeFetched);
								starFramework2.anyTimeStarkForLeaf(knowledgeGraph, thisCalcNode,
										neighborIndexingInstance, cacheServer);
								depthJoinLevel = thisCalcNode.levelInCalcTree - 1;

							}

							for (; depthJoinLevel >= 0; depthJoinLevel--) {
								CalculationTreeSiblingNodes calculationTreeSiblingNodes = starFramework2.joinLevelSiblingNodesMap
										.get(depthJoinLevel);
								starFramework2.anyTimeTwoWayHashJoin(calculationTreeSiblingNodes.leftNode,
										calculationTreeSiblingNodes.rightNode, starFramework2.k);
							}
							depthJoinLevel = 0;

							if (level == (numberOfStars - 1)) {
								baseStaticFeatures = initStaticFeatures(queryIndex, bwStaticFeatures, starFramework2,
										numberOfQNodes, numberOfQRelationships, numberOfStars, maxNumberOfSQ,
										maxNumberOfCalcNodes);
							}

						}

						if (starFramework2.anyTimeAlgorithmShouldFinish()) {

						}

						selectionFeatures = computeSelectionFeatures(queryIndex, starFramework2,
								fetchSeqByStarQueryIndex.size(), numberOfStars, numberOfCalcNodes, baseFeatures,
								0 /* STOP */, baseStaticFeatures, bwPASelectionFeatures, maxNumberOfSQ,
								maxNumberOfCalcNodes, true);

						stoppingFeatures = computeSelectionFeatures(queryIndex, starFramework2,
								fetchSeqByStarQueryIndex.size(), numberOfStars, numberOfCalcNodes, baseFeatures,
								0 /* STOP */, baseStaticFeatures, bwStoppingFeatures, maxNumberOfSQ,
								maxNumberOfCalcNodes, true);

						// adding more stopping instances after finishing the
						// procedure:

						// addingMoreStoppingInstancesAfterFinishingTheOracle(queryIndex,
						// starFramework2,
						// fetchSeqByStarQueryIndex.size(), numberOfStars,
						// numberOfCalcNodes, baseFeatures, 0,
						// baseStaticFeatures, maxNumberOfSQ,
						// maxNumberOfCalcNodes, bwStoppingFeatures);

						finalizeFeatures(queryIndex, bwStaticFeatures, starFramework2, baseStaticFeatures,
								maxNumberOfSQ);
						starFramework2 = null;

						System.gc();
						System.runFinalization();

						bwTimeSF.flush();

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

		bwTimeSF.flush();
		bwTimeSF.close();

		bwPASelectionFeatures.flush();
		bwPASelectionFeatures.close();
		bwStaticFeatures.flush();
		bwStaticFeatures.close();
		bwPAExpansionFeatures.flush();
		bwPAExpansionFeatures.close();

		knowledgeGraph.shutdown();

		System.out.println("program is finished properly!");

	}

	private void addingMoreStoppingInstancesAfterFinishingTheOracle(int queryIndex, AnyTimeStarFramework starFramework,
			int level, int numberOfStars, int numberOfCalcNodes, BaseFeatures baseFeatures, int paSelected,
			Features baseStaticFeatures, int maxNumberOfSQ, int maxNumberOfCalcNodes, BufferedWriter bwStoppingFeatures)
			throws Exception {
		// BaseFeatures baseFeatures = null;
		SelectionFeatures selectionFeatures = null;
		ExpansionFeatures expansionFeatures = null;
		SelectionFeatures stoppingFeatures = null;
		int paExpansion = 1;
		int stoppingCnt = 0;
		ArrayList<Integer> sqMLIndices = new ArrayList<Integer>();
		for (int i = 0; i < starFramework2.starQueries.size(); i++) {
			sqMLIndices.add(i + 1);
		}

		while (stoppingCnt < 100 && sqMLIndices.size() > 0) {

			paSelected = sqMLIndices.get(stoppingCnt % sqMLIndices.size());

			stoppingCnt++;

			int depthJoinLevel = 0;

			selectionFeatures = computeSelectionFeatures(queryIndex, starFramework2, level, numberOfStars,
					numberOfCalcNodes, baseFeatures, 0, baseStaticFeatures, null, maxNumberOfSQ, maxNumberOfCalcNodes,
					false);

			expansionFeatures = computeExpansionFeatures(queryIndex, starFramework2, level, numberOfStars,
					numberOfCalcNodes, baseFeatures, selectionFeatures, paSelected, 0, baseStaticFeatures, null,
					maxNumberOfSQ, maxNumberOfCalcNodes, false);

			baseFeatures = baseFeatureFiller(queryIndex, selectionFeatures, starFramework2, numberOfStars,
					numberOfCalcNodes, baseFeatures, 0, 0, maxNumberOfSQ, maxNumberOfCalcNodes);

			TreeNode<CalculationNode> thisCalcNode = starFramework2.calcTreeStarQueriesNodeMapBySQMLIndex
					.get(paSelected); // sq1,
										// sq2,
										// sq3,
										// ...
			thisCalcNode.getData().numberOfPartialAnswersShouldBeFetched = 1;
			starFramework2.anyTimeStarkForLeaf(knowledgeGraph, thisCalcNode, neighborIndexingInstance, cacheServer);
			depthJoinLevel = thisCalcNode.levelInCalcTree - 1;

			for (; depthJoinLevel >= 0; depthJoinLevel--) {
				CalculationTreeSiblingNodes calculationTreeSiblingNodes = starFramework2.joinLevelSiblingNodesMap
						.get(depthJoinLevel);
				starFramework2.anyTimeTwoWayHashJoin(calculationTreeSiblingNodes.leftNode,
						calculationTreeSiblingNodes.rightNode, starFramework2.k);
			}
			depthJoinLevel = 0;

			if (starFramework2.anyTimeAlgorithmShouldFinish())

			{

			}

			selectionFeatures = computeSelectionFeatures(queryIndex, starFramework2, level + stoppingCnt, numberOfStars,
					numberOfCalcNodes, baseFeatures, 0 /* STOP */, baseStaticFeatures, null, maxNumberOfSQ,
					maxNumberOfCalcNodes, false);

			stoppingFeatures = computeSelectionFeatures(queryIndex, starFramework2, level + stoppingCnt, numberOfStars,
					numberOfCalcNodes, baseFeatures, 0 /* STOP */, baseStaticFeatures, bwStoppingFeatures,
					maxNumberOfSQ, maxNumberOfCalcNodes, true);
		}

	}

	private ExpansionFeatures computeExpansionFeatures(int queryIndex, AnyTimeStarFramework starFramework, int level,
			int numberOfStars, int numberOfCalcNodes, BaseFeatures baseFeatures, SelectionFeatures selectionFeatures,
			int paSelected, int expandValue, Features baseStaticFeatures, BufferedWriter bwPAExpansionFeatures,
			int maxNumberOfSQ, int maxNumberOfCalcNodes, boolean shouldPrint) throws Exception {

		int sqIndex = paSelected - 1;

		double currentThisLB = selectionFeatures.lbCurrent;
		double currentThisUB = selectionFeatures.ubCurrent[paSelected];
		// parent in calcNode;
		double currentParentUB = starFramework.calcTreeNodeMap.get(paSelected).getParent().getData().anytimeUpperBound;
		int pqDiffThisFromParent = selectionFeatures.pqDiffThisFromParent[sqIndex];
		int pqDiffThisFromRoot = selectionFeatures.pqDiffThisFromRoot[sqIndex];
		int generateNextBestMatchQueued = selectionFeatures.generateNextBestMatchQueued[sqIndex];

		double ubDifferenceFromCurrentLB = selectionFeatures.ubDifferenceFromCurrentLB[paSelected];
		double ubDifferenceFromParentUB = selectionFeatures.ubDifferenceFromParentUB[paSelected];
		double ubDifferenceFromRootUB = selectionFeatures.ubDifferenceFromRootUB[paSelected];

		double lbDifferenceFromRootLB = selectionFeatures.lbDifferenceFromRootLB;
		double lbDifferenceFromParentLB = selectionFeatures.lbDifferenceFromParentLB;
		int previousPASelected = selectionFeatures.previousPASelected;
		int previousExpansionValue = baseFeatures != null ? baseFeatures.paParentExpansion : 0;
		int howManyTimesSelectedBefore = selectionFeatures.howManyTimesSelectedBefore[sqIndex];
		// int contributionToCurrentAnswer = 0;// TODO:????
		// int sqCalcTreeDepth = selectionFeatures.sqCalcTreeDepth[sqIndex];
		int currentDepth = selectionFeatures.currentDepth[sqIndex];
		boolean isStarkIsEnough = selectionFeatures.isStarkIsEnough[sqIndex];
		int remainingPA = starFramework.calcTreeNodeMap.get(paSelected).data.firstPQItemSize
				- selectionFeatures.currentDepth[sqIndex];

		int currentPQ = selectionFeatures.pqCurrent[sqIndex];

		int searchLevel = level;
		double maxUB = 0d;
		for (int i = 0; i < numberOfCalcNodes; i++) {
			maxUB = Math.max(selectionFeatures.ubCurrent[i], maxUB);
		}
		// getting the (maximum k*ub) - sum(current answers scores)

		double diffMaxPossibleRankCurrentRank = starFramework.k * maxUB;
		double currentRank = 0d;

		for (GraphResult gr : starFramework.anyTimeResults) {
			diffMaxPossibleRankCurrentRank -= gr.anyTimeItemValue;
			currentRank += gr.anyTimeItemValue;
		}

		boolean isPreviouslySelected = false;
		if (selectionFeatures.previousPASelected == paSelected) {
			isPreviouslySelected = true;
		}

		ExpansionFeatures expFeatures = new ExpansionFeatures(queryIndex, currentPQ, currentThisLB, currentThisUB,
				currentParentUB, pqDiffThisFromParent, pqDiffThisFromRoot, generateNextBestMatchQueued,
				ubDifferenceFromCurrentLB, ubDifferenceFromParentUB, ubDifferenceFromRootUB, lbDifferenceFromRootLB,
				lbDifferenceFromParentLB, previousPASelected, howManyTimesSelectedBefore, currentDepth, isStarkIsEnough,
				remainingPA, searchLevel, diffMaxPossibleRankCurrentRank, isPreviouslySelected, maxUB, currentRank,
				previousExpansionValue, expandValue);

		if (shouldPrint) {
			expFeatures.print(baseStaticFeatures, bwPAExpansionFeatures);
			bwPAExpansionFeatures.flush();
		}
		return expFeatures;
	}

	private SelectionFeatures computeSelectionFeatures(int queryIndex, AnyTimeStarFramework starFramework, int level,
			int numberOfStars, int numberOfCalcNodes, BaseFeatures baseFeatures, int paSelected,
			Features baseStaticFeatures, BufferedWriter bwPASelectionFeatures, int maxNumberOfSQ,
			int maxNumberOfCalcNodes, boolean shouldPrint) throws Exception {

		int[] pqCurrent = new int[maxNumberOfSQ];
		double[] ubCurrent = new double[maxNumberOfCalcNodes];
		double lbCurrent;

		int[] pqDiffThisFromParent = new int[maxNumberOfSQ];
		int[] pqDiffThisFromRoot = new int[maxNumberOfSQ];
		int[] generateNextBestMatchQueued = new int[maxNumberOfSQ];
		double[] ubDifferenceFromCurrentLB = new double[maxNumberOfCalcNodes];
		double[] ubDifferenceFromParentUB = new double[maxNumberOfCalcNodes];
		double[] ubDifferenceFromRootUB = new double[maxNumberOfCalcNodes];
		double lbDifferenceFromRootLB = 0d;
		double lbDifferenceFromParentLB = 0d;
		int previousPASelected = 0;
		// int previousExpansionValue = 0;
		// int[] contributionToCurrentAnswer = new int[maxNumberOfSQ];
		// int[] sqCalcTreeDepth = new int[maxNumberOfSQ];
		int[] currentDepth = new int[maxNumberOfSQ];
		int[] remainingPA = new int[maxNumberOfSQ];
		boolean[] isStarkIsEnough = new boolean[maxNumberOfSQ];
		int[] howManySelectedBefore = new int[maxNumberOfSQ];

		lbCurrent = starFramework.leastAnyTimeValueResult;

		if (baseFeatures != null) {
			lbDifferenceFromRootLB = starFramework.leastAnyTimeValueResult - baseFeatures.lbRoot;
			lbDifferenceFromParentLB = starFramework.leastAnyTimeValueResult - baseFeatures.lbParent;
			previousPASelected = baseFeatures.paParentSelected;
			// previousExpansionValue = baseFeatures.paParentExpansion;
		}
		for (int i = 0; i < numberOfStars; i++) {

			for (NodeWithValue nwv : starFramework.currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
					.get(starFramework.starQueries.get(i)).keySet()) {
				generateNextBestMatchQueued[i] += starFramework.currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
						.get(starFramework.starQueries.get(i)).get(nwv).size();
			}

			// sqCalcTreeDepth[i] = starFramework.calcTreeStarQueriesNodeMap
			// .get(starFramework.starQueries.get(i).starQueryCalcNodeIndex).data.levelInCalcTree;

			currentDepth[i] = starFramework.calcTreeStarQueriesNodeMap
					.get(starFramework.starQueries.get(i).starQueryCalcNodeIndex).data.depthOfDigging;

			isStarkIsEnough[i] = starFramework.calcTreeStarQueriesNodeMap
					.get(starFramework.starQueries.get(i).starQueryCalcNodeIndex).data.callStarKIsEnough;

			pqCurrent[i] = starFramework.starkForLeafPQResults.get(starFramework.starQueries.get(i)).size();

			howManySelectedBefore[i] = starFramework.calcTreeStarQueriesNodeMap
					.get(starFramework.starQueries.get(i).starQueryCalcNodeIndex).data.howManyTimesSelectedBefore;

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

		for (int i = numberOfStars; i < maxNumberOfSQ; i++) {
			isStarkIsEnough[i] = true;
			pqDiffThisFromParent[i] = 0;
			pqDiffThisFromRoot[i] = 0;
		}
		for (int i = starFramework.calcTreeNodeMap.size(); i < maxNumberOfCalcNodes; i++) {
			ubCurrent[i] = 0;
			ubDifferenceFromCurrentLB[i] = 0;
			ubDifferenceFromParentUB[i] = 0;
			ubDifferenceFromRootUB[i] = 0;
		}

		SelectionFeatures sfeatures = new SelectionFeatures(queryIndex, pqCurrent, ubCurrent, lbCurrent,
				pqDiffThisFromParent, pqDiffThisFromRoot, generateNextBestMatchQueued, ubDifferenceFromCurrentLB,
				ubDifferenceFromParentUB, ubDifferenceFromRootUB, lbDifferenceFromRootLB, lbDifferenceFromParentLB,
				previousPASelected, howManySelectedBefore, currentDepth, isStarkIsEnough, remainingPA, paSelected);

		if (shouldPrint) {
			sfeatures.print(baseStaticFeatures, bwPASelectionFeatures);
			bwPASelectionFeatures.flush();
		}

		return sfeatures;
	}

	public BaseFeatures baseFeatureFiller(int queryIndex, SelectionFeatures selectionFeatures,
			AnyTimeStarFramework starFramework, int numberOfStars, int numberOfCalcNodes,
			BaseFeatures previousBaseFeatures, int paSelected, int paExpansion, int maxNumberOfSQ,
			int maxNumberOfCalcNodes) {

		int[] pqParent = new int[maxNumberOfSQ];
		int[] pqRoot = new int[maxNumberOfSQ];
		double[] ubParent = new double[maxNumberOfCalcNodes];
		double[] ubRoot = new double[maxNumberOfCalcNodes];
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

	}

	private Features initStaticFeatures(int queryIndex, BufferedWriter bwStaticFeatures,
			AnyTimeStarFramework starFramework, int numberOfQNodes, int numberOfQRelationships, int numberOfStars,
			int maxNumberOfSQ, int maxNumberOfCalcNodes) throws Exception {
		// sumJoinableNodes
		int[] joinableNodes = new int[maxNumberOfSQ];

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

		int[] nodesInStar = new int[maxNumberOfSQ];
		double[] avgPivotDegreeInDataGraph = new double[maxNumberOfSQ];
		int[] estimatedPA = new int[maxNumberOfSQ];
		int[] possiblePivots = new int[maxNumberOfSQ];
		int[] firstPQItemSize = new int[maxNumberOfSQ];

		for (int i = 0; i < numberOfStars; i++) {
			nodesInStar[i] = starFramework2.starQueries.get(i).allStarGraphQueryNodes.size();
			avgPivotDegreeInDataGraph[i] = starFramework2.starQueries.get(i).avgDegreeOfPossiblePivots;
			estimatedPA[i] = starFramework2.starQueries.get(i).numberOfPAEstimate;
			possiblePivots[i] = starFramework2.starQueries.get(i).numberOfPossiblePivots;
			firstPQItemSize[i] = starFramework.calcTreeNodeMap
					.get(starFramework2.starQueries.get(i).starQueryCalcNodeIndex).getData().firstPQItemSize;
		}

		return new Features(numberOfQNodes, numberOfQRelationships, numberOfStars, nodesInStar,
				avgPivotDegreeInDataGraph, estimatedPA, firstPQItemSize, possiblePivots, joinableNodes);

	}

	private void finalizeFeatures(int queryIndex, BufferedWriter bwStaticFeatures, AnyTimeStarFramework starFramework,
			Features staticFeatures, int maxNumberOfSQs) throws Exception {

		TreeNode<CalculationNode> tempNode = starFramework.rootTreeNode;
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

		// anyTimeDepth
		for (GraphResult gr : starFramework2.anyTimeResults) {
			for (Integer starQueryIndex : gr.starQueryIndexDepthMap.keySet()) {
				if (gr.starQueryIndexDepthMap.get(starQueryIndex) > maxAnyTimeAnswerDepthStarQueryMap
						.get(starQueryIndex)) {
					maxAnyTimeAnswerDepthStarQueryMap.put(starQueryIndex,
							gr.starQueryIndexDepthMap.get(starQueryIndex));
				}
			}
		}

		// sumJoinableNodes

		int[] joinableNodes = staticFeatures.joinableNodes;
		int[] nodesInStar = staticFeatures.nodesInStar;
		double[] avgPivotDegreeInDataGraph = staticFeatures.avgPivotDegreeInDataGraph;
		int[] estimatedPA = staticFeatures.estimatedPA;
		int[] possiblePivots = staticFeatures.possiblePivots;
		int[] firstPQItemSize = staticFeatures.firstPQItemSize;
		int numberOfQNodes, numberOfQRelationships, numberOfStars;
		numberOfQNodes = staticFeatures.nodes;
		numberOfQRelationships = staticFeatures.edges;
		numberOfStars = staticFeatures.stars;
		int[] anyTimeDepth = new int[maxNumberOfSQs];
		for (int i = 0; i < numberOfStars; i++) {
			anyTimeDepth[i] = maxAnyTimeAnswerDepthStarQueryMap
					.get(starFramework2.starQueries.get(i).starQueryCalcNodeIndex);
		}

		Features newStaticFeatures = new Features(queryIndex, numberOfQNodes, numberOfQRelationships, numberOfStars,
				nodesInStar, avgPivotDegreeInDataGraph, estimatedPA, firstPQItemSize, possiblePivots, joinableNodes,
				anyTimeDepth);

		newStaticFeatures.printForStatic(bwStaticFeatures);
	}

	private HashSet<Integer> readQueryIndexToBeChecked() throws Exception {
		HashSet<Integer> set = new HashSet<Integer>();

		FileInputStream fis = new FileInputStream(oracleSequenceFile);

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] tokenized = line.split(";");
			if (tokenized.length == 0) {
				continue;
			}

			set.add(Integer.parseInt(tokenized[0]));
			System.out.println("queries to be checked: " + tokenized[0]);
		}

		br.close();
		return set;
	}

	private ArrayList<HashMap<Integer, Integer>> readOracleSequence(int queryIndex, int size) throws Exception {
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

		if (fetchSeqByStarQueryIndex.size() == 0) {
			System.err.println("fetchSeqByStarQueryIndex.size() is zero for queryIndex " + queryIndex);
		}
		return fetchSeqByStarQueryIndex;

	}

	public static void main(String[] args) throws Exception {
		SequenceRunnerForAllSQs beamSearchRunner = new SequenceRunnerForAllSQs();
		beamSearchRunner.initialize(args);
	}

	private AnyTimeStarFramework getNewStarFrameworkInstance(GraphDatabaseService queryGraph,
			GraphDatabaseService knowledgeGraph, int k2, float alpha, Levenshtein levenshtein) {
		cacheServer.clear();
		this.starFramework2 = new AnyTimeStarFramework(queryGraph, knowledgeGraph, k, alpha, levenshtein);
		starFramework2.decomposeQuery(queryGraph, knowledgeGraph, neighborIndexingInstance, cacheServer);

		TreeNode<CalculationNode> tempNode = this.starFramework2.rootTreeNode;
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

		return this.starFramework2;
	}

	public AnyTimeStarFramework getNewStarFrameworkInstance() {
		return getNewStarFrameworkInstance(queryGraph, knowledgeGraph, k, alpha, levenshtein);
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