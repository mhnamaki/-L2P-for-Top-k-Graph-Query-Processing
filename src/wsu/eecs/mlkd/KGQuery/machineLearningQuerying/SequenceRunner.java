package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.tooling.GlobalGraphOperations;

import com.fasterxml.jackson.annotation.JsonFormat.Feature;

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
import wsu.eecs.mlkd.KGQuery.TopKQuery.StarFramework;
import wsu.eecs.mlkd.KGQuery.TopKQuery.TreeNode;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;
import wsu.eecs.mlkd.KGQuery.test.QueryFromFile;
import wsu.eecs.mlkd.KGQuery.test.QueryGenerator;
import wsu.eecs.mlkd.KGQuery.test.StarFrameworkExperimenter;

public class SequenceRunner {
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
		// int delta = 0;
		// int beamSize = 0;
		WhichOracle oracle = WhichOracle.macroSingleAction;

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
			// else if (args[i].equals("-delta")) {
			// delta = Integer.parseInt(args[++i]);
			// }
			// else if (args[i].equals("-beamSize")) {
			// beamSize = Integer.parseInt(args[++i]);
			// }
			else if (args[i].equals("-whichOracle")) {
				String tempOracle = args[++i];
				switch (tempOracle) {
				case "microSingleAction":
					oracle = WhichOracle.microSingleAction;
					break;
				case "macroSingleAction":
					oracle = WhichOracle.macroSingleAction;
					break;
				case "macroMultiAction":
					oracle = WhichOracle.macroMultiAction;
					break;

				default:
					break;
				}
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

		File[] foutStaticFeatures = new File[5];
		FileOutputStream[] fosStaticFeatures = new FileOutputStream[5];
		BufferedWriter[] bwStaticFeatures = new BufferedWriter[5];
		String firstLine = "";

		for (int i = 0; i < 5; i++) {
			int numberOfSq = i + 2;
			foutStaticFeatures[i] = new File("staticFeaturesFor_" + numberOfSq + ".txt");
			fosStaticFeatures[i] = new FileOutputStream(foutStaticFeatures[i], true);
			bwStaticFeatures[i] = new BufferedWriter(new OutputStreamWriter(fosStaticFeatures[i]));

			if (startingQueryIndex == 0) {
				firstLine = "queryIndex,nodes,edges,stars,";
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "nodesInStar" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "avgPivotDegreeInDataGraph" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "estimatedPA" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "firstPQItemSize" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "possiblePivots" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "joinableNodes" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "anyTimeDepth" + m + ",";
				}

				bwStaticFeatures[i].write(firstLine);
				bwStaticFeatures[i].newLine();

			}
		}

		File[] foutPASelectionFeatures = new File[5];
		FileOutputStream[] fosPASelectionFeatures = new FileOutputStream[5];
		BufferedWriter[] bwPASelectionFeatures = new BufferedWriter[5];

		File[] foutStoppingFeatures = new File[5];
		FileOutputStream[] fosStoppingFeatures = new FileOutputStream[5];
		BufferedWriter[] bwStoppingFeatures = new BufferedWriter[5];

		for (int i = 0; i < 5; i++) {
			int numberOfSq = i + 2;
			foutPASelectionFeatures[i] = new File("paSelectionFeaturesFor_" + numberOfSq + ".txt");
			fosPASelectionFeatures[i] = new FileOutputStream(foutPASelectionFeatures[i], true);
			bwPASelectionFeatures[i] = new BufferedWriter(new OutputStreamWriter(fosPASelectionFeatures[i]));

			foutStoppingFeatures[i] = new File("stoppingFeaturesFor_" + numberOfSq + ".txt");
			fosStoppingFeatures[i] = new FileOutputStream(foutStoppingFeatures[i], true);
			bwStoppingFeatures[i] = new BufferedWriter(new OutputStreamWriter(fosStoppingFeatures[i]));

			if (startingQueryIndex == 0) {
				firstLine = "queryIndex,nodes,edges,stars,";
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "nodesInStar" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "avgPivotDegreeInDataGraph" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "estimatedPA" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "firstPQItemSize" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "possiblePivots" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "joinableNodes" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "pqCurrent" + m + ",";
				}
				int calcNodes = 0;
				if (numberOfSq == 2) {
					calcNodes = 3;
				} else {
					calcNodes = numberOfSq + 2;
				}
				for (int m = 1; m <= calcNodes; m++) {
					firstLine += "ubCurrent" + m + ",";
				}
				firstLine += "lbCurrent,";

				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "pqDiffThisFromParent" + m + ",";
				}

				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "pqDiffThisFromRoot" + m + ",";
				}

				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "generateNextBestMatchQueued" + m + ",";
				}

				for (int m = 1; m <= calcNodes; m++) {
					firstLine += "ubDifferenceFromCurrentLB" + m + ",";
				}

				for (int m = 1; m <= calcNodes; m++) {
					firstLine += "ubDifferenceFromParentUB" + m + ",";
				}

				for (int m = 1; m <= calcNodes; m++) {
					firstLine += "ubDifferenceFromRootUB" + m + ",";
				}

				firstLine += "lbDifferenceFromRootLB,";
				firstLine += "lbDifferenceFromParentLB,";

				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "howManyTimesSelectedBefore" + m + ",";
				}

				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "contributionToCurrentAnswer" + m + ",";
				}

				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "sqCalcTreeDepth" + m + ",";
				}

				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "currentDepth" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "isStarkIsEnough" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "remainingPA" + m + ",";
				}

				firstLine += "previousPASelected" + ",";

				firstLine += "starQuerySelectedIndex" + ",";

				bwPASelectionFeatures[i].write(firstLine);
				bwPASelectionFeatures[i].newLine();

				bwStoppingFeatures[i].write(firstLine);
				bwStoppingFeatures[i].newLine();

			}

		}

		File[] foutPAExpansionFeatures = new File[5];
		FileOutputStream[] fosPAExpansionFeatures = new FileOutputStream[5];
		BufferedWriter[] bwPAExpansionFeatures = new BufferedWriter[5];

		for (int i = 0; i < 5; i++) {
			int numberOfSq = i + 2;
			foutPAExpansionFeatures[i] = new File("paExpansionFeaturesFor_" + numberOfSq + ".txt");
			fosPAExpansionFeatures[i] = new FileOutputStream(foutPAExpansionFeatures[i], true);
			bwPAExpansionFeatures[i] = new BufferedWriter(new OutputStreamWriter(fosPAExpansionFeatures[i]));

			if (startingQueryIndex == 0) {
				firstLine = "queryIndex,nodes,edges,stars,";
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "nodesInStar" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "avgPivotDegreeInDataGraph" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "estimatedPA" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "firstPQItemSize" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "possiblePivots" + m + ",";
				}
				for (int m = 1; m <= numberOfSq; m++) {
					firstLine += "joinableNodes" + m + ",";
				}
				firstLine += "currentPQ,currentThisLB,currentThisUB,currentParentUB,pqDiffThisFromParent,pqDiffThisFromRoot,"
						+ "generateNextBestMatchQueued,ubDifferenceFromCurrentLB,ubDifferenceFromParentUB,ubDifferenceFromRootUB,"
						+ "lbDifferenceFromRootLB,lbDifferenceFromParentLB,howManyTimesSelectedBefore,contributionToCurrentAnswer,"
						+ "sqCalcTreeDepth,currentDepth,isStarkIsEnough,remainingPA,searchLevel,diffMaxPossibleRankCurrentRank,"
						+ "isPreviouslySelected,maxUB,currentRank,previousPASelected,paSelected,previousExpansionValue,expandValue";
			}

			bwPAExpansionFeatures[i].write(firstLine);
			bwPAExpansionFeatures[i].newLine();
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

		HashSet<Integer> queriesSet = readQueryIndexToBeChecked();

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

				if (!queriesSet.contains(queryIndex)) {
					continue;
				}

				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH,
						queryFromFile);
				DummyFunctions.printIfItIsInDebuggedMode("end ConstrucQueryGraph");

				queryGraph = smallGraph;
				registerShutdownHook(queryGraph);

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
						Double finalDifferenceTime = 0d;
						ArrayList<Double> differenceTimes = new ArrayList<Double>();

						// for (int exp = 0; exp < numberOfSameExperiment;
						// exp++) {
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
						boolean shouldFinish = false;

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
										bwPASelectionFeatures[numberOfStars - 2]);

								expansionFeatures = computeExpansionFeatures(queryIndex, starFramework2, level,
										numberOfStars, numberOfCalcNodes, baseFeatures, selectionFeatures, paSelected,
										fetchByQueryIndex.get(paSelected), baseStaticFeatures,
										bwPAExpansionFeatures[numberOfStars - 2]);

							}

							// TODO: for multi-action?
							if (level >= numberOfStars) {
								baseFeatures = baseFeatureFiller(queryIndex, selectionFeatures, starFramework2,
										numberOfStars, numberOfCalcNodes, baseFeatures, paSelected,
										fetchByQueryIndex.get(paSelected));
							}
							for (Integer queryIndexToBeFetched : fetchByQueryIndex.keySet()) {

								TreeNode<CalculationNode> thisCalcNode = starFramework2.calcTreeNodeMap
										.get(queryIndexToBeFetched);
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
								baseStaticFeatures = initStaticFeatures(queryIndex, bwStaticFeatures[numberOfStars - 2],
										starFramework2, numberOfQNodes, numberOfQRelationships, numberOfStars);
							}

						}

						if (starFramework2.anyTimeAlgorithmShouldFinish()) {
							shouldFinish = true;
						}

						selectionFeatures = computeSelectionFeatures(queryIndex, starFramework2,
								fetchSeqByStarQueryIndex.size(), numberOfStars, numberOfCalcNodes, baseFeatures,
								0 /* STOP */, baseStaticFeatures, bwPASelectionFeatures[numberOfStars - 2]);

						// expansionFeatures =
						// computeExpansionFeatures(queryIndex, starFramework2,
						// fetchSeqByStarQueryIndex.size(), numberOfStars,
						// numberOfCalcNodes, baseFeatures,
						// selectionFeatures, 0 /* STOP */, 0 /* STOP */,
						// baseStaticFeatures,
						// bwPAExpansionFeatures[numberOfStars - 2]);

						stoppingFeatures = computeSelectionFeatures(queryIndex, starFramework2,
								fetchSeqByStarQueryIndex.size(), numberOfStars, numberOfCalcNodes, baseFeatures,
								0 /* STOP */, baseStaticFeatures, bwStoppingFeatures[numberOfStars - 2]);

						// end_time = System.nanoTime();

						// diffTime = (end_time - start_time) / 1e6;
						// System.out.println("StarFramework exp: " + exp + " is
						// finished in " + diffTime.intValue()
						// + " miliseconds!");

						// differenceTimes.add(diffTime);

						// if (exp != (numberOfSameExperiment - 1)) {
						// starFramework2 = null;
						// } else {
						finalizeFeatures(queryIndex, bwStaticFeatures[numberOfStars - 2], starFramework2,
								baseStaticFeatures);
						starFramework2 = null;
						// }
						// }
						System.gc();
						System.runFinalization();

						// finalDifferenceTime =
						// Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimes,
						// numberOfSameExperiment);
						// System.out.println("StarFramework avg is finished in
						// " + finalDifferenceTime + " miliseconds!");

						// bwTimeSF.newLine();
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

		for (int i = 0; i < 5; i++) {
			bwPASelectionFeatures[i].flush();
			bwPASelectionFeatures[i].close();
			bwStaticFeatures[i].flush();
			bwStaticFeatures[i].close();
			bwPAExpansionFeatures[i].flush();
			bwPAExpansionFeatures[i].close();
		}

		knowledgeGraph.shutdown();

		System.out.println("program is finished properly!");

	}

	private ExpansionFeatures computeExpansionFeatures(int queryIndex, AnyTimeStarFramework starFramework, int level,
			int numberOfStars, int numberOfCalcNodes, BaseFeatures baseFeatures, SelectionFeatures selectionFeatures,
			int paSelected, int expandValue, Features baseStaticFeatures, BufferedWriter bwPAExpansionFeatures)
			throws Exception {

		int sqIndex = starFramework.calcTreeStarQueriesNodeMap.get(paSelected).data.starQuery.starQueryIndex;

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
		int contributionToCurrentAnswer = 0;// TODO:????
		int sqCalcTreeDepth = selectionFeatures.sqCalcTreeDepth[sqIndex];
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
				lbDifferenceFromParentLB, previousPASelected, howManyTimesSelectedBefore, contributionToCurrentAnswer,
				sqCalcTreeDepth, currentDepth, isStarkIsEnough, remainingPA, searchLevel,
				diffMaxPossibleRankCurrentRank, isPreviouslySelected, maxUB, currentRank, paSelected,
				previousExpansionValue, expandValue);
		expFeatures.print(baseStaticFeatures, bwPAExpansionFeatures);
		return expFeatures;
	}

	private SelectionFeatures computeSelectionFeatures(int queryIndex, AnyTimeStarFramework starFramework, int level,
			int numberOfStars, int numberOfCalcNodes, BaseFeatures baseFeatures, int paSelected,
			Features baseStaticFeatures, BufferedWriter bwPASelectionFeatures) throws Exception {

		int[] pqCurrent = new int[numberOfStars];
		double[] ubCurrent = new double[numberOfCalcNodes];
		double lbCurrent;

		int[] pqDiffThisFromParent = new int[numberOfStars];
		int[] pqDiffThisFromRoot = new int[numberOfStars];
		int[] generateNextBestMatchQueued = new int[numberOfStars];
		double[] ubDifferenceFromCurrentLB = new double[numberOfCalcNodes];
		double[] ubDifferenceFromParentUB = new double[numberOfCalcNodes];
		double[] ubDifferenceFromRootUB = new double[numberOfCalcNodes];
		double lbDifferenceFromRootLB = 0d;
		double lbDifferenceFromParentLB = 0d;
		int previousPASelected = 0;
		int previousExpansionValue = 0;
		int[] contributionToCurrentAnswer = new int[numberOfStars];
		int[] sqCalcTreeDepth = new int[numberOfStars];
		int[] currentDepth = new int[numberOfStars];
		int[] remainingPA = new int[numberOfStars];
		boolean[] isStarkIsEnough = new boolean[numberOfStars];
		int[] howManySelectedBefore = new int[numberOfStars];

		lbCurrent = starFramework.leastAnyTimeValueResult;

		if (baseFeatures != null) {
			lbDifferenceFromRootLB = starFramework.leastAnyTimeValueResult - baseFeatures.lbRoot;
			lbDifferenceFromParentLB = starFramework.leastAnyTimeValueResult - baseFeatures.lbParent;
			previousPASelected = baseFeatures.paParentSelected;
			previousExpansionValue = baseFeatures.paParentExpansion;
		}
		for (int i = 0; i < numberOfStars; i++) {

			for (NodeWithValue nwv : starFramework.currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
					.get(starFramework.starQueries.get(i)).keySet()) {
				generateNextBestMatchQueued[i] += starFramework.currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
						.get(starFramework.starQueries.get(i)).get(nwv).size();
			}

			sqCalcTreeDepth[i] = starFramework.calcTreeStarQueriesNodeMap
					.get(starFramework.starQueries.get(i).starQueryCalcNodeIndex).data.levelInCalcTree;

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

		SelectionFeatures sfeatures = new SelectionFeatures(queryIndex, pqCurrent, ubCurrent, lbCurrent,
				pqDiffThisFromParent, pqDiffThisFromRoot, generateNextBestMatchQueued, ubDifferenceFromCurrentLB,
				ubDifferenceFromParentUB, ubDifferenceFromRootUB, lbDifferenceFromRootLB, lbDifferenceFromParentLB,
				previousPASelected, howManySelectedBefore, contributionToCurrentAnswer, sqCalcTreeDepth, currentDepth,
				isStarkIsEnough, remainingPA, paSelected);

		sfeatures.print(baseStaticFeatures, bwPASelectionFeatures);

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

	}

	private Features initStaticFeatures(int queryIndex, BufferedWriter bwStaticFeatures,
			AnyTimeStarFramework starFramework, int numberOfQNodes, int numberOfQRelationships, int numberOfStars)
			throws Exception {
		// sumJoinableNodes
		int[] joinableNodes = new int[numberOfStars];

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

		int[] nodesInStar = new int[numberOfStars];
		double[] avgPivotDegreeInDataGraph = new double[numberOfStars];
		int[] estimatedPA = new int[numberOfStars];
		int[] possiblePivots = new int[numberOfStars];
		int[] firstPQItemSize = new int[numberOfStars];

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
			Features staticFeatures) throws Exception {

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
		int[] anyTimeDepth = new int[numberOfStars];
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

		return fetchSeqByStarQueryIndex;

	}

	public static void main(String[] args) throws Exception {
		SequenceRunner beamSearchRunner = new SequenceRunner();
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

	public static void finalizeMethod(AnyTimeStarFramework starFramework2, BufferedWriter bwTime, String prefix,
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

		for (Integer starQueryIndex : maxAnswerDepthStarQueryMap.keySet()) {
			prefix += "(" + starQueryIndex + ":" + maxAnswerDepthStarQueryMap.get(starQueryIndex) + ")";
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
			System.out.println("In Anytime Answers: depthOfDigging for starQuery with nodeIndex: " + starQueryIndex
					+ " is " + maxAnyTimeAnswerDepthStarQueryMap.get(starQueryIndex));
		}

		if (bwTime != null) {
			bwTime.write(prefix);
			bwTime.newLine();
			bwTime.flush();
		}

	}

	public static File[] fileInTheDirfinder(String dirName) {
		File dir = new File(dirName);

		return dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String filename) {
				return filename.endsWith(".txt");
			}
		});

	}
}