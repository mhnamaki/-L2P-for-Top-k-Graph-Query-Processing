package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;

import javax.tools.StandardJavaFileManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

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

public class Oracle {
	private static String MODELGRAPH_DB_PATH = "";
	private static String PATTERNGRAPH_DB_PATH = "";

	public static String queryFileName = "";
	public static String queryFileDirectory = "";

	public static String GName = ""; // Yago, DBPedia, ...
	public static int k = 0;
	public static String queryDBInNeo4j = "query.db";
	public static String GDirectory = "";
	public static int numberOfSameExperiment = 4;
	public static HashMap<Integer, Integer> calcTreeNodeStarQueryMaxDepthMap;
	// public static HashMap<Integer, Integer> maxAnswerDepthStarQueryMap;
	public static HashMap<Integer, TreeNode<CalculationNode>> calcTreeNodeMap;
	public static HashMap<Integer, CalculationTreeSiblingNodes> joinLevelSiblingNodesMap;
	// public static StarFramework starFramework2;
	public static int teta = 0;
	public static File foutTime;
	public static BufferedWriter bwTime;

	public static CacheServer cacheServer;
	public static NeighborIndexing neighborIndexingInstance;

	public static void main(String[] args) throws Exception {
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
			}

		}

		if (!GDirectory.endsWith("/")) {
			GDirectory += "/";
		}
		MODELGRAPH_DB_PATH = GDirectory + GName;
		PATTERNGRAPH_DB_PATH = queryFileDirectory + queryDBInNeo4j;

		if (queryFileName.equals("") || GName.equals("") || DummyProperties.similarityThreshold == null) {
			throw new Exception(
					"You should provide all the parameters -queryFileName  -queryFileDirectory  -GName -similarityThreshold -teta -k");
		}

		QueryGenerator queryGenerator = new QueryGenerator(GDirectory + GName);

		// output the
		foutTime = new File("oracle_" + GName + queryFileName + "_timeResults_starFramework.txt");
		FileOutputStream fosTime = new FileOutputStream(foutTime);

		bwTime = new BufferedWriter(new OutputStreamWriter(fosTime));

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
		System.out.println("knowledgeGraphNeighborIndexer finished in " + difference + " miliseconds!");

		float alpha = 0.5F;
		Levenshtein levenshtein = new Levenshtein(nodeLabelsIndex, Dummy.DummyProperties.numberOfPrefixChars);

		List<QueryFromFile> queriesFromFile = queryGenerator
				.getQueryFromFile(queryFileDirectory + queryFileName + ".txt");

		int queryIndex = 0;
		for (QueryFromFile queryFromFile : queriesFromFile) {
			System.gc();
			DummyFunctions.printIfItIsInDebuggedMode("start ConstrucQueryGraph");

			queryIndex++;

			GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH, queryFromFile);
			DummyFunctions.printIfItIsInDebuggedMode("end ConstrucQueryGraph");

			GraphDatabaseService queryGraph = smallGraph;
			registerShutdownHook(queryGraph);

			start_time = System.nanoTime();
			neighborIndexingInstance.queryNeighborIndexer(queryGraph);
			end_time = System.nanoTime();
			difference = (end_time - start_time) / 1e6;
			Dummy.DummyFunctions
					.printIfItIsInDebuggedMode("queryNeighborIndexer finished in " + difference + " miliseconds!");
			boolean theQueryHasAnyAnswer = true;
			cacheServer = new CacheServer();
			int finalDifferenceTimeSF = 0;
			int finalResultSizeTempSF = 0;
			ArrayList<Integer> differenceTimesSF = new ArrayList<Integer>();
			StarFramework starFramework2 = null;
			for (int exp = 0; exp < numberOfSameExperiment && theQueryHasAnyAnswer; exp++) {
				Double diffTime = 0d;
				System.out.println("queryfileName: " + queryFileName + ", queryIndex: " + queryIndex + " k: " + k);

				try (Transaction tx1 = queryGraph.beginTx()) {
					try (Transaction tx2 = knowledgeGraph.beginTx()) {
						cacheServer.clear();
						starFramework2 = new StarFramework(queryGraph, knowledgeGraph, k, alpha, levenshtein);
						starFramework2.decomposeQuery(queryGraph, knowledgeGraph, neighborIndexingInstance,
								cacheServer);

						start_time = System.nanoTime();
						ArrayList<GraphResult> finalResults = starFramework2.starRoundRobinRun(queryGraph,
								knowledgeGraph, neighborIndexingInstance, cacheServer);
						end_time = System.nanoTime();
						diffTime = (end_time - start_time) / 1e6;

						differenceTimesSF.add(diffTime.intValue());

						System.out.println("StarFramework finished in " + diffTime + " miliseconds!");

						finalResultSizeTempSF = finalResults.size();
						tx2.success();

					} catch (Exception exc) {
						System.out.println("queryGraph Transaction failed");
						exc.printStackTrace();
					}

					tx1.success();

				} catch (Exception exc) {
					System.out.println("modelGraph Transaction failed");
					exc.printStackTrace();
				}
			}
			finalDifferenceTimeSF = Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimesSF,
					numberOfSameExperiment);
			System.out.println("avg StarFramework finished in " + finalDifferenceTimeSF + " miliseconds!");
			bwTime.write("StarFramework;" + queryFileName + ";" + GName + ";" + finalDifferenceTimeSF + ";"
					+ finalResultSizeTempSF);
			bwTime.newLine();
			bwTime.flush();

			finalizeMethod(starFramework2);
			// for (Integer index :
			// calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			// System.out.println("maximum depth: " +
			// calcTreeNodeStarQueryMaxDepthMap.get(index));
			// }

			int finalDifferenceTimeBestActions = 0;
			ArrayList<Integer> differenceTimesBestActions = new ArrayList<Integer>();
			for (int exp = 0; exp < numberOfSameExperiment; exp++) {
				try (Transaction tx1 = queryGraph.beginTx()) {
					try (Transaction tx2 = knowledgeGraph.beginTx()) {
						starFramework2 = initialize(queryGraph, knowledgeGraph, k, alpha, levenshtein);
						Double diffTime = bestPossibleActions(starFramework2, knowledgeGraph);
						differenceTimesBestActions.add(diffTime.intValue());
						tx2.success();
					} catch (Exception exc) {
						System.out.println("queryGraph Transaction failed");
						exc.printStackTrace();
					}

					tx1.success();

				} catch (Exception exc) {
					System.out.println("modelGraph Transaction failed");
					exc.printStackTrace();
				}
			}
			finalDifferenceTimeBestActions = Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimesBestActions,
					numberOfSameExperiment);
			System.out.println("BestActions Oracle finished in " + finalDifferenceTimeBestActions + " miliseconds!");
			bwTime.write("BestActions;" + queryFileName + ";" + GName + ";" + finalDifferenceTimeBestActions);
			bwTime.newLine();
			bwTime.flush();

			int finalDifferenceTimeOracle = 0;
			ArrayList<Integer> differenceTimesOracle = new ArrayList<Integer>();
			for (int exp = 0; exp < numberOfSameExperiment; exp++) {
				try (Transaction tx1 = queryGraph.beginTx()) {
					try (Transaction tx2 = knowledgeGraph.beginTx()) {
						starFramework2 = initialize(queryGraph, knowledgeGraph, k, alpha, levenshtein);
						Double diffTime = macroMultiActions(starFramework2, knowledgeGraph, teta);
						differenceTimesOracle.add(diffTime.intValue());
						tx2.success();
					} catch (Exception exc) {
						System.out.println("queryGraph Transaction failed");
						exc.printStackTrace();
					}

					tx1.success();

				} catch (Exception exc) {
					System.out.println("modelGraph Transaction failed");
					exc.printStackTrace();
				}
			}
			finalDifferenceTimeOracle = Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimesOracle,
					numberOfSameExperiment);
			System.out.println("macroMultiActions Oracle finished in " + finalDifferenceTimeOracle + " miliseconds!");
			bwTime.write("macroMultiActions;" + queryFileName + ";" + GName + ";" + finalDifferenceTimeOracle);
			bwTime.newLine();
			bwTime.flush();

			int finalDifferenceTimeOracle2 = 0;
			ArrayList<Integer> differenceTimesOracle2 = new ArrayList<Integer>();
			for (int exp = 0; exp < numberOfSameExperiment; exp++) {
				try (Transaction tx1 = queryGraph.beginTx()) {
					try (Transaction tx2 = knowledgeGraph.beginTx()) {
						starFramework2 = initialize(queryGraph, knowledgeGraph, k, alpha, levenshtein);
						Double diffTime = macroSingleActions(starFramework2, knowledgeGraph, teta);
						differenceTimesOracle2.add(diffTime.intValue());

						tx2.success();
					} catch (Exception exc) {
						System.out.println("queryGraph Transaction failed");
						exc.printStackTrace();
					}

					tx1.success();

				} catch (Exception exc) {
					System.out.println("modelGraph Transaction failed");
					exc.printStackTrace();
				}

			}
			finalDifferenceTimeOracle2 = Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimesOracle2,
					numberOfSameExperiment);
			System.out.println("macroSingleActions Oracle finished in " + finalDifferenceTimeOracle2 + " miliseconds!");
			bwTime.write("macroSingleActions;" + queryFileName + ";" + GName + ";" + finalDifferenceTimeOracle2);
			bwTime.newLine();
			bwTime.flush();
			queryGraph.shutdown();
		}

		bwTime.flush();
		bwTime.close();
		knowledgeGraph.shutdown();
	}

	public static void finalizeMethod(StarFramework starFramework2) {
		TreeNode<CalculationNode> tempNode = starFramework2.rootTreeNode;
		calcTreeNodeStarQueryMaxDepthMap = new HashMap<Integer, Integer>();
		// maxAnswerDepthStarQueryMap = new HashMap<Integer, Integer>();

		if (tempNode.getData().isStarQuery) {
			calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getData().nodeIndex, tempNode.getData().depthOfDigging);
			System.out.println("In SF: depthOfDigging for starQuery with nodeIndex: " + tempNode.getData().nodeIndex
					+ " is " + tempNode.getData().depthOfDigging);
			// maxAnswerDepthStarQueryMap.put(tempNode.getData().nodeIndex, 0);
		}

		while (tempNode != null) {
			if (tempNode.getRightChild() != null && tempNode.getRightChild().getData().isStarQuery) {
				calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getRightChild().getData().nodeIndex,
						tempNode.getRightChild().getData().depthOfDigging);
				// maxAnswerDepthStarQueryMap.put(tempNode.getRightChild().getData().nodeIndex,
				// 0);

				System.out.println("In SF: depthOfDigging for starQuery with nodeIndex: "
						+ tempNode.getRightChild().getData().nodeIndex + " is "
						+ tempNode.getRightChild().getData().depthOfDigging);
			}
			if (tempNode.getLeftChild() != null && tempNode.getLeftChild().getData().isStarQuery) {
				calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getLeftChild().getData().nodeIndex,
						tempNode.getLeftChild().getData().depthOfDigging);
				// maxAnswerDepthStarQueryMap.put(tempNode.getLeftChild().getData().nodeIndex,
				// 0);

				System.out.println("In SF: depthOfDigging for starQuery with nodeIndex: "
						+ tempNode.getLeftChild().getData().nodeIndex + " is "
						+ tempNode.getLeftChild().getData().depthOfDigging);
			}
			tempNode = tempNode.getLeftChild();
		}
	}
	// private static void microSingleAction(GraphDatabaseService
	// knowledgeGraph, int teta) throws Exception {
	//
	//
	// double start_time = System.nanoTime();
	// HashSet<Integer> nodeIndexForLastJoins = new HashSet<Integer>();
	// ArrayList<Integer> nodeIndexForRandomAccess = new ArrayList<Integer>();
	// for (Integer calcNodeStarQueryIndex :
	// calcTreeNodeStarQueryMaxDepthMap.keySet()) {
	// nodeIndexForRandomAccess.add(calcNodeStarQueryIndex);
	// nodeIndexForLastJoins.add(calcNodeStarQueryIndex);
	// }
	// Collections.sort(nodeIndexForRandomAccess, Collections.reverseOrder());
	// int cnt = 0;
	// boolean isFetechedSomething = false;
	// do {
	// int depthJoinLevel = 0;
	// // fetch depth % teta from all node indices in calc tree.
	// Integer calcNodeStarQueryIndex = nodeIndexForRandomAccess.get(cnt %
	// (nodeIndexForRandomAccess.size()));
	// TreeNode<CalculationNode> thisCalcNode =
	// calcTreeNodeMap.get(calcNodeStarQueryIndex);
	// isFetechedSomething = false;
	// if (calcTreeNodeStarQueryMaxDepthMap.get(calcNodeStarQueryIndex) > 0) {
	// thisCalcNode.getData().numberOfPartialAnswersShouldBeFetched = 1;
	// calcTreeNodeStarQueryMaxDepthMap.put(calcNodeStarQueryIndex,
	// calcTreeNodeStarQueryMaxDepthMap.get(calcNodeStarQueryIndex) - 1);
	// starFramework2.starkForLeaf(knowledgeGraph, thisCalcNode);
	// isFetechedSomething = true;
	// depthJoinLevel = Math.max(thisCalcNode.levelInCalcTree - 1,
	// depthJoinLevel);
	//
	// } else {
	// nodeIndexForLastJoins.remove(calcNodeStarQueryIndex);
	// }
	// cnt++;
	// if (isFetechedSomething) {
	// // call join on the depth level.
	// CalculationTreeSiblingNodes calculationTreeSiblingNodes =
	// joinLevelSiblingNodesMap.get(depthJoinLevel);
	// starFramework2.twoWayHashJoin(calculationTreeSiblingNodes.leftNode,
	// calculationTreeSiblingNodes.rightNode, k);
	// } else if (nodeIndexForLastJoins.isEmpty()) {
	// int maxJLevel = 0;
	// for (Integer jLevel : joinLevelSiblingNodesMap.keySet()) {
	// maxJLevel = Math.max(jLevel, maxJLevel);
	// }
	// for (; maxJLevel >= 0; maxJLevel--) {
	// CalculationTreeSiblingNodes calculationTreeSiblingNodes =
	// joinLevelSiblingNodesMap
	// .get(depthJoinLevel);
	// starFramework2.twoWayHashJoin(calculationTreeSiblingNodes.leftNode,
	// calculationTreeSiblingNodes.rightNode, k);
	// }
	// }
	// depthJoinLevel = 0;
	// }
	// // check to see if the algorithm is finished?
	// while (!starFramework2.algorithmShouldFinish());
	//
	// double end_time = System.nanoTime();
	// double difference = (end_time - start_time) / 1e6;
	// System.out.println("microSingleAction Oracle finished in " + difference +
	// "miliseconds!");
	// bwTime.write("microSingleAction;" + queryFileName + ";" + GName + ";" +
	// difference);
	// bwTime.newLine();
	// bwTime.flush();
	// }

	public static Double bestPossibleActions(StarFramework starFramework, GraphDatabaseService knowledgeGraph)
			throws Exception {
		// read the (nodeIndex, depth) pairs.
		HashMap<Integer, Integer> tempCalcTreeNodeStarQueryMaxDepthMap = new HashMap<Integer, Integer>();
		ArrayList<Integer> starQueriesIndices = new ArrayList<Integer>();
		for (Integer index : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			tempCalcTreeNodeStarQueryMaxDepthMap.put(index, calcTreeNodeStarQueryMaxDepthMap.get(index));
			starQueriesIndices.add(index);
		}

		Collections.reverse(starQueriesIndices);

		double start_time = System.nanoTime();
		do {
			int depthJoinLevel = 0;

			// fetch depth % teta from all node indices in calc tree.
			for (Integer calcNodeStarQueryIndex : starQueriesIndices) {
				TreeNode<CalculationNode> thisCalcNode = calcTreeNodeMap.get(calcNodeStarQueryIndex);
				int numberToBeFetched = tempCalcTreeNodeStarQueryMaxDepthMap.get(calcNodeStarQueryIndex);

				if (numberToBeFetched > 0) {
					thisCalcNode.getData().numberOfPartialAnswersShouldBeFetched = numberToBeFetched;
					tempCalcTreeNodeStarQueryMaxDepthMap.put(calcNodeStarQueryIndex,
							tempCalcTreeNodeStarQueryMaxDepthMap.get(calcNodeStarQueryIndex) - numberToBeFetched);
					starFramework.starkForLeaf(knowledgeGraph, thisCalcNode, neighborIndexingInstance, cacheServer);
					depthJoinLevel = Math.max(thisCalcNode.levelInCalcTree - 1, depthJoinLevel);
				}
			}

			// call join on the depth level.
			for (; depthJoinLevel >= 0; depthJoinLevel--) {
				CalculationTreeSiblingNodes calculationTreeSiblingNodes = joinLevelSiblingNodesMap.get(depthJoinLevel);
				starFramework.twoWayHashJoin(calculationTreeSiblingNodes.leftNode,
						calculationTreeSiblingNodes.rightNode, k);
			}

			depthJoinLevel = 0;
		}
		// check to see if the algorithm is finished?
		while (!starFramework.algorithmShouldFinish());

		double end_time = System.nanoTime();
		System.out.println("bestAction");
		// for (Integer calcNodeStarQueryIndex :
		// calcTreeNodeStarQueryMaxDepthMap.keySet()) {
		// System.out.println("macroMultiActions: qIndex:" +
		// calcNodeStarQueryIndex + ", "
		// +
		// calcTreeNodeMap.get(calcNodeStarQueryIndex).getData().depthOfDigging);
		// }
		return (end_time - start_time) / 1e6;

	}

	public static Double macroMultiActions(StarFramework starFramework, GraphDatabaseService knowledgeGraph, int teta)
			throws Exception {
		// read the (nodeIndex, depth) pairs.
		HashMap<Integer, Integer> tempCalcTreeNodeStarQueryMaxDepthMap = new HashMap<Integer, Integer>();
		ArrayList<Integer> starQueriesIndices = new ArrayList<Integer>();
		for (Integer index : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			tempCalcTreeNodeStarQueryMaxDepthMap.put(index, calcTreeNodeStarQueryMaxDepthMap.get(index));
			starQueriesIndices.add(index);
		}

		Collections.reverse(starQueriesIndices);

		double start_time = System.nanoTime();
		do {
			int depthJoinLevel = 0;

			// fetch depth % teta from all node indices in calc tree.
			for (Integer calcNodeStarQueryIndex : starQueriesIndices) {
				TreeNode<CalculationNode> thisCalcNode = calcTreeNodeMap.get(calcNodeStarQueryIndex);
				int numberToBeFetched = Math.min(tempCalcTreeNodeStarQueryMaxDepthMap.get(calcNodeStarQueryIndex),
						teta);

				if (numberToBeFetched > 0) {
					thisCalcNode.getData().numberOfPartialAnswersShouldBeFetched = numberToBeFetched;
					tempCalcTreeNodeStarQueryMaxDepthMap.put(calcNodeStarQueryIndex,
							tempCalcTreeNodeStarQueryMaxDepthMap.get(calcNodeStarQueryIndex) - numberToBeFetched);
					starFramework.starkForLeaf(knowledgeGraph, thisCalcNode, neighborIndexingInstance, cacheServer);

					depthJoinLevel = Math.max(thisCalcNode.levelInCalcTree - 1, depthJoinLevel);

				}
			}

			// call join on the depth level.
			for (; depthJoinLevel >= 0; depthJoinLevel--) {
				CalculationTreeSiblingNodes calculationTreeSiblingNodes = joinLevelSiblingNodesMap.get(depthJoinLevel);
				starFramework.twoWayHashJoin(calculationTreeSiblingNodes.leftNode,
						calculationTreeSiblingNodes.rightNode, k);
			}

			depthJoinLevel = 0;
		}
		// check to see if the algorithm is finished?
		while (!starFramework.algorithmShouldFinish());

		double end_time = System.nanoTime();

		System.out.println("macroMulti");
		// for (Integer calcNodeStarQueryIndex :
		// calcTreeNodeStarQueryMaxDepthMap.keySet()) {
		// System.out.println("macroMultiActions: qIndex:" +
		// calcNodeStarQueryIndex + ", "
		// +
		// calcTreeNodeMap.get(calcNodeStarQueryIndex).getData().depthOfDigging);
		// }
		return (end_time - start_time) / 1e6;

	}

	public static Double macroSingleActions(StarFramework starFramework2, GraphDatabaseService knowledgeGraph,
			int teta) throws Exception {
		// read the (nodeIndex, depth) pairs.

		HashMap<Integer, Integer> tempCalcTreeNodeStarQueryMaxDepthMap = new HashMap<Integer, Integer>();
		for (Integer index : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			tempCalcTreeNodeStarQueryMaxDepthMap.put(index, calcTreeNodeStarQueryMaxDepthMap.get(index));
		}

		double start_time = System.nanoTime();
		do {
			int depthJoinLevel = 0;
			boolean isFetechedSomething = false;
			// fetch depth % teta from all node indices in calc tree.
			int maxRemainingNodeIndex = 0;
			for (Integer calcNodeStarQueryIndex : tempCalcTreeNodeStarQueryMaxDepthMap.keySet()) {
				int numberToBeFetched = Math.min(tempCalcTreeNodeStarQueryMaxDepthMap.get(calcNodeStarQueryIndex),
						teta);
				if (numberToBeFetched > 0) {
					maxRemainingNodeIndex = Math.max(calcNodeStarQueryIndex, maxRemainingNodeIndex);
				}
			}
			TreeNode<CalculationNode> thisCalcNode = calcTreeNodeMap.get(maxRemainingNodeIndex);
			int numberToBeFetched = Math.min(tempCalcTreeNodeStarQueryMaxDepthMap.get(maxRemainingNodeIndex), teta);
			if (numberToBeFetched > 0) {
				thisCalcNode.getData().numberOfPartialAnswersShouldBeFetched = numberToBeFetched;
				tempCalcTreeNodeStarQueryMaxDepthMap.put(maxRemainingNodeIndex,
						tempCalcTreeNodeStarQueryMaxDepthMap.get(maxRemainingNodeIndex) - numberToBeFetched);
				starFramework2.starkForLeaf(knowledgeGraph, thisCalcNode, neighborIndexingInstance, cacheServer);

				depthJoinLevel = thisCalcNode.levelInCalcTree - 1;
				isFetechedSomething = true;
			}
			// call join on the depth level.
			for (; depthJoinLevel >= 0; depthJoinLevel--) {
				CalculationTreeSiblingNodes calculationTreeSiblingNodes = joinLevelSiblingNodesMap.get(depthJoinLevel);
				starFramework2.twoWayHashJoin(calculationTreeSiblingNodes.leftNode,
						calculationTreeSiblingNodes.rightNode, k);
			}
			depthJoinLevel = 0;
		}
		// check to see if the algorithm is finished?
		while (!starFramework2.algorithmShouldFinish());

		double end_time = System.nanoTime();
		System.out.println("macroSingle");
		// for (Integer calcNodeStarQueryIndex :
		// calcTreeNodeStarQueryMaxDepthMap.keySet()) {
		// System.out.println("macroSingleActions: qIndex:" +
		// calcNodeStarQueryIndex + ", "
		// +
		// calcTreeNodeMap.get(calcNodeStarQueryIndex).getData().depthOfDigging);
		// }
		return (end_time - start_time) / 1e6;

	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	private static StarFramework initialize(GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph,
			int k, float alpha, Levenshtein levenshtein) {
		cacheServer.clear();
		StarFramework starFramework2 = new StarFramework(queryGraph, knowledgeGraph, k, alpha, levenshtein);
		starFramework2.decomposeQuery(queryGraph, knowledgeGraph, neighborIndexingInstance, cacheServer);

		calcTreeNodeMap = new HashMap<Integer, TreeNode<CalculationNode>>();
		joinLevelSiblingNodesMap = new HashMap<Integer, CalculationTreeSiblingNodes>();
		TreeNode<CalculationNode> tempNode = starFramework2.rootTreeNode;

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

		return starFramework2;

	}

}
