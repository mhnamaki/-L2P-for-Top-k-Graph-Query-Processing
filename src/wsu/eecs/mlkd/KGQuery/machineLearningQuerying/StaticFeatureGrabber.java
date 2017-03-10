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

public class StaticFeatureGrabber {
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
	private String queryInfoAddress;
	private String queryDepthAddress;

	public void initialize(String[] args) throws Exception {
		int numberOfPrefixChars = 0;
		// int delta = 0;
		// int beamSize = 0;

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

			} else if (args[i].equals("-queryInfoAddress")) {
				queryInfoAddress = args[++i];

			} else if (args[i].equals("-similarityThreshold")) {
				DummyProperties.similarityThreshold = Float.parseFloat(args[++i]);
			} else if (args[i].equals("-k")) {
				k = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-numberOfPrefixChars")) {
				numberOfPrefixChars = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-startingQueryIndex")) {
				startingQueryIndex = Integer.parseInt(args[++i]);
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

		knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(MODELGRAPH_DB_PATH);
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

		ArrayList<QueryInfo> queriesInfo = QueryInfo.queryInfoRead(queryInfoAddress);
		HashSet<Integer> queriesToBeChecked = new HashSet<Integer>();
		for (QueryInfo qi : queriesInfo) {
			queriesToBeChecked.add(qi.queryId);
		}

		// ArrayList<QueryDepth> queriesDepth =
		// QueryDepth.queryDepthRead(queryDepthAddress);

		for (File file : fileInTheDirfinder(queryFileDirectory)) {
			queryFileName = file.getName();

			List<QueryFromFile> queriesFromFile = queryGenerator.getQueryFromFile(file.getAbsolutePath());

			int queryIndex = 0;
			for (QueryFromFile queryFromFile : queriesFromFile) {

				DummyFunctions.printIfItIsInDebuggedMode("start ConstrucQueryGraph");

				queryIndex = queryFromFile.queryIndex;
				if (queryIndex < startingQueryIndex) {
					continue;
				}
				if (!queriesToBeChecked.contains(queryIndex)) {
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
						Double finalDifferenceTime = 0d;
						ArrayList<Double> differenceTimes = new ArrayList<Double>();

						// for (int exp = 0; exp < numberOfSameExperiment;
						// exp++) {
						starFramework2 = getNewStarFrameworkInstance();

						int numberOfStars = starFramework2.starQueries.size();
						int numberOfCalcNodes = starFramework2.calcTreeNodeMap.size();

						Features baseStaticFeatures = null;

						// reading the oracle sequences

						ArrayList<Integer> queryIndices = new ArrayList<Integer>();
						for (Integer qIndex : starFramework2.calcTreeStarQueriesNodeMap.keySet()) {
							queryIndices.add(starFramework2.calcTreeStarQueriesNodeMap.get(qIndex).data.nodeIndex);
						}

						boolean shouldFinish = false;

						start_time = System.nanoTime();

						int cnt = 0;

						while (!shouldFinish) {
							int queryIndexToBeFetched = queryIndices.get(cnt % queryIndices.size());
							int depthJoinLevel = 0;

							TreeNode<CalculationNode> thisCalcNode = starFramework2.calcTreeNodeMap
									.get(queryIndexToBeFetched);

							thisCalcNode.getData().numberOfPartialAnswersShouldBeFetched = 1;

							starFramework2.anyTimeStarkForLeaf(knowledgeGraph, thisCalcNode, neighborIndexingInstance,
									cacheServer);
							depthJoinLevel = thisCalcNode.levelInCalcTree - 1;

							for (; depthJoinLevel >= 0; depthJoinLevel--) {
								CalculationTreeSiblingNodes calculationTreeSiblingNodes = starFramework2.joinLevelSiblingNodesMap
										.get(depthJoinLevel);
								starFramework2.anyTimeTwoWayHashJoin(calculationTreeSiblingNodes.leftNode,
										calculationTreeSiblingNodes.rightNode, starFramework2.k);
							}
							depthJoinLevel = 0;

							if (cnt == (numberOfStars - 1)) {
								baseStaticFeatures = initStaticFeatures(queryIndex, bwStaticFeatures[numberOfStars - 2],
										starFramework2, numberOfQNodes, numberOfQRelationships, numberOfStars);
								break;
							}
							cnt++;

							if (starFramework2.anyTimeAlgorithmShouldFinish() || queryIndices.size() == 0) {
								shouldFinish = true;
							}
						}

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
			bwStaticFeatures[i].flush();
			bwStaticFeatures[i].close();
		}

		knowledgeGraph.shutdown();

		System.out.println("program is finished properly!");

	}

	private HashMap<Integer, Integer> getQueriesDepth(int queryIndex, ArrayList<QueryDepth> queriesDepth) {
		for (QueryDepth qd : queriesDepth) {
			if (qd.queryId == queryIndex) {
				return qd.queryIndexDepthMap;
			}
		}
		return null;
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

	private ArrayList<HashMap<Integer, Integer>> readOracleDepth(int queryIndex, int size) throws Exception {
		ArrayList<HashMap<Integer, Integer>> fetchSeqByStarQueryIndex = new ArrayList<HashMap<Integer, Integer>>();

		FileInputStream fis = new FileInputStream("sequences_" + size + ".txt");

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
		StaticFeatureGrabber staticfeaturegrabber = new StaticFeatureGrabber();
		staticfeaturegrabber.initialize(args);
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