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

public class BeamSearchRunner {
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
	public int startingQueryIndex = 0;
	public int endingQueryIndex = 0;
	public String queryInfoAddress;
	int totalAnswersDepth = 0;
	int totalSFDepth = 0;
	private String queriesFoldPath;
	private int foldStartFrom = -1;
	private int foldEndTo = -1;
	private double rankCoeff = 1;
	private double timeCoeff = 1;
	private double depthCoeff = 1;

	private double totalQueriesTimeByOracle = 0d;
	private double totalQueriesTimeBySF = 0d;

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
			} else if (args[i].equals("-queryInfoAddress")) {
				queryInfoAddress = args[++i];
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
			} else if (args[i].equals("-whichOracle")) {
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
			} else if (args[i].equals("-foldStartFrom")) {
				foldStartFrom = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-foldEndTo")) {
				foldEndTo = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-queriesFoldPath")) {
				queriesFoldPath = args[++i];
			} else if (args[i].equals("-rankCoeff")) {
				rankCoeff = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-timeCoeff")) {
				timeCoeff = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-depthCoeff")) {
				depthCoeff = Double.parseDouble(args[++i]);
			}

		}

		cacheServer = new CacheServer();

		// beamSizes initialize
		ArrayList<Integer> beamSizes = new ArrayList<Integer>();
		beamSizes.add(1);
		// beamSizes.add(2);
		// beamSizes.add(5);
		// beamSizes.add(10);
		// beamSizes.add(20);
		// beamSizes.add(50);

		// delta initialize
		ArrayList<Integer> deltas = new ArrayList<Integer>();
		// deltas.add(10);
		// deltas.add(20);
		// deltas.add(10);
		deltas.add(20);
		// deltas.add(30);
		// deltas.add(500);

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

		// output the SF versions
		File foutTimeSF = new File(queryFileName + "_" + GName + "_timeResults_SF.txt");
		FileOutputStream fosTimeSF = new FileOutputStream(foutTimeSF, true);

		BufferedWriter bwTimeSF = new BufferedWriter(new OutputStreamWriter(fosTimeSF));

		// File foutAnswersSF = new File(GName + queryFileName +
		// "anytime_answerResults_SF.txt");
		// FileOutputStream fosAnswersSF = new FileOutputStream(foutAnswersSF);
		//
		// BufferedWriter bwAnswersSF = new BufferedWriter(new
		// OutputStreamWriter(fosAnswersSF));

		// output beamSearch with different beamSizes and delta's
		File foutTimeBeamSearch = new File(queryFileName + "_" + GName + "_timeResults_BeamSearch.txt");
		FileOutputStream fosTimeBeamSearch = new FileOutputStream(foutTimeBeamSearch, true);

		BufferedWriter bwTimeBeamSearch = new BufferedWriter(new OutputStreamWriter(fosTimeBeamSearch));

		// File foutAnswersBeamSearch = new File(GName + queryFileName +
		// "_answerResults_BeamSearch.txt");
		// FileOutputStream fosAnswersBeamSearch = new
		// FileOutputStream(foutAnswersBeamSearch);
		//
		// BufferedWriter bwAnswersBeamSearch = new BufferedWriter(new
		// OutputStreamWriter(fosAnswersBeamSearch));

		CommonFunctions cfunction = new CommonFunctions();

		File timeByOracleFile = new File("totalQueriesTimeByOracle.txt");
		FileOutputStream fosTimeByOracle = new FileOutputStream(timeByOracleFile);
		BufferedWriter bwTimeByOracle = new BufferedWriter(new OutputStreamWriter(fosTimeByOracle));

		File timeBySFAndOracleFile = new File("totalQueriesTimeBySFAndOracle.txt");
		FileOutputStream fosTimeBySFAndOracle = new FileOutputStream(timeBySFAndOracleFile, true);
		BufferedWriter bwTimeBySFAndOracle = new BufferedWriter(new OutputStreamWriter(fosTimeBySFAndOracle));

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
						Double finalDifferenceTime = 0d;
						ArrayList<Double> differenceTimes = new ArrayList<Double>();

						InfoHolder[] infoHolders = new InfoHolder[numberOfSameExperiment];
						for (int i = 0; i < infoHolders.length; i++) {
							infoHolders[i] = new InfoHolder();
						}

						// ArrayList<GraphResult> finalResults = null;
						for (int exp = 0; exp < numberOfSameExperiment; exp++) {
							starFramework2 = getNewStarFrameworkInstance();
							infoHolders[exp].queryIndex = queryIndex;
							start_time = System.nanoTime();
							starFramework2.starRoundRobinRun(queryGraph, knowledgeGraph, neighborIndexingInstance,
									cacheServer, infoHolders[exp]);

							end_time = System.nanoTime();
							diffTime = (end_time - start_time) / 1e6;
							System.out.println("StarFramework exp: " + exp + " is finished in " + diffTime.intValue()
									+ " miliseconds!");

							differenceTimes.add(diffTime);

							if (exp != (numberOfSameExperiment - 1)) {
								starFramework2 = null;
							}
						}
						System.gc();
						System.runFinalization();

						finalDifferenceTime = Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimes,
								numberOfSameExperiment);

						totalQueriesTimeBySF += finalDifferenceTime;

						System.out.println("StarFramework avg is finished in " + finalDifferenceTime + " miliseconds!");

						int anyTimeResultsSizeTemp = starFramework2.anyTimeResults.size();
						String prefix = "OurSF;" + queryFileName + ";" + queryIndex + ";" + numberOfQNodes + ";"
								+ numberOfQRelationships + ";" + GName + ";" + k + ";" + finalDifferenceTime + ";"
								+ anyTimeResultsSizeTemp + ";"
								+ starFramework2.calcTreeStarQueriesNodeMap.keySet().size() + ";";

						HashMap<Integer, Integer> maxAnyTimeAnswerDepthStarQueryMap = new HashMap<Integer, Integer>();
						double trueRank = DummyFunctions.getRank(starFramework2.anyTimeResults);

						// bwTimeSF.newLine();
						bwTimeSF.flush();
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
							finalizeMethod(starFramework2, bwTimeSF, prefix, maxAnyTimeAnswerDepthStarQueryMap);
							// printAnswerResult(bwAnswersSF, queryIndex,
							// starFramework2);

							// DummyProperties.debuggMode = true;
							for (Integer beamSize : beamSizes) {
								for (Integer delta : deltas) {
									// for (MinObjectiveFunction rankingFunction
									// : MinObjectiveFunction.values()) {

									System.out.println();
									Double oracleTime = 0d;

									BeamSearch bSearch = null;

									start_time = System.currentTimeMillis();
									infoHolders[0].oracleTotalRequestForFetches = starFramework2.starQueries.size();
									infoHolders[0].oracleFetchesCalls = starFramework2.starQueries.size();
									bSearch = new BeamSearch(queryGraph, knowledgeGraph, beamSize, delta, oracle, this,
											neighborIndexingInstance, cacheServer, maxAnyTimeAnswerDepthStarQueryMap,
											trueRank, numberOfQNodes, numberOfQRelationships,
											MinObjectiveFunction.TotalTime, rankCoeff, timeCoeff, depthCoeff,
											finalDifferenceTime, infoHolders[0]);

									System.out.println("SF Oracle could solve it at:" + Double
											.valueOf(bSearch.lastBeamSearchNode.totalTimeInMilliseconds).intValue()
											+ " miliseconds");
									oracleTime = (double) ((System.currentTimeMillis() - start_time) / (1000));

									System.out.println(queryFileName + " queryIndex:" + queryIndex + " beamSize:"
											+ beamSize + " delta:" + delta + " top-k time: "
											+ Double.valueOf(bSearch.lastBeamSearchNode.totalTimeInMilliseconds)
													.intValue()
											+ " miliseconds" + " oracleTime:" + oracleTime.intValue() + ";"
											+ MinObjectiveFunction.TotalTime.toString());
									System.out.println();

									printBeamResult(GName, queryIndex, queryFileName, numberOfQNodes,
											numberOfQRelationships, beamSize, delta, k,
											Double.valueOf(bSearch.lastBeamSearchNode.totalTimeInMilliseconds)
													.intValue(),
											bwTimeBeamSearch, bSearch.lastBeamSearchNode, oracleTime,
											MinObjectiveFunction.TotalTime.toString(), infoHolders);

									totalQueriesTimeByOracle += bSearch.lastBeamSearchNode.totalTimeInMilliseconds;

									bSearch = null;
									System.gc();
									System.runFinalization();
									// }
								}
							}

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
		bwTimeBeamSearch.flush();
		// bwAnswersBeamSearch.flush();
		// bwAnswersSF.flush();
		bwTimeSF.flush();

		bwTimeBeamSearch.close();
		// bwAnswersBeamSearch.close();
		// bwAnswersSF.close();
		bwTimeSF.close();

		knowledgeGraph.shutdown();

		bwTimeByOracle.write(Double.toString(totalQueriesTimeByOracle));
		bwTimeByOracle.flush();
		bwTimeByOracle.close();

		bwTimeBySFAndOracle.write(Double.toString(totalQueriesTimeBySF) + ";"
				+ Double.toString(totalQueriesTimeByOracle) + ";" + (totalQueriesTimeBySF / totalQueriesTimeByOracle));
		bwTimeBySFAndOracle.newLine();

		System.out.println(
				"speed-up: " + Double.toString(totalQueriesTimeBySF) + ";" + Double.toString(totalQueriesTimeByOracle));
		bwTimeBySFAndOracle.flush();
		bwTimeBySFAndOracle.close();

		System.out.println("program is finished properly!");

	}

	public static void main(String[] args) throws Exception {
		BeamSearchRunner beamSearchRunner = new BeamSearchRunner();
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

	public AnyTimeStarFramework copy() {
		return this.starFramework2.copy(queryGraph, knowledgeGraph);

	}

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
				+ numberOfRelationships + ";" + beamSize + ";" + delta + ";"
				+ beamSearchNode.starframework.finalResultsArrayList.size() + ";" + k + ";" + difference + ";"
				+ beamSearchNode.level + ";" + oracleTime + ";" + rankingFunction + ";";

		int totalOracleFetched = 0;
		for (Integer index : beamSearchNode.resultPartialAnswersShouldBeByNodeIndex.keySet()) {
			prefix += " (" + index + ": " + beamSearchNode.resultPartialAnswersShouldBeByNodeIndex.get(index) + ") ";
			totalOracleFetched += beamSearchNode.resultPartialAnswersShouldBeByNodeIndex.get(index);
		}

		prefix += ";" + totalSFDepth + ";" + totalAnswersDepth + ";" + totalOracleFetched;

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

		prefix += ";" + beamSearchNode.sequenceForDebug + ";";

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
