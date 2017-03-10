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

public class SequenceRunnerWithoutFeatures {
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
	public int endingQueryIndex;

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

						for (int exp = 0; exp < numberOfSameExperiment; exp++) {
							starFramework2 = getNewStarFrameworkInstance();

							start_time = System.nanoTime();
							starFramework2.starRoundRobinRun(queryGraph, knowledgeGraph, neighborIndexingInstance,
									cacheServer);

							end_time = System.nanoTime();
							diffTime = (end_time - start_time) / 1e6;
							System.out.println("StarFramework exp: " + exp + " is finished in " + diffTime.intValue()
									+ " miliseconds!");

							differenceTimes.add(diffTime);

							if (exp != (numberOfSameExperiment - 1)) {
								starFramework2 = null;
							}
						}

						finalDifferenceTime = Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimes,
								numberOfSameExperiment);
						System.out.println("StarFramework avg is finished in " + finalDifferenceTime + " miliseconds!");

						for (int exp = 0; exp < numberOfSameExperiment; exp++) {
							starFramework2 = getNewStarFrameworkInstance();

							int numberOfStars = starFramework2.starQueries.size();
							int numberOfCalcNodes = starFramework2.calcTreeNodeMap.size();

							// reading the oracle sequences
							ArrayList<HashMap<Integer, Integer>> fetchSeqByStarQueryIndex = readOracleSequence(
									queryIndex, starFramework2.starQueries.size());

							if (fetchSeqByStarQueryIndex.size() < numberOfStars) {
								tx2.success();
								tx2.close();
								queryGraph.shutdown();
								queryGraph = null;
								System.err.println(fetchSeqByStarQueryIndex.size() + " < " + numberOfStars);
								continue;
							}
							System.out.println("beamResults:");
							boolean shouldFinish = false;
							double stime;
							start_time = System.nanoTime();
							int starkOperation = 0;
							int joinOperation = 0;
							for (int level = 0; level < fetchSeqByStarQueryIndex.size(); level++) {
								HashMap<Integer, Integer> fetchByQueryIndex = fetchSeqByStarQueryIndex.get(level);
								int depthJoinLevel = 0;
								for (Integer queryIndexToBeFetched : fetchByQueryIndex.keySet()) {

									TreeNode<CalculationNode> thisCalcNode = starFramework2.calcTreeNodeMap
											.get(queryIndexToBeFetched);
									thisCalcNode.getData().numberOfPartialAnswersShouldBeFetched = fetchByQueryIndex
											.get(queryIndexToBeFetched);
									starkOperation++;
									stime = System.nanoTime();
									starFramework2.anyTimeStarkForLeaf(knowledgeGraph, thisCalcNode,
											neighborIndexingInstance, cacheServer);
									System.out
											.println("f:" + starkOperation + " :" + (System.nanoTime() - stime) / 1e6);
									depthJoinLevel = thisCalcNode.levelInCalcTree - 1;

								}
								stime = System.nanoTime();
								for (; depthJoinLevel >= 0; depthJoinLevel--) {
									joinOperation++;
									CalculationTreeSiblingNodes calculationTreeSiblingNodes = starFramework2.joinLevelSiblingNodesMap
											.get(depthJoinLevel);
									starFramework2.anyTimeTwoWayHashJoin(calculationTreeSiblingNodes.leftNode,
											calculationTreeSiblingNodes.rightNode, starFramework2.k);
								}
								System.out.println("j:" + joinOperation + " :" + (System.nanoTime() - stime) / 1e6);
								depthJoinLevel = 0;

								if (starFramework2.anyTimeAlgorithmShouldFinish()) {
									shouldFinish = true;
								}
							}

							end_time = System.nanoTime();

							diffTime = (end_time - start_time) / 1e6;
							System.out.println("oracle sequence exp: " + exp + " is finished in " + diffTime.intValue()
									+ " miliseconds!");
							differenceTimes.add(diffTime);

							if (exp != (numberOfSameExperiment - 1)) {
								starFramework2 = null;
							} else {
								starFramework2 = null;
							}
						}

						finalDifferenceTime = Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimes,
								numberOfSameExperiment);
						System.out
								.println("oracle sequence avg is finished in " + finalDifferenceTime + " miliseconds!");

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

		knowledgeGraph.shutdown();

		System.out.println("program is finished properly!");

	}

	private ArrayList<HashMap<Integer, Integer>> readOracleSequence(int queryIndex, int size) throws Exception {
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
		SequenceRunnerWithoutFeatures beamSearchRunner = new SequenceRunnerWithoutFeatures();
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

	public static File[] fileInTheDirfinder(String dirName) {
		File dir = new File(dirName);

		return dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String filename) {
				return filename.endsWith(".txt");
			}
		});

	}
}