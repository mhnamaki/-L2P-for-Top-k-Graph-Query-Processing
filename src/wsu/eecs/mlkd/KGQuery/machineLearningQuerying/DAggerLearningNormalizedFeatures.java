package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
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

import weka.classifiers.Classifier;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public class DAggerLearningNormalizedFeatures {

	// get a training set and a validation set
	// learn a classifier over training examples from exact imitation of the
	// training set.
	// for each dagger iteration
	// get a query from training set
	// at each fetch/join step, compute features and get the classifier result
	// compare oracle sequence with the current classifier result
	// add classification example
	// after all training queries, learn a classifier on the new examples.

	private static String MODELGRAPH_DB_PATH = "";
	private static String PATTERNGRAPH_DB_PATH = "";

	public static String queryFileName = "";
	public static String queryFileDirectory = "";

	public static String GName = ""; // Yago, DBPedia, ...

	public static String queryDBInNeo4j = "query.db";
	public static String GDirectory = "";
	public int numberOfSameExperiment = 1;
	public AnyTimeStarFramework mlSF;
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
	private int maxNumberOfIteration;

	private double beta;
	Random rand = new Random();
	private String queriesFoldPath;
	private int trainingFoldStartFrom;
	private int trainingFoldEndTo;
	private String trainingQueriesSelectionFeaturesPath;
	private String trainingQueriesRegressionFeaturesPath;

	CommonFunctions commonFunctions = new CommonFunctions();
	private int numberOfTrees;
	private int maxDepth;
	private boolean isCostSensitive;

	final int SELECTION_FEATURES_SIZE = 108;
	final int EXPANSION_FEATURES_SIZE = 49;

	double[] selectionNormalizationFeaturesVector;
	double[] expansionNormalizationFeaturesVector;

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
			} else if (args[i].equals("-numberOfPrefixChars")) {
				numberOfPrefixChars = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-startingQueryIndex")) {
				startingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-endingQueryIndex")) {
				endingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-oracleSequenceFile")) {
				oracleSequenceFile = args[++i];
			} else if (args[i].equals("-queriesFoldPath")) {
				queriesFoldPath = args[++i];
			} else if (args[i].equals("-trainingQueriesSelectionFeaturesPath")) {
				trainingQueriesSelectionFeaturesPath = args[++i];
			} else if (args[i].equals("-trainingQueriesRegressionFeaturesPath")) {
				trainingQueriesRegressionFeaturesPath = args[++i];
			}

			else if (args[i].equals("-trainingFoldStartFrom")) {
				trainingFoldStartFrom = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-trainingFoldEndTo")) {
				trainingFoldEndTo = Integer.parseInt(args[++i]);
			}
			// else if (args[i].equals("-validationFoldStartFrom")) {
			// validationFoldStartFrom = Integer.parseInt(args[++i]);
			// } else if (args[i].equals("-validationFoldEndTo")) {
			// validationFoldEndTo = Integer.parseInt(args[++i]);
			// } else if (args[i].equals("-testingFoldStartFrom")) {
			// testingFoldStartFrom = Integer.parseInt(args[++i]);
			// } else if (args[i].equals("-testingFoldEndTo")) {
			// testingFoldEndTo = Integer.parseInt(args[++i]);
			// }

			else if (args[i].equals("-maxNumberOfIteration")) {
				maxNumberOfIteration = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-beta")) {
				beta = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-numberOfTrees")) {
				numberOfTrees = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-maxDepth")) {
				maxDepth = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-isCostSensitive")) {
				if (Integer.parseInt(args[++i]) == 0) {
					isCostSensitive = false;
				} else {
					isCostSensitive = true;
				}
			}

		}

		// ExactImitation exactImitation = new ExactImitation();
		// exactImitation.loadModels();

		// ExactImitation exactImitation = new
		// ExactImitation(trainingQueriesSelectionFeaturesPath,
		// trainingQueriesRegressionFeaturesPath, queriesFoldPath,
		// trainingFoldStartFrom, trainingFoldEndTo);

		fillNormalizationVectorsAndPrintThem();

		ExactImitation exactImitation = new ExactImitation(trainingQueriesSelectionFeaturesPath,
				trainingQueriesRegressionFeaturesPath);

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

		knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(MODELGRAPH_DB_PATH)
				.setConfig(GraphDatabaseSettings.pagecache_memory, "6g").newGraphDatabase();
		DummyFunctions.printIfItIsInDebuggedMode("after initialization of GraphDatabaseServices");

		commonFunctions.registerShutdownHook(knowledgeGraph);

		File foutTime = new File(GName + "_machineLearningQueryRunningTime.txt");
		FileOutputStream fosTime = new FileOutputStream(foutTime, true);
		BufferedWriter bwMLTime = new BufferedWriter(new OutputStreamWriter(fosTime));
		if (startingQueryIndex < 2) {
			bwMLTime.write(
					"queryIndex;sfDifferenceTime;mlDifferenceTime;totalSFDepth;totalMLSFDepth;totalAnswersDepth;sfQuality;mlSFQuality;SF Depth; MLSF Depth;Answers Depth");
			bwMLTime.newLine();
		}

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

		HashSet<Integer> trainingQueriesSet = commonFunctions.readQueryIndexBasedOnFolds(queriesFoldPath,
				trainingFoldStartFrom, trainingFoldEndTo);
		// HashSet<Integer> validationQueriesSet =
		// commonFunctions.readQueryIndexBasedOnFolds(queriesFoldPath,
		// validationFoldStartFrom, validationFoldEndTo);
		// HashSet<Integer> testingQueriesSet =
		// commonFunctions.readQueryIndexBasedOnFolds(queriesFoldPath,
		// testingFoldStartFrom, testingFoldEndTo);

		// learning
		Classifier[] classifiers = new Classifier[maxNumberOfIteration + 1];
		Classifier[] regressors = new Classifier[maxNumberOfIteration + 1];
		Instances classifiersExamples = null;
		Instances regressorsExamples = null;

		classifiers[0] = exactImitation.learnClassifier(exactImitation.instanceClassifier, trainingFoldStartFrom,
				trainingFoldEndTo, numberOfTrees, maxDepth, isCostSensitive, selectionNormalizationFeaturesVector);

		System.out.println("exactImitation.instanceClassifier: " + exactImitation.instanceClassifier.numInstances());

		regressors[0] = exactImitation.learnRegressor(exactImitation.instanceRegressor, trainingFoldStartFrom,
				trainingFoldEndTo, expansionNormalizationFeaturesVector);

		System.out.println("exactImitation.instanceRegressor: " + exactImitation.instanceRegressor.numInstances());

		classifiersExamples = initializeExamples("training-selection-split-by-query.arff", exactImitation,
				selectionNormalizationFeaturesVector);
		regressorsExamples = initializeExamples("training-expansion-split-by-query.arff", exactImitation,
				expansionNormalizationFeaturesVector);

		System.out.println("classifiersExamples size: " + classifiersExamples.numInstances());

		System.out.println(" init classifiersExamples numAttributes: " + classifiersExamples.numAttributes());
		System.out.println();

		System.out.println("regressorsExamples size: " + regressorsExamples.numInstances());
		System.out.println(" init regressorsExamples numAttributes: " + regressorsExamples.numAttributes());
		System.out.println();

		exactImitation.SaveModelSetName("DAggerClassifier_0", classifiers[0]);
		exactImitation.SaveModelSetName("DAggerRegressor_0", regressors[0]);

		System.out.println("-numberOfTrees:" + numberOfTrees + " , maxDepth:" + maxDepth + " , isCostSensitive:"
				+ isCostSensitive);

		for (int iteration = 0; iteration < maxNumberOfIteration; iteration++) {
			System.out.println("iteration: " + iteration);
			searchOverQueries(iteration, trainingQueriesSet, classifiers, regressors, classifiersExamples,
					regressorsExamples);

			System.out.println("number of classifiersExamples: " + classifiersExamples.numInstances());
			System.out.println("number of regressorsExamples: " + regressorsExamples.numInstances());

			int nextItr = iteration + 1;

			logTheExamples(iteration, classifiersExamples, regressorsExamples);

			classifiers[nextItr] = commonFunctions.learnRFClassifier(classifiersExamples, numberOfTrees, maxDepth,
					isCostSensitive);
			regressors[nextItr] = commonFunctions.learnRegressor(regressorsExamples);

			exactImitation.SaveModelSetName("DAggerClassifier_" + nextItr, classifiers[nextItr]);
			exactImitation.SaveModelSetName("DAggerRegressor_" + nextItr, regressors[nextItr]);

			beta = Math.pow(0.8, nextItr);

		}
		// findAndSaveBestClassifierRegressorPairUntilHere(classifiers,
		// regressors, iteration, validationQueriesSet);
		bwMLTime.flush();
		bwMLTime.close();
		knowledgeGraph.shutdown();
		System.out.println("program is finished properly!");
	}

	private void fillNormalizationVectorsAndPrintThem() throws Exception {

		selectionNormalizationFeaturesVector = new double[SELECTION_FEATURES_SIZE];
		selectionNormalizationFeaturesVector = createNormalizationFeaturesVector(trainingQueriesSelectionFeaturesPath,
				SELECTION_FEATURES_SIZE);

		expansionNormalizationFeaturesVector = new double[EXPANSION_FEATURES_SIZE];
		expansionNormalizationFeaturesVector = createNormalizationFeaturesVector(trainingQueriesRegressionFeaturesPath,
				EXPANSION_FEATURES_SIZE);

		File foutNorm = new File("normalizationVectors.txt");
		FileOutputStream fosNorm = new FileOutputStream(foutNorm);

		BufferedWriter bwNorm = new BufferedWriter(new OutputStreamWriter(fosNorm));

		for (int i = 0; i < selectionNormalizationFeaturesVector.length; i++) {
			bwNorm.write(selectionNormalizationFeaturesVector[i] + ", ");
		}
		bwNorm.newLine();
		for (int i = 0; i < expansionNormalizationFeaturesVector.length; i++) {
			bwNorm.write(expansionNormalizationFeaturesVector[i] + ", ");
		}

		bwNorm.close();

	}

	private double[] createNormalizationFeaturesVector(String trainingQueriesFeaturesPath, int maxFeaturesSize)
			throws Exception {

		double[] normalizationFeaturesVector = new double[maxFeaturesSize];

		FileInputStream fis = new FileInputStream(trainingQueriesFeaturesPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		while ((line = br.readLine()) != null) {
			String[] features = line.split(",");
			// features[0] is queryIndex and features[last] is output label
			if (features.length > 0) {
				for (int i = 1; i < features.length - 1; i++) {
					if (!features[i].trim().toLowerCase().equals("true")
							&& !features[i].trim().toLowerCase().equals("false")) {
						double tempABSFValue = Math.abs(Double.parseDouble(features[i]));
						if (tempABSFValue > normalizationFeaturesVector[i - 1]) {
							normalizationFeaturesVector[i - 1] = tempABSFValue * 1.6;
						}
					} else {
						normalizationFeaturesVector[i - 1] = 1;
					}
				}
			}
		}

		br.close();

		boolean dividedByZero = false;
		System.out.println();
		System.out.println(trainingQueriesFeaturesPath + " " + maxFeaturesSize);
		for (int i = 0; i < normalizationFeaturesVector.length; i++) {
			if (normalizationFeaturesVector[i] == 0) {
				System.err.println("normalizationFeaturesVector[" + i + "] == 0");
				dividedByZero = true;
			}
		}

		String normalizationFeaturesVectorStr = "";
		for (int i = 1; i <= normalizationFeaturesVector.length; i++) {
			normalizationFeaturesVectorStr += i + ":" + normalizationFeaturesVector[i - 1] + ", ";
		}

		System.out.println(normalizationFeaturesVectorStr);

		if (dividedByZero) {
			throw new Exception("dividedByZero for normalization features will be happened!");
		}

		return normalizationFeaturesVector;
	}

	private void logTheExamples(int iteration, Instances classifiersExamples, Instances regressorsExamples)
			throws Exception {
		File foutCls = new File("classifiersExamples_" + iteration + ".txt");
		FileOutputStream fosCls = new FileOutputStream(foutCls);
		BufferedWriter bwCls = new BufferedWriter(new OutputStreamWriter(fosCls));

		File foutReg = new File("regressorsExamples_" + iteration + ".txt");
		FileOutputStream fosReg = new FileOutputStream(foutReg);
		BufferedWriter bwReg = new BufferedWriter(new OutputStreamWriter(fosReg));

		// make sure the last attribute will be printed also
		for (int i = 0; i < classifiersExamples.numInstances(); i++) {
			bwCls.write(classifiersExamples.instance(i).classValue() + " ");
			for (int j = 0; j < (classifiersExamples.instance(i).numAttributes() - 1); j++) {
				bwCls.write(j + ":" + classifiersExamples.instance(i).value(j) + " ");
			}
			bwCls.write("\n");
		}

		// make sure the last attribute will be printed also
		for (int i = 0; i < regressorsExamples.numInstances(); i++) {
			bwReg.write(regressorsExamples.instance(i).classValue() + " ");
			for (int j = 0; j < (regressorsExamples.instance(i).numAttributes() - 1); j++) {
				bwReg.write(j + ":" + regressorsExamples.instance(i).value(j) + " ");
			}
			bwReg.write("\n");
		}

		bwCls.close();
		bwReg.close();

	}

	private void searchOverQueries(int iteration, HashSet<Integer> queriesSet, Classifier[] classifiers,
			Classifier[] regressors, Instances classifiersExamples, Instances regressorsExamples) throws Exception {
		QueryGenerator queryGenerator = new QueryGenerator(GDirectory + GName);
		for (File file : CommonFunctions.fileInTheDirfinder(queryFileDirectory)) {
			queryFileName = file.getName();
			List<QueryFromFile> queriesFromFile = queryGenerator.getQueryFromFile(file.getAbsolutePath());

			int queryIndex = 0;
			for (QueryFromFile queryFromFile : queriesFromFile) {
				queryIndex = queryFromFile.queryIndex;
				if (queryIndex < startingQueryIndex || queryIndex > endingQueryIndex) {
					continue;
				}

				if (!queriesSet.contains(queryIndex)) {
					continue;
				}

				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH,
						queryFromFile);

				queryGraph = smallGraph;

				neighborIndexingInstance.queryNeighborIndexer(queryGraph);

				System.out.println("queryfileName: " + queryFileName + ", queryIndex: " + queryIndex + " k: " + k);

				ArrayList<Integer> oracleSelectionSteps = new ArrayList<Integer>();
				ArrayList<Integer> oracleExpansionSteps = new ArrayList<Integer>();

				commonFunctions.setOracleSteps(oracleSequenceFile, queryIndex, oracleSelectionSteps,
						oracleExpansionSteps);

				try (Transaction tx1 = queryGraph.beginTx()) {
					try (Transaction tx2 = knowledgeGraph.beginTx()) {
						int numberOfQNodes = neighborIndexingInstance.queryNodeIdSet.size();
						int numberOfQRelationships = 0;
						for (Relationship rel : GlobalGraphOperations.at(queryGraph).getAllRelationships()) {
							numberOfQRelationships++;
						}

						mlSF = getNewStarFrameworkInstance();

						int numberOfStars = mlSF.starQueries.size();
						int numberOfCalcNodes = mlSF.calcTreeNodeMap.size();

						int maxNumberOfStars = DummyProperties.maxNumberOfSQ;
						int maxNumberOfCalcNodes = 2 * DummyProperties.maxNumberOfSQ - 1;

						Features baseStaticFeatures = null;

						// HashSet<Integer> sqIndices = new HashSet<Integer>();
						HashMap<Integer, Integer> classValMap = new HashMap<Integer, Integer>();
						// HashMap<Integer, Integer> classValMapReverse = new
						// HashMap<Integer, Integer>();

						// for (Integer sqIndex :
						// mlSF.calcTreeStarQueriesNodeMap.keySet()) {
						// sqIndices.add(sqIndex);
						// }
						int allCalcNodeSize = mlSF.calcTreeNodeMap.size();

						// 0: stop. 1=SQ1, 2=SQ2, ..., 5=SQ5
						FastVector fvClassVal = new FastVector(numberOfStars);
						for (int i = 0; i <= maxNumberOfStars; i++) {
							fvClassVal.addElement(String.valueOf(i));
							classValMap.put(i, i);
						}
						// classValMap.put(0, 0);
						// classValMapReverse.put(0, 0);
						// fvClassVal.addElement("0");
						// for (Integer c : sqIndices) {
						// classValMap.put(fvClassVal.size(), c);
						// classValMapReverse.put(c, fvClassVal.size());
						// fvClassVal.addElement(c.toString());
						// }

						FastVector fvWekaClassificationAttributes = commonFunctions
								.getWekaAttributesForCreatingANewClassificationInstance(allCalcNodeSize, fvClassVal,
										maxNumberOfStars, maxNumberOfCalcNodes);

						FastVector fvWekaRegressionAttributes = commonFunctions
								.getWekaAttributesForCreatingANewRegressionInstance(fvClassVal, maxNumberOfStars,
										maxNumberOfCalcNodes);

						int depthJoinLevel = 0;

						for (Integer starQNode : mlSF.calcTreeStarQueriesNodeMap.keySet()) {
							mlSF.calcTreeStarQueriesNodeMap.get(starQNode)
									.getData().numberOfPartialAnswersShouldBeFetched = 1;
							mlSF.anyTimeStarkForLeaf(knowledgeGraph, mlSF.calcTreeStarQueriesNodeMap.get(starQNode),
									neighborIndexingInstance, cacheServer);
							depthJoinLevel = mlSF.calcTreeStarQueriesNodeMap.get(starQNode).levelInCalcTree - 1;

							for (; depthJoinLevel >= 0; depthJoinLevel--) {
								CalculationTreeSiblingNodes calculationTreeSiblingNodes = mlSF.joinLevelSiblingNodesMap
										.get(depthJoinLevel);
								mlSF.anyTimeTwoWayHashJoin(calculationTreeSiblingNodes.leftNode,
										calculationTreeSiblingNodes.rightNode, mlSF.k);
							}
						}

						baseStaticFeatures = initStaticFeatures(queryIndex, mlSF, numberOfQNodes,
								numberOfQRelationships, numberOfStars, maxNumberOfStars, maxNumberOfCalcNodes);

						BaseFeatures baseFeatures = null;
						SelectionFeatures selectionFeatures = null;
						ExpansionFeatures expansionFeatures = null;
						// SelectionFeatures stoppingFeatures = null;
						depthJoinLevel = 0;
						int paExpansion = 1;
						int level = numberOfStars;
						while (!mlSF.anyTimeAlgorithmShouldFinish()) {
							if (level >= oracleSelectionSteps.size()) {
								break;
							}
							// System.out.println("level: " + level);

							int paSelected = 0;

							selectionFeatures = computeSelectionFeatures(queryIndex, mlSF, level, numberOfStars,
									numberOfCalcNodes, baseFeatures, baseStaticFeatures, maxNumberOfStars,
									maxNumberOfCalcNodes);

							int paSelectedFromOracle = oracleSelectionSteps.get(level);

							Instances testingInstance = commonFunctions.createClassificationTestingInstance(
									fvWekaClassificationAttributes, baseStaticFeatures, selectionFeatures,
									maxNumberOfStars, selectionNormalizationFeaturesVector);

							paSelected = classifyUsingCurrent(beta, classifiers[iteration], classValMap,
									paSelectedFromOracle, testingInstance.firstInstance());

							if (paSelected != paSelectedFromOracle) {
								testingInstance.firstInstance().setClassValue(paSelectedFromOracle);
								classifiersExamples.add(testingInstance.firstInstance());
							}
							// System.out.println("paSelected: " +
							// paSelected);

							// TODO: if paSelected is stop! then, we cannot
							// recover from this error at all!

							if (paSelected < 1 && (level + 1) >= oracleSelectionSteps.size()) {
								break;
							}
							if (paSelected < 1) {
								paSelected = paSelectedFromOracle;
							}

							// if selected SQ stark is enough?
							if (mlSF.calcTreeStarQueriesNodeMapBySQMLIndex.get(paSelected).data.callStarKIsEnough) {
								// select previous SQ if possible.
								if (baseFeatures != null && baseFeatures.paParentSelected != paSelected
										&& !mlSF.calcTreeStarQueriesNodeMapBySQMLIndex
												.get(baseFeatures.paParentSelected).data.callStarKIsEnough) {
									paSelected = baseFeatures.paParentSelected;
								} else {
									// otherwise, find the SQ with min
									// digged depth
									int minDepth = Integer.MAX_VALUE;
									for (int i = 0; i < numberOfStars; i++) {
										// int sqCalcNodeIndex =
										// mlSF.starQueries.get(i).starQueryCalcNodeIndex;
										if (!mlSF.calcTreeStarQueriesNodeMapBySQMLIndex
												.get(i + 1).data.callStarKIsEnough) {
											if (mlSF.calcTreeStarQueriesNodeMapBySQMLIndex
													.get(i + 1).data.depthOfDigging < minDepth) {
												minDepth = mlSF.calcTreeStarQueriesNodeMapBySQMLIndex
														.get(i + 1).data.depthOfDigging;
												paSelected = i + 1;
											}
										}
									}
								}
							}
							expansionFeatures = computeExpansionFeatures(queryIndex, mlSF, level, numberOfStars,
									numberOfCalcNodes, baseFeatures, selectionFeatures, paSelected, baseStaticFeatures);

							// expansionFeatures.print(baseStaticFeatures,
							// bwMLGenExpFeatures);

							Instances regTestingInstance = commonFunctions.createRegressionTestingInstance(
									fvWekaRegressionAttributes, baseStaticFeatures, expansionFeatures, maxNumberOfStars,
									expansionNormalizationFeaturesVector);

							int paOracleExpansion = oracleExpansionSteps.get(level);

							paExpansion = predictUsingCurrent(beta, regressors[iteration], paOracleExpansion,
									regTestingInstance.firstInstance());

							if (paExpansion != paOracleExpansion) {
								// TODO: add the correct label for
								// groundtruth
								regTestingInstance.firstInstance().setClassValue(paOracleExpansion);
								regressorsExamples.add(regTestingInstance.firstInstance());
							}

							// System.out.println("paExpansion: " +
							// paExpansion);

							baseFeatures = baseFeatureFiller(queryIndex, selectionFeatures, mlSF, numberOfStars,
									numberOfCalcNodes, baseFeatures, paSelected, paExpansion, maxNumberOfStars,
									maxNumberOfCalcNodes);

							// TODO: multi-action

							TreeNode<CalculationNode> thisCalcNode = mlSF.calcTreeStarQueriesNodeMapBySQMLIndex
									.get(paSelected);
							thisCalcNode.getData().numberOfPartialAnswersShouldBeFetched = paExpansion;
							mlSF.anyTimeStarkForLeaf(knowledgeGraph, thisCalcNode, neighborIndexingInstance,
									cacheServer);
							depthJoinLevel = thisCalcNode.levelInCalcTree - 1;

							// debug
							// System.out.println("sq: " +
							// thisCalcNode.getData().starQuery.starQueryIndex
							// + " depthOfDigging: " +
							// thisCalcNode.getData().depthOfDigging);

							for (; depthJoinLevel >= 0; depthJoinLevel--) {
								CalculationTreeSiblingNodes calculationTreeSiblingNodes = mlSF.joinLevelSiblingNodesMap
										.get(depthJoinLevel);
								mlSF.anyTimeTwoWayHashJoin(calculationTreeSiblingNodes.leftNode,
										calculationTreeSiblingNodes.rightNode, mlSF.k);
							}
							depthJoinLevel = 0;

							level++;
						}
						mlSF = null;

						System.gc();
						System.runFinalization();

						System.out.println();
						tx2.success();
						tx2.close();

					} catch (

					Exception exc) {
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

	}

	// private void findAndSaveBestClassifierRegressorPairUntilHere(Classifier[]
	// classifiers, Classifier[] regressors,
	// int iteration, HashSet<Integer> validationQueriesSet) throws Exception {
	//
	// MachineLearningQueryRunner mlQueryRunner = new
	// MachineLearningQueryRunner(knowledgeGraph, queryFileDirectory,
	// GDirectory, GName, startingQueryIndex, endingQueryIndex,
	// PATTERNGRAPH_DB_PATH, k, levenshtein,
	// cacheServer, neighborIndexingInstance);
	//
	// for (int i = 0; i < iteration; i++) {
	// for (int j = 0; j < iteration; j++) {
	// mlQueryRunner.findSpeedUpAndQualityOutOfAClassifierRegressorForASet(validationQueriesSet,
	// classifiers[i], regressors[j], iteration,
	// GName + "_machineLearningQueryRunningTime_" + i + "_" + j + ".txt");
	// }
	// }
	//
	// }

	private int predictUsingCurrent(double beta, Classifier regressor, int oraclePAExpansion, Instance instance)
			throws Exception {
		double randomVar = rand.nextDouble();
		if (randomVar < beta) {
			// oracle
			return oraclePAExpansion;
		} else {
			return (int) regressor.classifyInstance(instance);
		}
	}

	private int classifyUsingCurrent(double beta, Classifier classifier, HashMap<Integer, Integer> classValMap,
			int oraclePASelected, Instance instance) throws Exception {

		double randomVar = rand.nextDouble();
		if (randomVar < beta) {
			// oracle
			return oraclePASelected;
		} else {
			int classifierout = (int) classifier.classifyInstance(instance);
			if (classifierout > 5 || classifierout < 0) {
				System.err.println("classifierout: " + classifierout);
			}
			if (classValMap.get(classifierout) == null) {
				System.err.println("classifierout is not there! " + classifierout);
			}
			return classValMap.get(classifierout);
		}
	}

	private Instances initializeExamples(String trainingQueriesFeaturesPath, ExactImitation exactImitation, double[] normalizationFeaturesVector)
			throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(trainingQueriesFeaturesPath));
		Instances data = new Instances(reader);
		reader.close();

		// setting class attribute
		data.setClassIndex(data.numAttributes() - 1);

		data = exactImitation.getInstancesWithOutQueryIndex(data, normalizationFeaturesVector);

		System.out.println("index0: " + data.firstInstance().attribute(0).name());
		System.out.println("numAttributes: " + data.numAttributes());

		return data;
	}

	// private int predictInstance(REPTree regressor, Instance testingInstance)
	// throws Exception {
	//
	// return (int) regressor.classifyInstance(testingInstance);
	// }

	private ExpansionFeatures computeExpansionFeatures(int queryIndex, AnyTimeStarFramework starFramework, int level,
			int numberOfStars, int numberOfCalcNodes, BaseFeatures baseFeatures, SelectionFeatures selectionFeatures,
			int paSelected, Features baseStaticFeatures) throws Exception {

		int sqIndex = paSelected - 1;

		double currentThisLB = selectionFeatures.lbCurrent;
		double currentThisUB = selectionFeatures.ubCurrent[paSelected];
		// parent in calcNode;
		double currentParentUB = starFramework.calcTreeStarQueriesNodeMapBySQMLIndex.get(paSelected).getParent()
				.getData().anytimeUpperBound;
		int pqDiffThisFromParent = selectionFeatures.pqDiffThisFromParent[sqIndex];
		int pqDiffThisFromRoot = selectionFeatures.pqDiffThisFromRoot[sqIndex];
		int generateNextBestMatchQueued = selectionFeatures.generateNextBestMatchQueued[sqIndex];

		double ubDifferenceFromCurrentLB = selectionFeatures.ubDifferenceFromCurrentLB[paSelected];
		double ubDifferenceFromParentUB = selectionFeatures.ubDifferenceFromParentUB[paSelected];
		double ubDifferenceFromRootUB = selectionFeatures.ubDifferenceFromRootUB[paSelected];

		double lbDifferenceFromRootLB = selectionFeatures.lbDifferenceFromRootLB;
		double lbDifferenceFromParentLB = selectionFeatures.lbDifferenceFromParentLB;
		int previousPASelected = selectionFeatures.previousPASelected;
		int previousExpansionValue = 0;
		if (baseFeatures != null)
			previousExpansionValue = baseFeatures.paParentExpansion;

		int howManyTimesSelectedBefore = selectionFeatures.howManyTimesSelectedBefore[sqIndex];

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
				previousExpansionValue, -1);

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
		int previousPASelected = 0;
		// int[] contributionToCurrentAnswer = new int[maxNumberOfStars];
		int[] sqCalcTreeDepth = new int[maxNumberOfStars];
		int[] currentDepth = new int[maxNumberOfStars];
		int[] remainingPA = new int[maxNumberOfStars];
		boolean[] isStarkIsEnough = new boolean[maxNumberOfStars];
		int[] howManySelectedBefore = new int[maxNumberOfStars];

		lbCurrent = starFramework.leastAnyTimeValueResult;

		if (baseFeatures != null) {
			lbDifferenceFromRootLB = starFramework.leastAnyTimeValueResult - baseFeatures.lbRoot;
			lbDifferenceFromParentLB = starFramework.leastAnyTimeValueResult - baseFeatures.lbParent;
			previousPASelected = baseFeatures.paParentSelected;
		}

		// int minCurDepthHelper = Integer.MAX_VALUE;
		// int minCurrentDepthSQIndexHelper = 0;

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
			// if (currentDepth[i] < minCurDepthHelper) {
			// minCurDepthHelper = currentDepth[i];
			// minCurrentDepthSQIndexHelper = i;
			// }
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
		}

		for (Integer nodeId : starFramework.calcTreeNodeMap.keySet()) {
			ubCurrent[nodeId] = starFramework.calcTreeNodeMap.get(nodeId).data.anytimeUpperBound;
			ubDifferenceFromCurrentLB[nodeId] = ubCurrent[nodeId] - lbCurrent;
			if (baseFeatures != null) {
				ubDifferenceFromParentUB[nodeId] = ubCurrent[nodeId] - baseFeatures.ubParent[nodeId];
				ubDifferenceFromRootUB[nodeId] = ubCurrent[nodeId] - baseFeatures.ubRoot[nodeId];
			}
		}

		for (int i = numberOfStars; i < maxNumberOfStars; i++) {
			generateNextBestMatchQueued[i] = 0;
			sqCalcTreeDepth[i] = 0;
			currentDepth[i] = 0;
			isStarkIsEnough[i] = true;
			pqCurrent[i] = 0;
			howManySelectedBefore[i] = 0;
			remainingPA[i] = 0;

		}

		SelectionFeatures sfeatures = new SelectionFeatures(queryIndex, pqCurrent, ubCurrent, lbCurrent,
				pqDiffThisFromParent, pqDiffThisFromRoot, generateNextBestMatchQueued, ubDifferenceFromCurrentLB,
				ubDifferenceFromParentUB, ubDifferenceFromRootUB, lbDifferenceFromRootLB, lbDifferenceFromParentLB,
				previousPASelected, howManySelectedBefore, currentDepth, isStarkIsEnough, remainingPA, -1);

		// sfeatures.print(baseStaticFeatures, bwPASelectionFeatures);

		// sfeatures.minCurrentDepthHelper = minCurDepthHelper;
		// sfeatures.minCurrentDepthSQIndexHelper =
		// minCurrentDepthSQIndexHelper;

		return sfeatures;
	}

	public BaseFeatures baseFeatureFiller(int queryIndex, SelectionFeatures selectionFeatures,
			AnyTimeStarFramework starFramework, int numberOfStars, int numberOfCalcNodes,
			BaseFeatures previousBaseFeatures, int paSelected, int paExpansion, int maxNumberOfStars,
			int maxNumberOfCalcNodes) {

		int[] pqParent = new int[maxNumberOfStars];
		int[] pqRoot = new int[maxNumberOfStars];
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

		return new Features(numberOfQNodes, numberOfQRelationships, numberOfStars, nodesInStar,
				avgPivotDegreeInDataGraph, estimatedPA, firstPQItemSize, possiblePivots, joinableNodes);

	}

	public static void main(String[] args) throws Exception {
		DAggerLearningNormalizedFeatures mlQRunner = new DAggerLearningNormalizedFeatures();
		mlQRunner.initialize(args);
	}

	private AnyTimeStarFramework getNewStarFrameworkInstance(GraphDatabaseService queryGraph,
			GraphDatabaseService knowledgeGraph, int k2, float alpha, Levenshtein levenshtein) {
		cacheServer.clear();
		AnyTimeStarFramework starFramework = new AnyTimeStarFramework(queryGraph, knowledgeGraph, k, alpha,
				levenshtein);
		starFramework.decomposeQuery(queryGraph, knowledgeGraph, neighborIndexingInstance, cacheServer);

		TreeNode<CalculationNode> tempNode = starFramework.rootTreeNode;
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

		return starFramework;
	}

	public AnyTimeStarFramework getNewStarFrameworkInstance() {
		return getNewStarFrameworkInstance(queryGraph, knowledgeGraph, k, alpha, levenshtein);
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

}
