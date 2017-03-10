package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import ml.dmlc.xgboost4j.java.XGBoostError;
import weka.classifiers.Classifier;
import weka.core.FastVector;
import weka.core.Instances;

import ml.dmlc.xgboost4j.java.Booster;;

public class BoostingLearning {

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
	private int round = 1; // default;
	private int maxTreeDepth = 6; // default;
	private int expansionErrorThreshold = 10; // default;

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
			} else if (args[i].equals("-maxNumberOfIteration")) {
				maxNumberOfIteration = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-round")) {
				round = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-maxTreeDepth")) {
				maxTreeDepth = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-expansionErrorThreshold")) {
				expansionErrorThreshold = Integer.parseInt(args[++i]);
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

		knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(MODELGRAPH_DB_PATH)
				.setConfig(GraphDatabaseSettings.pagecache_memory, "6g").newGraphDatabase();
		DummyFunctions.printIfItIsInDebuggedMode("after initialization of GraphDatabaseServices");

		commonFunctions.registerShutdownHook(knowledgeGraph);

		HashMap<String, HashSet<Long>> nodeLabelsIndex = PreProcessingLabels.getPrefixLabelsIndex(knowledgeGraph,
				Dummy.DummyProperties.numberOfPrefixChars);

		neighborIndexingInstance = new NeighborIndexing();
		neighborIndexingInstance.knowledgeGraphNeighborIndexer(knowledgeGraph);

		levenshtein = new Levenshtein(nodeLabelsIndex, Dummy.DummyProperties.numberOfPrefixChars);

		HashSet<Integer> trainingQueriesSet = commonFunctions.readQueryIndexBasedOnFolds(queriesFoldPath,
				trainingFoldStartFrom, trainingFoldEndTo);

		System.out.println("CovertArffFileToDMatrix selection");
		CovertArffFileToDMatrix(trainingQueriesSelectionFeaturesPath, trainingQueriesSet, "newClsExmpls_0.txt");
		DMatrix dmatSelection = new DMatrix("newClsExmpls_0.txt");

		System.out.println("CovertArffFileToDMatrix regression");
		CovertArffFileToDMatrix(trainingQueriesRegressionFeaturesPath, trainingQueriesSet, "newRegExmpls_0.txt");
		DMatrix dmatRegression = new DMatrix("newRegExmpls_0.txt");

		HashMap<String, Object> paramsCls = new HashMap<String, Object>();
		paramsCls.put("eta", 0.3);
		paramsCls.put("booster", "gbtree");
		paramsCls.put("silent", 1);
		paramsCls.put("objective", "multi:softmax");
		paramsCls.put("num_class", "6"); // 0stop, 1,2,3,4,5
		paramsCls.put("eval_metric", "mlogloss");
		paramsCls.put("max_depth", Integer.toString(maxTreeDepth));

		HashMap<String, DMatrix> watchesCls = new HashMap<String, DMatrix>();
		watchesCls.put("selTrain", dmatSelection);

		HashMap<String, Object> paramsReg = new HashMap<String, Object>();
		paramsReg.put("eta", 0.3);
		paramsReg.put("booster", "gbtree");
		paramsReg.put("silent", 1);
		paramsReg.put("objective", "reg:linear");
		paramsReg.put("eval_metric", "rmse");
		paramsReg.put("max_depth", Integer.toString(maxTreeDepth));

		HashMap<String, DMatrix> watchesReg = new HashMap<String, DMatrix>();
		watchesReg.put("regTrain", dmatRegression);

		Booster boosterClassifier = XGBoost.train(dmatSelection, paramsCls, round, watchesCls, null, null);
		boosterClassifier.saveModel("BoosterClassifier_0.model");
		dumpModelOnDisk("classifier_0_Info.txt", boosterClassifier.getModelDump("", false));

		Booster boosterRegressor = XGBoost.train(dmatRegression, paramsReg, round, watchesReg, null, null);
		boosterRegressor.saveModel("BoosterRegressor_0.model");
		dumpModelOnDisk("regressor_0_Info.txt", boosterRegressor.getModelDump("", false));

		File foutExpansionError = new File("errorAnalysis.txt");
		FileOutputStream fosExpansionError = new FileOutputStream(foutExpansionError);
		BufferedWriter bwExpansionError = new BufferedWriter(new OutputStreamWriter(fosExpansionError));

		bwExpansionError.write(
				"nextItr, expPredictSmallerError, expPredictLargerError, expErrorAfterThresholdSmallerPrediction, expErrorAfterThresholdLargerPrediction, classificationError, ");

		bwExpansionError.write(
				"avgExpPredictSmallerError, avgExpPredictLargerError, avgExpErrorAfterThresholdSmallerPrediction, avgExpErrorAfterThresholdLargerPrediction, ");

		bwExpansionError
				.write("expErrorAfterThresholdSmallerPredictionCount, expErrorAfterThresholdLargerPredictionCount");

		bwExpansionError.newLine();

		for (int iteration = 0; iteration < maxNumberOfIteration; iteration++) {
			System.out.println("iteration: " + iteration);

			int nextItr = iteration + 1;

			String newClassificationInstancesFilePath = "newClsExmpls_" + nextItr + ".txt";
			String newRegressionInstancesFilePath = "newRegExmpls_" + nextItr + ".txt";
			/// String newErrorAnalysisFilePath = "newErrorAnalysis_" + +
			/// ".txt";

			searchOverQueries(iteration, trainingQueriesSet, boosterClassifier, boosterRegressor,
					newClassificationInstancesFilePath, newRegressionInstancesFilePath, bwExpansionError, nextItr);

			System.out.println("number of classifiersExamples: " + dmatSelection.rowNum());
			System.out.println("number of regressorsExamples: " + dmatRegression.rowNum());

			dmatSelection = new DMatrix(newClassificationInstancesFilePath);
			boosterClassifier.update(dmatSelection, round);
			boosterClassifier.saveModel("BoosterClassifier_" + nextItr + ".model");
			dumpModelOnDisk("classifier_" + nextItr + "_Info.txt", boosterClassifier.getModelDump("", false));

			dmatRegression = new DMatrix(newRegressionInstancesFilePath);
			boosterRegressor.update(dmatRegression, round);
			boosterRegressor.saveModel("BoosterRegressor_" + nextItr + ".model");
			dumpModelOnDisk("regressor_" + nextItr + "_Info.txt", boosterRegressor.getModelDump("", false));
		}
		bwExpansionError.close();
	}

	private void dumpModelOnDisk(String infoPath, String[] modelDump) throws Exception {
		File foutinfoPath = new File(infoPath);
		FileOutputStream fosInfo = new FileOutputStream(foutinfoPath);
		BufferedWriter bwInfo = new BufferedWriter(new OutputStreamWriter(fosInfo));

		for (int i = 0; i < modelDump.length; i++) {
			bwInfo.write(modelDump[i] + " ");
		}

		bwInfo.close();
	}

	private void searchOverQueries(int iteration, HashSet<Integer> queriesSet,
			ml.dmlc.xgboost4j.java.Booster boosterClassifier, ml.dmlc.xgboost4j.java.Booster boosterRegressor,
			String newClsExamplesFilePath, String newRegExamplesFilePath, BufferedWriter bwExpansionError, int nextItr)
			throws Exception {

		int expPredictSmallerErrorAmount = 0;
		int expPredictSmallerErrorCount = 0;

		int expPredictLargerErrorAmount = 0;
		int expPredictLargerErrorCount = 0;

		int expErrorAfterThresholdSmallerPredictionAmount = 0;
		int expErrorAfterThresholdSmallerPredictionCount = 0;

		int expErrorAfterThresholdLargerPredictionAmount = 0;
		int expErrorAfterThresholdLargerPredictionCount = 0;

		int classificationError = 0;

		File foutCls = new File(newClsExamplesFilePath);
		FileOutputStream fosCls = new FileOutputStream(foutCls);
		BufferedWriter bwCls = new BufferedWriter(new OutputStreamWriter(fosCls));

		File foutReg = new File(newRegExamplesFilePath);
		FileOutputStream fosReg = new FileOutputStream(foutReg);
		BufferedWriter bwReg = new BufferedWriter(new OutputStreamWriter(fosReg));

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

							DMatrix dMatrixTestCls = commonFunctions.createClassificationBoosterInstance(
									baseStaticFeatures, selectionFeatures, maxNumberOfStars);

							float[][] predicts = boosterClassifier.predict(dMatrixTestCls);

							paSelected = (int) predicts[0][0];

							// System.out.println("boosterClassifier prediction:
							// " + paSelectedFromOracle + ", "
							// + paSelected + ", " + predicts[0][0]);

							if (paSelected != paSelectedFromOracle) {
								// TODO: add the correct label for
								// groundtruth
								printFeaturesIntoDMatFileCls(baseStaticFeatures, selectionFeatures, maxNumberOfStars,
										paSelectedFromOracle, bwCls);

								classificationError++;
							}

							// TODO: if paSelected is stop! then, we cannot
							// recover from this error at all!

							if (paSelected < 1 && (level + 1) >= oracleSelectionSteps.size()) {
								break;
							}
							if (paSelected < 1) {
								paSelected = paSelectedFromOracle;
							}

							if (mlSF.calcTreeStarQueriesNodeMapBySQMLIndex.get(paSelected) == null) {
								paSelected = paSelectedFromOracle;
							}
							// if selected SQ stark is enough?
							else if (mlSF.calcTreeStarQueriesNodeMapBySQMLIndex
									.get(paSelected).data.callStarKIsEnough) {
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

							DMatrix dMatrixTestReg = commonFunctions.createRegressionBoosterInstance(baseStaticFeatures,
									expansionFeatures, maxNumberOfStars);

							int paOracleExpansion = oracleExpansionSteps.get(level);

							float[][] predictsReg = boosterRegressor.predict(dMatrixTestReg);

							paExpansion = (int) Math.ceil(predictsReg[0][0]);

							// System.out.println("boosterRegressor prediction:
							// " + paOracleExpansion + ", " + paExpansion
							// + ", " + predictsReg[0][0]);

							// if (paExpansion != paOracleExpansion) {

							if (paExpansion > paOracleExpansion) {
								expPredictLargerErrorAmount += paExpansion - paOracleExpansion;
								expPredictLargerErrorCount++;
							} else {
								expPredictSmallerErrorAmount += paOracleExpansion - paExpansion;
								expPredictSmallerErrorCount++;
							}

							if (paExpansion > (paOracleExpansion + expansionErrorThreshold)
									|| paExpansion < (paOracleExpansion - expansionErrorThreshold)) {

								if (paExpansion > paOracleExpansion) {
									expErrorAfterThresholdLargerPredictionAmount += paExpansion - paOracleExpansion;
									expErrorAfterThresholdLargerPredictionCount++;
								} else {
									expErrorAfterThresholdSmallerPredictionAmount += paOracleExpansion - paExpansion;
									expErrorAfterThresholdSmallerPredictionCount++;
								}

								printFeaturesIntoDMatFileReg(baseStaticFeatures, expansionFeatures, maxNumberOfStars,
										paOracleExpansion, bwReg);
							}

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

		bwCls.flush();
		bwCls.close();

		bwReg.flush();
		bwReg.close();

		double avgExpPredictSmallerError = 0d;
		double avgExpPredictLargerError = 0d;
		double avgExpErrorAfterThresholdSmallerPrediction = 0d;
		double avgExpErrorAfterThresholdLargerPrediction = 0d;

		if (expPredictSmallerErrorCount > 0) {
			avgExpPredictSmallerError = expPredictSmallerErrorAmount / expPredictSmallerErrorCount;
		}
		if (expPredictLargerErrorCount > 0) {
			avgExpPredictLargerError = expPredictLargerErrorAmount / expPredictLargerErrorCount;
		}

		if (expErrorAfterThresholdSmallerPredictionCount > 0) {
			avgExpErrorAfterThresholdSmallerPrediction = expErrorAfterThresholdSmallerPredictionAmount
					/ expErrorAfterThresholdSmallerPredictionCount;
		}

		if (expErrorAfterThresholdLargerPredictionCount > 0) {
			avgExpErrorAfterThresholdLargerPrediction = expErrorAfterThresholdLargerPredictionAmount
					/ expErrorAfterThresholdLargerPredictionCount;
		}

		bwExpansionError.write(nextItr + ", " + expPredictSmallerErrorAmount + ", " + expPredictLargerErrorAmount + ", "
				+ expErrorAfterThresholdSmallerPredictionAmount + ", " + expErrorAfterThresholdLargerPredictionAmount
				+ ", " + classificationError + ", ");

		bwExpansionError.write(avgExpPredictSmallerError + ", " + avgExpPredictLargerError + ", "
				+ avgExpErrorAfterThresholdSmallerPrediction + ", " + avgExpErrorAfterThresholdLargerPrediction + ", ");

		bwExpansionError.write(
				expErrorAfterThresholdSmallerPredictionCount + ", " + expErrorAfterThresholdLargerPredictionCount);

		bwExpansionError.newLine();

		bwExpansionError.flush();

	}

	private void printFeaturesIntoDMatFileCls(Features baseStaticFeatures, SelectionFeatures selectionFeatures,
			int maxNumberOfStars, int paSelectedFromOracle, BufferedWriter bw) throws IOException {

		Object[] allFeatures = selectionFeatures.getSelectionFeaturesArray(
				baseStaticFeatures.getStaticFeaturesArray(maxNumberOfStars), maxNumberOfStars);

		float[] testingInstance = new float[allFeatures.length];

		for (int f = 0; f < (allFeatures.length - 1); f++) {
			if (allFeatures[f].toString().toLowerCase().equals("false")) {
				allFeatures[f] = "0";
			} else if (allFeatures[f].toString().toLowerCase().equals("true")) {
				allFeatures[f] = "1";
			}
			testingInstance[f + 1] = Float.parseFloat(allFeatures[f].toString());
		}
		testingInstance[0] = paSelectedFromOracle;

		bw.write(testingInstance[0] + " ");

		for (int i = 0; i < (testingInstance.length - 1); i++) {
			bw.write(i + ":" + testingInstance[i + 1] + " ");
		}

		bw.newLine();
	}

	private void printFeaturesIntoDMatFileReg(Features baseStaticFeatures, ExpansionFeatures expansionFeatures,
			int maxNumberOfStars, int paOracleExpansion, BufferedWriter bw) throws IOException {

		Object[] allFeatures = expansionFeatures.getExpansionFeaturesArray(
				baseStaticFeatures.getStaticFeaturesArray(maxNumberOfStars), maxNumberOfStars);

		float[] testingInstance = new float[allFeatures.length];

		for (int f = 0; f < (allFeatures.length - 1); f++) {
			if (allFeatures[f].toString().toLowerCase().equals("false")) {
				allFeatures[f] = "0";
			} else if (allFeatures[f].toString().toLowerCase().equals("true")) {
				allFeatures[f] = "1";
			}
			testingInstance[f + 1] = Float.parseFloat(allFeatures[f].toString());
		}
		testingInstance[0] = paOracleExpansion;

		bw.write(testingInstance[0] + " ");

		for (int i = 0; i < (testingInstance.length - 1); i++) {
			bw.write(i + ":" + testingInstance[i + 1] + " ");
		}

		bw.newLine();
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
				previousExpansionValue, -1);

		return expFeatures;
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

	private void CovertArffFileToDMatrix(String wekaTrainingFile, HashSet<Integer> trainingQueriesSet,
			String outputFilePath) throws Exception {

		FileInputStream fis = new FileInputStream(wekaTrainingFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		FileOutputStream fos = new FileOutputStream(outputFilePath);
		BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(fos));

		int nrow = 0;
		int ncol = 0;
		String line = null;
		ArrayList<Float> allFeatures = new ArrayList<Float>();
		while ((line = br.readLine()) != null) {
			String[] items = line.split(",");
			if (items.length > 1) {
				// first feature is query index
				int queryIndex = Integer.parseInt(items[0]);
				// if it's in the training folds.
				if (trainingQueriesSet.contains(queryIndex)) {
					nrow++;
					// all features - queryIndex
					float[] tempLine = new float[items.length - 1];

					if (ncol == 0) {
						ncol = items.length - 1;
					} else if (ncol != (items.length - 1)) {
						System.err.println("ncols != (items.length-1): " + ncol + " != " + (items.length - 1));
					}

					for (int i = 1; i < (items.length - 1); i++) {
						if (items[i].toLowerCase().equals("false")) {
							items[i] = "0";
						} else if (items[i].toLowerCase().equals("true")) {
							items[i] = "1";
						}
						tempLine[i] = Float.parseFloat(items[i]);
					}

					tempLine[0] = Float.parseFloat(items[items.length - 1]);

					brOut.write(tempLine[0] + " ");
					for (int i = 0; i < (tempLine.length - 1); i++) {
						// allFeatures.add(tempLine[i]);
						brOut.write(i + ":" + tempLine[i + 1] + " ");
					}
					brOut.newLine();

				}

			}
		}

		br.close();
		brOut.flush();
		brOut.close();

		// float[] floatArray = new float[allFeatures.size()];
		// int i = 0;
		//
		// for (Float f : allFeatures) {
		// floatArray[i++] = (f != null ? f : Float.NaN); // Or whatever
		// // default you want.
		// }
		//
		// DMatrix dmat = new DMatrix(floatArray, nrow, ncol);
		//
		// // we should do getLabel() to check if it's correct?
		// float[] lables = dmat.getLabel();
		//
		// for (int j = 0; j < lables.length; j++) {
		// System.out.println(lables[j] + ", ");
		// }

		/// return dmat;
	}

	public static void main(String[] args) throws Exception {
		BoostingLearning boostingLearning = new BoostingLearning();
		boostingLearning.initialize(args);

	}

}
