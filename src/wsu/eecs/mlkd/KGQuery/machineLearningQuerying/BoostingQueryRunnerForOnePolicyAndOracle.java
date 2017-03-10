package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
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
import wsu.eecs.mlkd.KGQuery.TopKQuery.NodeWithValue;
import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;
import wsu.eecs.mlkd.KGQuery.TopKQuery.TreeNode;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;
import wsu.eecs.mlkd.KGQuery.test.QueryFromFile;
import wsu.eecs.mlkd.KGQuery.test.QueryGenerator;

import weka.classifiers.Classifier;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.REPTree;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public class BoostingQueryRunnerForOnePolicyAndOracle {

	// build a training set and testing set
	// build a classifier by using all training set queries features
	// build a regresser by using all training set queries features
	// initalize a sf
	// run the first items call.
	// while not stop or not sf finish
	// ask classifier to select.
	// ask regressor for expansion.
	// do whatever they say.
	// save the current rank and duration for this

	private String PATTERNGRAPH_DB_PATH = "";

	private String queryFileName = "";
	private String queryFileDirectory = "";

	private String GName = ""; // Yago, DBPedia, ...

	private String GDirectory = "";
	private AnyTimeStarFramework sf;
	private AnyTimeStarFramework mlSF;
	private int k = 0;
	private GraphDatabaseService queryGraph;
	private GraphDatabaseService knowledgeGraph;
	private float alpha = 0.5F;
	private Levenshtein levenshtein;
	private HashMap<Integer, TreeNode<CalculationNode>> calcTreeNodeMap;
	private HashMap<Integer, CalculationTreeSiblingNodes> joinLevelSiblingNodesMap;
	private NeighborIndexing neighborIndexingInstance;
	private CacheServer cacheServer;
	private int startingQueryIndex;
	private String oracleSequenceFile;
	private int endingQueryIndex = 1000000;
	private CommonFunctions commonFunctions = new CommonFunctions();

	public BoostingQueryRunnerForOnePolicyAndOracle() {
		// internal use
	}

	// external use
	public BoostingQueryRunnerForOnePolicyAndOracle(GraphDatabaseService knowledgeGraph, String queryFileDirectory,
			String GDirectory, String GName, int startingQueryIndex, int endingQueryIndex, String PATTERNGRAPH_DB_PATH,
			int k, Levenshtein levenshtein, CacheServer cacheServer, NeighborIndexing neighborIndexingInstance) {
		this.neighborIndexingInstance = neighborIndexingInstance;
		this.knowledgeGraph = knowledgeGraph;
		this.queryFileDirectory = queryFileDirectory;
		this.GDirectory = GDirectory;
		this.GName = GName;
		this.startingQueryIndex = startingQueryIndex;
		this.endingQueryIndex = endingQueryIndex;
		this.PATTERNGRAPH_DB_PATH = PATTERNGRAPH_DB_PATH;
		this.k = k;
		this.levenshtein = levenshtein;
		this.cacheServer = cacheServer;
	}

	public void findSpeedUpAndQualityOutOfAClassifierRegressorForASet(Set<Integer> testSet, Predictor classifier,
			Predictor regressor, int numberOfSameExperiment, String resultFile, String oracleSequencePath,
			float[] selectionNormalizationFeaturesVector, float[] expansionNormalizationFeaturesVector)
			throws Exception {

		oracleSequenceFile = oracleSequencePath;

		File foutTime = new File(resultFile);
		FileOutputStream fosTime = new FileOutputStream(foutTime, true);
		BufferedWriter bwMLTime = new BufferedWriter(new OutputStreamWriter(fosTime));
		if (startingQueryIndex < 2) {
			bwMLTime.write(
					"queryIndex; sfDifferenceTime; mlDifferenceTime; totalSFDepth; totalMLSFDepth;totalAnswersDepth;sfQuality;mlSFQuality;featuresComputationalTime; mlStoppingError;sfStoppingError;mlEarlyStoppingError;mlFurtherStoppingError;classifierInferenceTime;regressionInferenceTime;extraMLComputationalTime;SF Depth; MLSF Depth;Answers Depth;numberOfClassificationCalls; expansionAvg");
			bwMLTime.newLine();
		}

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

				if (!testSet.contains(queryIndex)) {
					continue;
				}

				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH,
						queryFromFile);

				queryGraph = smallGraph;

				neighborIndexingInstance.queryNeighborIndexer(queryGraph);

				System.out.println("queryfileName: " + queryFileName + ", queryIndex: " + queryIndex + " k: " + k);
				double start_time, end_time;
				try (Transaction tx1 = queryGraph.beginTx()) {
					try (Transaction tx2 = knowledgeGraph.beginTx()) {
						int numberOfQNodes = neighborIndexingInstance.queryNodeIdSet.size();
						int numberOfQRelationships = 0;
						for (Relationship rel : GlobalGraphOperations.at(queryGraph).getAllRelationships()) {
							numberOfQRelationships++;
						}

						Double diffTime = 0d;
						ArrayList<Double> differenceTimes = new ArrayList<Double>();
						Double sfDifferenceTime = 0d;
						InfoHolder[] infoHolders = new InfoHolder[numberOfSameExperiment];
						for (int i = 0; i < infoHolders.length; i++) {
							infoHolders[i] = new InfoHolder();
						}

						for (int exp = 0; exp < numberOfSameExperiment; exp++) {
							sf = getNewStarFrameworkInstance();
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
						sfDifferenceTime = Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimes,
								numberOfSameExperiment);

						differenceTimes.clear();

						// ArrayList<Double> overallMLComputationalTimes = new
						// ArrayList<Double>();
						// Double overallMLComputationalTime = 0d;
						// double overallMLComputationStartTime,
						// overallMLComputationDuration = 0d;

						ArrayList<Double> classifierInferenceTimes = new ArrayList<Double>();
						Double classifierInferenceTime = 0d;
						double classifierInferenceStartTime, classifierInferenceDuration = 0d;

						ArrayList<Double> regressionInferenceTimes = new ArrayList<Double>();
						Double regressionInferenceTime = 0d;
						double regressionInferenceStartTime, regressionInferenceDuration = 0d;

						ArrayList<Double> featuresComputationalTimes = new ArrayList<Double>();
						Double featuresComputationalTime = 0d;
						double featuresComputationalStartTime, featuresComputationalDuration = 0d;

						ArrayList<Double> extraMLComputationalTimes = new ArrayList<Double>();
						Double extraMLComputationalTime = 0d;
						double extraMLComputationalStartTime, extraMLComputationalDuration = 0d;

						ArrayList<Integer> expansions = new ArrayList<Integer>();
						int numberOfClassifierCalls = 0;

						Double mlDifferenceTime = 0d;

						ArrayList<Integer> oracleSelectionSteps = new ArrayList<Integer>();
						ArrayList<Integer> oracleExpansionSteps = new ArrayList<Integer>();
						commonFunctions.setOracleSteps(oracleSequenceFile, queryIndex, oracleSelectionSteps,
								oracleExpansionSteps);

						for (int exp = 0; exp < numberOfSameExperiment; exp++) {
							mlSF = getNewStarFrameworkInstance();

							int numberOfStars = mlSF.starQueries.size();
							int numberOfCalcNodes = mlSF.calcTreeNodeMap.size();

							int maxNumberOfStars = DummyProperties.maxNumberOfSQ;
							int maxNumberOfCalcNodes = 2 * DummyProperties.maxNumberOfSQ - 1;

							Features baseStaticFeatures = null;

							int depthJoinLevel = 0;

							start_time = System.nanoTime();
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

							featuresComputationalStartTime = System.nanoTime();

							baseStaticFeatures = initStaticFeatures(queryIndex, mlSF, numberOfQNodes,
									numberOfQRelationships, numberOfStars, maxNumberOfStars, maxNumberOfCalcNodes);

							featuresComputationalDuration += (System.nanoTime() - featuresComputationalStartTime);

							BaseFeatures baseFeatures = null;
							SelectionFeatures selectionFeatures = null;
							ExpansionFeatures expansionFeatures = null;
							// SelectionFeatures stoppingFeatures = null;
							depthJoinLevel = 0;
							int paExpansion = 1;
							int level = numberOfStars;
							while (!mlSF.anyTimeAlgorithmShouldFinish()) {
								// System.out.println("level: " + level);

								int paSelected = 0;

								featuresComputationalStartTime = System.nanoTime();

								selectionFeatures = computeSelectionFeatures(queryIndex, mlSF, level, numberOfStars,
										numberOfCalcNodes, baseFeatures, baseStaticFeatures, maxNumberOfStars,
										maxNumberOfCalcNodes);

								featuresComputationalDuration += (System.nanoTime() - featuresComputationalStartTime);

								if (classifier != null) {
									classifierInferenceStartTime = System.nanoTime();

									paSelected = boosterClassifyInstance(classifier, baseStaticFeatures,
											selectionFeatures, maxNumberOfStars, selectionNormalizationFeaturesVector);

									classifierInferenceDuration += (System.nanoTime() - classifierInferenceStartTime);

								} else {
									// selection will be done by oracle.
									if (level >= oracleSelectionSteps.size()) {
										break;
									}
									paSelected = oracleSelectionSteps.get(level);
								}

								numberOfClassifierCalls++;

								// System.out.println("paSelected: " +
								// paSelected);

								if (paSelected < 1) {
									break;
								}
								// reaching to the queries time-bound
								if ((System.nanoTime() - start_time) / 1e6 > 100000) {
									break;
								}

								extraMLComputationalStartTime = System.nanoTime();
								// if selected SQ stark is enough?
								if (mlSF.calcTreeStarQueriesNodeMapBySQMLIndex.get(paSelected) == null
										|| mlSF.calcTreeStarQueriesNodeMapBySQMLIndex
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
								extraMLComputationalDuration += (System.nanoTime() - extraMLComputationalStartTime);

								// TODO: if any update needed after changing the
								// selectedPA.
								featuresComputationalStartTime = System.nanoTime();

								expansionFeatures = computeExpansionFeatures(queryIndex, mlSF, level, numberOfStars,
										numberOfCalcNodes, baseFeatures, selectionFeatures, paSelected,
										baseStaticFeatures);

								featuresComputationalDuration += (System.nanoTime() - featuresComputationalStartTime);

								// expansionFeatures.print(baseStaticFeatures,
								// bwMLGenExpFeatures);

								if (regressor != null) {
									regressionInferenceStartTime = System.nanoTime();

									paExpansion = boosterPredictInstance(regressor, baseStaticFeatures, paSelected,
											expansionFeatures, maxNumberOfStars, expansionNormalizationFeaturesVector);

									regressionInferenceDuration += (System.nanoTime() - regressionInferenceStartTime);
									// System.out.println("paExpansion: " +
									// paExpansion);

								} else {
									// expansion will be done by oracle.
									if (level >= oracleExpansionSteps.size()) {
										paExpansion = k * 3;
									} else {
										paExpansion = oracleExpansionSteps.get(level);
									}
								}

								expansions.add(paExpansion);

								featuresComputationalStartTime = System.nanoTime();

								baseFeatures = baseFeatureFiller(queryIndex, selectionFeatures, mlSF, numberOfStars,
										numberOfCalcNodes, baseFeatures, paSelected, paExpansion);

								featuresComputationalDuration += (System.nanoTime() - featuresComputationalStartTime);

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

//								if (level % 2000 == 0) {
//									System.out.println("level: " + level);
//								}
							}

							end_time = System.nanoTime();

							diffTime = (end_time - start_time) / 1e6;
							System.out.println("ML StarFramework exp: " + exp + " is finished in " + diffTime.intValue()
									+ " miliseconds!");

							differenceTimes.add(diffTime);

							featuresComputationalTimes.add((featuresComputationalDuration / 1e6));
							classifierInferenceTimes.add((classifierInferenceDuration / 1e6));
							regressionInferenceTimes.add((regressionInferenceDuration / 1e6));
							extraMLComputationalTimes.add((extraMLComputationalDuration / 1e6));

							if (exp != (numberOfSameExperiment - 1)) {
								mlSF = null;
							} else {
								mlDifferenceTime = Dummy.DummyFunctions.computeNonOutlierAverage(differenceTimes,
										numberOfSameExperiment);

								featuresComputationalTime = Dummy.DummyFunctions
										.computeNonOutlierAverage(featuresComputationalTimes, numberOfSameExperiment);

								classifierInferenceTime = Dummy.DummyFunctions
										.computeNonOutlierAverage(classifierInferenceTimes, numberOfSameExperiment);

								regressionInferenceTime = Dummy.DummyFunctions
										.computeNonOutlierAverage(regressionInferenceTimes, numberOfSameExperiment);

								extraMLComputationalTime = Dummy.DummyFunctions
										.computeNonOutlierAverage(extraMLComputationalTimes, numberOfSameExperiment);

								System.out.println(
										"ML StarFramework avg is finished in " + mlDifferenceTime + " miliseconds!");

							}
						}
						saveTheResults(queryIndex, bwMLTime, sf, sfDifferenceTime, mlSF, mlDifferenceTime,
								featuresComputationalTime, classifierInferenceTime, regressionInferenceTime,
								extraMLComputationalTime, numberOfClassifierCalls, expansions);
						sf = null;
						mlSF = null;

						System.gc();
						System.runFinalization();

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

		bwMLTime.flush();
		bwMLTime.close();

		// bwMLGenSelFeatures.flush();
		// bwMLGenSelFeatures.close();
		// bwMLGenExpFeatures.flush();
		// bwMLGenExpFeatures.close();

	}

	private Set<Integer> readQueryIndexFromSelectionFeaturesSet(String filePath) throws Exception {
		Set<Integer> queries = new HashSet<Integer>();
		FileInputStream fis = new FileInputStream(filePath);

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		while ((line = br.readLine()) != null) {
			queries.add(Integer.parseInt(line.split(";")[0]));
		}
		br.close();

		return queries;
	}

	private void saveTheResults(int queryIndex, BufferedWriter bwMLTime, AnyTimeStarFramework sf,
			Double sfDifferenceTime, AnyTimeStarFramework mlSF, Double mlDifferenceTime,
			Double featuresComputationalTime, Double classifierInferenceTime, Double regressionInferenceTime,
			Double extraMLComputationalTime, int numberOfClassifierCalls, ArrayList<Integer> expansions)
			throws Exception {

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

		bwMLTime.write(queryIndex + ";" + sfDifferenceTime + ";" + mlDifferenceTime + ";" + totalSFDepth + ";"
				+ totalMLSFDepth + ";" + totalAnswersDepth + ";" + sfQuality + ";" + mlSFQuality + ";"
				+ featuresComputationalTime + ";" + mlStoppingError + ";" + sfStoppingError + ";" + mlEarlyStoppingError
				+ ";" + mlFurtherStoppingError + ";" + classifierInferenceTime + ";" + regressionInferenceTime + ";"
				+ extraMLComputationalTime + ";" + posfix + numberOfClassifierCalls + ";" + avgExpansion);
		bwMLTime.newLine();
		bwMLTime.flush();

	}

	private int predictInstance(Classifier regressor, Features baseStaticFeatures, int paSelected,
			ExpansionFeatures expansionFeatures, FastVector fvWekaRegressionAttributes,
			double[] expansionNormalizationFeaturesVector) throws Exception {
		Instances testingInstance = commonFunctions.createRegressionTestingInstance(fvWekaRegressionAttributes,
				baseStaticFeatures, expansionFeatures, DummyProperties.maxNumberOfSQ,
				expansionNormalizationFeaturesVector);

		return (int) regressor.classifyInstance(testingInstance.firstInstance());
	}

	private int classifyInstance(Classifier classifier, Features baseStaticFeatures,
			SelectionFeatures selectionFeatures, FastVector fvWekaAttributes, HashMap<Integer, Integer> classValMap,
			double[] selectionNormalizationFeaturesVector) throws Exception {

		Instances testingInstance = commonFunctions.createClassificationTestingInstance(fvWekaAttributes,
				baseStaticFeatures, selectionFeatures, DummyProperties.maxNumberOfSQ,
				selectionNormalizationFeaturesVector);
		return classValMap.get((int) classifier.classifyInstance(testingInstance.firstInstance()));

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
}
