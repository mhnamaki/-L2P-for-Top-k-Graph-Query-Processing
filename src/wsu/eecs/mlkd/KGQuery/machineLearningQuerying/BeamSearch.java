package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Delayed;

import org.apache.commons.lang3.NotImplementedException;
import org.neo4j.graphdb.GraphDatabaseService;

import scala.sys.process.ProcessBuilderImpl.Dummy;
import wsu.eecs.mlkd.KGQuery.TopKQuery.AnyTimeStarFramework;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CacheServer;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CalculationNode;
import wsu.eecs.mlkd.KGQuery.TopKQuery.CalculationTreeSiblingNodes;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;
import wsu.eecs.mlkd.KGQuery.TopKQuery.GraphResult;
import wsu.eecs.mlkd.KGQuery.TopKQuery.InfoHolder;
import wsu.eecs.mlkd.KGQuery.TopKQuery.NeighborIndexing;
import wsu.eecs.mlkd.KGQuery.TopKQuery.NodeWithValue;
import wsu.eecs.mlkd.KGQuery.TopKQuery.StarFramework;
import wsu.eecs.mlkd.KGQuery.TopKQuery.TreeNode;
import wsu.eecs.mlkd.KGQuery.machineLearningQuerying.BeamSearchRunner.MinObjectiveFunction;
import wsu.eecs.mlkd.KGQuery.machineLearningQuerying.BeamSearchRunner.WhichOracle;

public class BeamSearch {
	public final int MAX_MEMORY_LIMIT = 1000000;
	public int beamSize = 0;
	public int delta = 0;
	public final int TETA = 1;
	public int maxDelta;
	public WhichOracle oracle;
	public BeamSearchRunner beamSearchRunner;
	public HashMap<Integer, Integer> calcTreeNodeStarQueryMaxDepthMap;
	public HashMap<Integer, Integer> maxAnyTimeAnswerDepthStarQueryMap;
	public double trueRank;
	public int numberOfQNodes;
	public int numberOfQRelationships;
	public boolean oracleShouldStop = false;
	public double maxDistance;
	boolean shouldFinish = false;
	public MinObjectiveFunction rankingFunction;
	int[] deltas = { 20, 60, 100, 140, 180, 200 };
	private double rankCoeff = 1;
	private double timeCoeff = 1;
	private double depthCoeff = 1;
	// public AnyTimeStarFramework initializedStarFramwork;

	public enum UpdateInfoEnum {
		YES, NO
	};

	public BeamSearchNode lastBeamSearchNode;
	private double SFTotalTime;
	private InfoHolder infoHolder;

	public BeamSearch(GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph, int beamSize, int delta,
			WhichOracle oracle, BeamSearchRunner beamSearchRunner, NeighborIndexing neighborIndexingInstance,
			CacheServer cacheServer, HashMap<Integer, Integer> maxAnyTimeAnswerDepthStarQueryMap, double trueRank,
			int numberOfQNodes, int numberOfQRelationships, MinObjectiveFunction rankingFunction, double rankCoeff,
			double timeCoeff, double depthCoeff, double SFTotalTime, InfoHolder infoHolder) throws Exception {
		this.beamSearchRunner = beamSearchRunner;
		this.delta = delta;
		this.oracle = oracle;
		this.beamSize = beamSize;
		this.maxAnyTimeAnswerDepthStarQueryMap = maxAnyTimeAnswerDepthStarQueryMap;
		this.trueRank = trueRank;
		this.numberOfQNodes = numberOfQNodes;
		this.numberOfQRelationships = numberOfQRelationships;
		this.rankingFunction = rankingFunction;
		this.rankCoeff = rankCoeff;
		this.timeCoeff = timeCoeff;
		this.depthCoeff = depthCoeff;
		this.SFTotalTime = SFTotalTime;
		this.infoHolder = infoHolder;

		maxDelta = delta * 10;

		maxDistance = getMaxPossibleDistance();
		initialize(queryGraph, knowledgeGraph, neighborIndexingInstance, cacheServer);

		// beamSearch();
	}

	private void initialize(GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph,
			NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) throws Exception {

		TreeNode<CalculationNode> tempNode = beamSearchRunner.starFramework2.rootTreeNode;
		calcTreeNodeStarQueryMaxDepthMap = new HashMap<Integer, Integer>();

		if (tempNode.getData().isStarQuery) {
			calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getData().nodeIndex, tempNode.getData().depthOfDigging);
		}

		while (tempNode != null) {
			if (tempNode.getRightChild() != null && tempNode.getRightChild().getData().isStarQuery) {
				calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getRightChild().getData().nodeIndex,
						tempNode.getRightChild().getData().depthOfDigging);
			}
			if (tempNode.getLeftChild() != null && tempNode.getLeftChild().getData().isStarQuery) {
				calcTreeNodeStarQueryMaxDepthMap.put(tempNode.getLeftChild().getData().nodeIndex,
						tempNode.getLeftChild().getData().depthOfDigging);
			}
			tempNode = tempNode.getLeftChild();
		}

		// beam search init
		beamSearchRunner.starFramework2 = null;

		BeamSearchNode startNode = rootHandling(queryGraph, knowledgeGraph, neighborIndexingInstance, cacheServer);

		lastBeamSearchNode = beamSearch(queryGraph, knowledgeGraph, startNode, beamSize, delta, oracle,
				neighborIndexingInstance, cacheServer);

	}

	private BeamSearchNode rootHandling(GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph,
			NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) throws Exception {

		ArrayList<ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>> generalArr = new ArrayList<ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>>();

		ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched> rootInsideArr = new ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>();

		String sequenceForDebug = "";
		for (Integer starQueryNodeIndex : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
			rootInsideArr.add(new StarQueryIndexWithNumberOfPartialAnswerToBeFetched(starQueryNodeIndex, 1));
			sequenceForDebug += "< " + DummyFunctions.getSQIndexByCalcNodeIndexAndNumberOfStars(starQueryNodeIndex,
					calcTreeNodeStarQueryMaxDepthMap.size()) + ": " + 1 + " >, ";
		}

		generalArr.add(rootInsideArr);

		BeamSearchNode startNode = new BeamSearchNode(null, generalArr);
		startNode.sequenceForDebug = sequenceForDebug;
		startNode.starframework = beamSearchRunner.getNewStarFrameworkInstance();

		// runIncrementally(queryGraph, knowledgeGraph, startNode,
		// UpdateInfoEnum.YES, neighborIndexingInstance,
		// cacheServer);

		return startNode;
	}

	private void printNodeInfo(BeamSearchNode node) {
		// for debug start
		if (DummyProperties.debuggMode) {
			node.starframework.getHowMuchResultAreInCalcNodes();
			System.out.println("level: " + node.level);
			System.out.println("rank: " + node.rank);
			System.out.println("trueRank: " + trueRank);
			System.out.println("targetDistance: " + node.targetDistance);
			System.out.println("maxDistance: " + maxDistance);
			// System.out.println("diffTime: " + node.diffTime);

			// System.out.println("diffTerm: " + node.diffTime);
			System.out.println("farAwayPenalty: " + node.farAwayPenalty);
			System.out.println("minObjective: " + node.minObjective);

			System.out.println("rankCoeff: " + rankCoeff);
			System.out.println("(trueRank - node.rank) / trueRank : " + (trueRank - node.rank) / trueRank);
			System.out.println("timeCoeff: " + timeCoeff);
			System.out.println(
					"(node.totalTimeInMilliseconds / SFTotalTime): " + node.totalTimeInMilliseconds / SFTotalTime);
			System.out.println("depthCoeff: " + depthCoeff);
			System.out.println("(node.targetDistance / maxDistance): " + node.targetDistance / maxDistance);

			System.out.println();
		}

	}

	public BeamSearchNode beamSearch(GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph,
			BeamSearchNode startNode, int beamSize, int delta, WhichOracle oracle,
			NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) throws Exception {
		// initialization
		// int g = 0;

		// because we dont' have loop in this case
		// HashSet<Node> visitedNodesSet = new HashSet<Node>();
		// visitedNodesSet.add(startNode);

		HashSet<BeamSearchNode> beamNodesSet = new HashSet<BeamSearchNode>();
		beamNodesSet.add(startNode);
		if (goalTest(queryGraph, knowledgeGraph, startNode, neighborIndexingInstance, cacheServer)) {
			return startNode;
		}

		/* main loop */
		while (!beamNodesSet.isEmpty()) {
			HashSet<BeamSearchNode> set = new HashSet<BeamSearchNode>();

			/* generate the SET nodes */
			for (BeamSearchNode node : beamNodesSet) {
				if ((node.level % 100) == 0) {
					System.out.println("node.level:" + node.level + " target distance: " + node.targetDistance);
				}
				ArrayList<BeamSearchNode> successors = generateSuccessors(node, delta, oracle);
				for (BeamSearchNode successor : successors) {
					if (goalTest(queryGraph, knowledgeGraph, successor, neighborIndexingInstance, cacheServer)) {
						createACopiedOfStarFramework(queryGraph, knowledgeGraph, successor, neighborIndexingInstance,
								cacheServer);
						return successor;
					}
					set.add(successor);
				}
			}

			beamNodesSet = new HashSet<BeamSearchNode>();
			// g = g + 1;

			/* fill the BEAM for the next loop */
			while ((!set.isEmpty()) && (beamNodesSet.size() < beamSize)) {
				BeamSearchNode betterState = getTheBestSuccessor(set);
				createACopiedOfStarFramework(queryGraph, knowledgeGraph, betterState, neighborIndexingInstance,
						cacheServer);
				set.remove(betterState);

				// if (!visitedNodesSet.contains(set)) {
				// if (visitedNodesSet.size() >= MAX_MEMORY_LIMIT) {
				// return Integer.MAX_VALUE;
				// }
				// visitedNodesSet.add(betterState);
				if (betterState.parentNode != null) {

					if (betterState.parentNode.parentNode != null)
						betterState.parentNode.parentNode.starframework = null;

					betterState.parentNode.parentNode = null; // solving memory
																// leakage
																// problem
				}
				beamNodesSet.add(betterState);
				// }
			}

			for (BeamSearchNode uselessNode : set) {
				DummyFunctions.printIfItIsInDebuggedMode("uselessNode: " + uselessNode.sequenceForDebug);
				if (uselessNode.parentNode != null) {
					uselessNode.parentNode.parentNode = null;
				}
				uselessNode.parentNode = null;// solving memory leakage problem
				uselessNode.starframework = null;// solving memory leakage
													// problem
			}
			// for debug start
			if (DummyProperties.debuggMode) {
				String beamNodesFinalResult = "";
				for (BeamSearchNode node : beamNodesSet) {
					beamNodesFinalResult += "node level: " + node.level + " > ";
					for (Integer index : node.resultPartialAnswersShouldBeByNodeIndex.keySet()) {
						beamNodesFinalResult += " (" + index + ", "
								+ node.resultPartialAnswersShouldBeByNodeIndex.get(index) + ") ";
					}
					beamNodesFinalResult += "\n";
				}
				System.out.println("beamNodesFinalResult: \n " + beamNodesFinalResult);
			}
			// for debug end

		}

		return null;
	}

	private void createACopiedOfStarFramework(GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph,
			BeamSearchNode betterState, NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) {
		// this node is selected to be in the beam
		betterState.starframework = betterState.parentNode.starframework.copy(queryGraph, knowledgeGraph);
		runIncrementally(queryGraph, knowledgeGraph, betterState, UpdateInfoEnum.NO, neighborIndexingInstance,
				cacheServer, 0);
	}

	private BeamSearchNode getTheBestSuccessor(HashSet<BeamSearchNode> set) {
		BeamSearchNode tempNode = null;

		for (BeamSearchNode successorNode : set) {
			if (tempNode == null) {
				tempNode = successorNode;
			} else {
				if (successorNode.minObjective < tempNode.minObjective) {
					tempNode = successorNode;
				}
			}
		}
		return tempNode;

	}

	private boolean goalTest(GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph,
			BeamSearchNode successor, NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) {

		if (DummyProperties.debuggMode) {
			String beamNodesFinalResult = "node level: " + successor.level + " > ";
			for (Integer index : successor.resultPartialAnswersShouldBeByNodeIndex.keySet()) {
				beamNodesFinalResult += " (" + index + ", "
						+ successor.resultPartialAnswersShouldBeByNodeIndex.get(index) + ") ";
			}
			// beamNodesFinalResult += "\n";
			System.out.println(beamNodesFinalResult);
		}

		ArrayList<Double> differenceTimes = new ArrayList<Double>();
		for (int exp = 0; exp < beamSearchRunner.numberOfSameExperiment; exp++) {

			differenceTimes.add(runIncrementally(queryGraph, knowledgeGraph, successor, UpdateInfoEnum.YES,
					neighborIndexingInstance, cacheServer, exp));

			if (exp == beamSearchRunner.numberOfSameExperiment - 1) {
				double avgDiffTime = DummyFunctions.computeNonOutlierAverage(differenceTimes,
						beamSearchRunner.numberOfSameExperiment);

				updateNodeInfo(successor, avgDiffTime);

				if (allStarsDiggedMore(successor) || (trueRank - successor.rank) <= 0) {
					shouldFinish = true;
				}
				// for debug start
				DummyFunctions.printIfItIsInDebuggedMode("sequence: " + successor.sequenceForDebug);
				printNodeInfo(successor);
				// System.out.println();

				// for debug end

			}
			if (successor.level != 0 && !shouldFinish)
				successor.starframework = null;

		}
		return shouldFinish;
	}

	private double runIncrementally(GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph,
			BeamSearchNode successor, UpdateInfoEnum shouldUpdateNodeInfo, NeighborIndexing neighborIndexingInstance,
			CacheServer cacheServer, int exp) {

		ArrayList<HashMap<Integer, Integer>> fetchSeqByStarQueryIndex = new ArrayList<HashMap<Integer, Integer>>();
		// ma dge az aval run nemikonim az level e qabl b in var.

		ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched> levelArr = successor.generalArrNumberOfFetches
				.get(successor.level);

		ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched> prevLevelArr;

		if (successor.level == 0) {
			prevLevelArr = new ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>();
			for (Integer nodeIndex : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
				prevLevelArr.add(new StarQueryIndexWithNumberOfPartialAnswerToBeFetched(nodeIndex, 0));
			}
		} else {
			prevLevelArr = successor.generalArrNumberOfFetches.get(successor.level - 1);
		}

		HashMap<Integer, Integer> queryFetchMap = new HashMap<Integer, Integer>();
		for (int index = 0; index < levelArr.size(); index++) {
			int diff = levelArr.get(index).numberOfPartialAnswersShouldBeFetched
					- prevLevelArr.get(index).numberOfPartialAnswersShouldBeFetched;
			if (diff > 0) {
				queryFetchMap.put(levelArr.get(index).starQueryIndex, diff);
			}

		}
		fetchSeqByStarQueryIndex.add(queryFetchMap);

		// double diffTime = Double.MAX_VALUE;

		if (successor.starframework == null && successor.parentNode != null) {
			successor.starframework = successor.parentNode.starframework.copy(queryGraph, knowledgeGraph);
		} else if (successor.parentNode == null) {
			successor.starframework = beamSearchRunner.getNewStarFrameworkInstance();
		}
		double sFetchTime, sJoinTime, sCheckFinsihTime;
		double starTime = System.nanoTime();
		for (HashMap<Integer, Integer> fetchByQueryIndex : fetchSeqByStarQueryIndex) {
			int depthJoinLevel = 0;
			sFetchTime = System.nanoTime();
			for (Integer queryIndexToBeFetched : fetchByQueryIndex.keySet()) {

				TreeNode<CalculationNode> thisCalcNode = successor.starframework.calcTreeNodeMap
						.get(queryIndexToBeFetched);
				thisCalcNode.getData().numberOfPartialAnswersShouldBeFetched = fetchByQueryIndex
						.get(queryIndexToBeFetched);

				if (shouldUpdateNodeInfo == UpdateInfoEnum.NO) {
					infoHolder.oracleFetchesCalls++;
					infoHolder.oracleTotalRequestForFetches += fetchByQueryIndex.get(queryIndexToBeFetched);
				}

				successor.starframework.anyTimeStarkForLeaf(knowledgeGraph, thisCalcNode, neighborIndexingInstance,
						cacheServer);
				depthJoinLevel = thisCalcNode.levelInCalcTree - 1;

			}

			if (shouldUpdateNodeInfo == UpdateInfoEnum.NO || (successor.level == 0 && exp == 1)) {
				infoHolder.oracleFetchesTime += ((System.nanoTime() - sFetchTime) / 1e6);
			}

			sJoinTime = System.nanoTime();
			for (; depthJoinLevel >= 0; depthJoinLevel--) {
				CalculationTreeSiblingNodes calculationTreeSiblingNodes = successor.starframework.joinLevelSiblingNodesMap
						.get(depthJoinLevel);

				if (shouldUpdateNodeInfo == UpdateInfoEnum.NO) {
					infoHolder.oracleJoinCalls++;
				}
				successor.starframework.anyTimeTwoWayHashJoin(calculationTreeSiblingNodes.leftNode,
						calculationTreeSiblingNodes.rightNode, successor.starframework.k);
			}
			depthJoinLevel = 0;
			if (shouldUpdateNodeInfo == UpdateInfoEnum.NO || (successor.level == 0 && exp == 1)) {
				infoHolder.oracleJoinTime += ((System.nanoTime() - sJoinTime) / 1e6);
			}

		}

		sCheckFinsihTime = System.nanoTime();
		shouldFinish = successor.starframework.anyTimeAlgorithmShouldFinish();
		if (shouldUpdateNodeInfo == UpdateInfoEnum.NO || (successor.level == 0 && exp == 1)) {
			infoHolder.oracleCheckShouldFinishTime += ((System.nanoTime() - sCheckFinsihTime) / 1e6);
		}

		return (System.nanoTime() - starTime) / 1e6;

		// return shouldFinish;

	}

	private boolean allStarsDiggedMore(BeamSearchNode successor) {
		int numberOfStars = maxAnyTimeAnswerDepthStarQueryMap.keySet().size();
		int cnt = 0;
		for (Integer sqIndex : maxAnyTimeAnswerDepthStarQueryMap.keySet()) {
			int currentDepth = successor.resultPartialAnswersShouldBeByNodeIndex.get(sqIndex);
			int correctDepth = maxAnyTimeAnswerDepthStarQueryMap.get(sqIndex);
			if (currentDepth >= correctDepth) {
				cnt++;
			}
		}
		if (cnt >= numberOfStars) {
			return true;
		}
		return false;
	}

	private void updateNodeInfo(BeamSearchNode successor, double diffTime) {
		if (successor.parentNode != null) {
			successor.totalTimeInMilliseconds += successor.parentNode.totalTimeInMilliseconds;
		}
		successor.diffTime = diffTime;
		successor.totalTimeInMilliseconds += diffTime;
		successor.rank = getCurrentRank(successor);
		successor.farAwayPenalty = getFarAwayPentaly(successor);
		successor.targetDistance = getTargetDistance(successor);

		// ?

		// if (rankingFunction == MinObjectiveFunction.DiffTime) {
		//
		// // based on the diffTime
		// successor.minObjective = (trueRank - successor.rank) + 5 *
		// successor.diffTime + successor.farAwayPenalty
		// + 70 * (successor.targetDistance / maxDistance);
		//
		// } else if (rankingFunction == MinObjectiveFunction.JustDepth) {
		//
		// // just based on the rank and distance to the target depth
		// successor.minObjective = 10 * (successor.rank / trueRank) +
		// successor.farAwayPenalty
		// + 10 * (successor.targetDistance / maxDistance);
		//
		// } else if (rankingFunction == MinObjectiveFunction.TotalTime) {

		// based on the totalMilisecond
		successor.minObjective = rankCoeff * ((trueRank - successor.rank) / trueRank)
				+ timeCoeff * (successor.totalTimeInMilliseconds / SFTotalTime) + successor.farAwayPenalty
				+ depthCoeff * (successor.targetDistance / maxDistance);

		// }

	}

	private double getMaxPossibleDistance() {
		double maxDepthDistance = 0d;
		for (Integer sqIndex : maxAnyTimeAnswerDepthStarQueryMap.keySet()) {
			maxDepthDistance += maxAnyTimeAnswerDepthStarQueryMap.get(sqIndex);
		}
		return maxDepthDistance;
	}

	private double getTargetDistance(BeamSearchNode successor) {
		double depthDistance = 0d;
		for (Integer sqIndex : maxAnyTimeAnswerDepthStarQueryMap.keySet()) {
			int currentDepth = successor.resultPartialAnswersShouldBeByNodeIndex.get(sqIndex);
			int correctDepth = maxAnyTimeAnswerDepthStarQueryMap.get(sqIndex);
			if (currentDepth < correctDepth)
				depthDistance += correctDepth - currentDepth;
		}
		return depthDistance;
	}

	private double getFarAwayPentaly(BeamSearchNode successor) {
		double penalty = 0d;
		for (Integer sqIndex : maxAnyTimeAnswerDepthStarQueryMap.keySet()) {
			int currentDepth = successor.resultPartialAnswersShouldBeByNodeIndex.get(sqIndex);
			int correctDepth = maxAnyTimeAnswerDepthStarQueryMap.get(sqIndex);
			if (currentDepth > correctDepth)
				penalty += currentDepth - correctDepth;
		}
		return penalty;
	}

	private double getCurrentRank(BeamSearchNode successor) {
		double rankDistance = DummyFunctions.getRank(successor.starframework.anyTimeResults);
		// if (rankDistance < 0) {
		// System.err.println("rankDistance: " + rankDistance);
		// }
		return rankDistance;
	}

	private double getNodeQuality(BeamSearchNode successor) {
		// it can be various version of the quality computer.

		// Sigma over all partial/ semi complete/ complete answers scores
		// float totalQuality = 0;

		// float sum = 0;
		// for (Integer index : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
		// float diff = (calcTreeNodeStarQueryMaxDepthMap.get(index)
		// - successor.resultPartialAnswersShouldBeByNodeIndex.get(index));
		// if (diff < 0) {
		// diff = 2 * diff;
		// }
		// sum += diff;
		//
		// }
		// if (sum != 0) {
		// totalQuality = 1000 / sum;
		// } else {
		// totalQuality = Float.MAX_VALUE;
		// }

		// just star queries
		// for (Integer nodeIndex : calcTreeNodeStarQueryMaxDepthMap.keySet()) {
		// TreeNode<CalculationNode> thisCalcNode =
		// beamSearchRunner.calcTreeNodeMap.get(nodeIndex);
		// for (GraphResult graphResult : thisCalcNode.getData().graphResult) {
		// totalQuality += graphResult.getTotalValue();
		// }
		// }

		// all query nodes
		// for (Integer nodeIndex : beamSearchRunner.calcTreeNodeMap.keySet()) {
		// TreeNode<CalculationNode> thisCalcNode =
		// beamSearchRunner.calcTreeNodeMap.get(nodeIndex);
		// for (GraphResult graphResult : thisCalcNode.getData().graphResult) {
		// totalQuality += graphResult.getTotalValue();
		// }
		// }

		///////////// sum distinct node ids by considering how many occurance in
		///////////// all ////////
		double totalQuality = 0;
		for (Integer starNodeIndex : successor.starframework.calcTreeStarQueriesNodeMap.keySet()) {
			// get star query calc node
			TreeNode<CalculationNode> starCalcNode = successor.starframework.calcTreeStarQueriesNodeMap
					.get(starNodeIndex);
			double totalQualityForStarQuery = 0;

			// for each query node
			for (org.neo4j.graphdb.Node qNode : starCalcNode.getData().allThisQueryGraphNodes) {

				HashSet<Long> visitedNodeIds = new HashSet<Long>();

				// for each result in star query node
				for (GraphResult gr : starCalcNode.getData().graphResult) {

					// for the corresponding qNode
					NodeWithValue gNode = gr.assembledResult.get(qNode);
					if (!visitedNodeIds.contains(gNode.node.getId())) {
						totalQualityForStarQuery += gNode.simValue
								/ successor.starframework.queryNodeIntersectMap.get(qNode);
						visitedNodeIds.add(gNode.node.getId());
					}
				}

			}

			totalQuality += totalQualityForStarQuery;

		}
		return totalQuality;
	}

	private ArrayList<BeamSearchNode> generateSuccessors(BeamSearchNode node, int delta, WhichOracle oracle)
			throws Exception {
		ArrayList<BeamSearchNode> successorNodesSet = new ArrayList<BeamSearchNode>();
		// if (node.parentNode == null) { // if this is the first level.
		// for (int curDelta = delta; curDelta <= maxDelta; curDelta += delta) {
		// ArrayList<ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>>
		// arr = new
		// ArrayList<ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>>();
		// for (ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>
		// foo : node.generalArrNumberOfFetches) {
		// arr.add((ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>)
		// foo.clone());
		// }
		//
		// String sequenceForDebug = "";
		// ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>
		// insideArr = new
		// ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>();
		// for (Integer starQueryIndex :
		// maxAnyTimeAnswerDepthStarQueryMap.keySet()) {
		// int d = Math.min(curDelta,
		// maxAnyTimeAnswerDepthStarQueryMap.get(starQueryIndex));
		// insideArr.add(new
		// StarQueryIndexWithNumberOfPartialAnswerToBeFetched(starQueryIndex,
		// d));
		// sequenceForDebug += " <" + starQueryIndex + ", " + d + " >, ";
		// }
		//
		// // and add it to the arr
		// arr.add(insideArr);
		// BeamSearchNode successorNode = new BeamSearchNode(node, arr);
		// successorNode.sequenceForDebug = sequenceForDebug;
		// sequenceForDebug = null;
		// successorNodesSet.add(successorNode);
		//
		// }
		// } else {
		for (int index = 0; index < node.generalArrNumberOfFetches.get(0).size(); index++) {
			if (oracle == WhichOracle.macroSingleAction) {
				if (node.starframework.calcTreeNodeMap
						.get(node.generalArrNumberOfFetches.get(0).get(index).starQueryIndex)
						.getData().depthOfDigging < maxAnyTimeAnswerDepthStarQueryMap
								.get(node.generalArrNumberOfFetches.get(0).get(index).starQueryIndex)) {
					for (Integer curDelta : deltas) {
						// for (int curDelta = delta; curDelta <= maxDelta;
						// curDelta += delta) {
						// what was the sequence until here.
						ArrayList<ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>> arr = new ArrayList<ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>>();
						for (ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched> foo : node.generalArrNumberOfFetches) {
							arr.add((ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>) foo.clone());
						}

						// what should be the sequence for this successor
						// node
						ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched> insideArr = new ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>();

						for (StarQueryIndexWithNumberOfPartialAnswerToBeFetched foo : arr.get(arr.size() - 1)) {
							insideArr.add((StarQueryIndexWithNumberOfPartialAnswerToBeFetched) foo.clone());
						}

						// we get from previous but increase the index by
						// delta.
						insideArr.get(index).numberOfPartialAnswersShouldBeFetched += curDelta;

						// and add it to the arr
						arr.add(insideArr);
						BeamSearchNode successorNode = new BeamSearchNode(node, arr);
						successorNode.sequenceForDebug = successorNode.parentNode.sequenceForDebug + " <"
								+ DummyFunctions.getSQIndexByCalcNodeIndexAndNumberOfStars(
										node.generalArrNumberOfFetches.get(0).get(index).starQueryIndex,
										node.starframework.starQueries.size())
								+ ": " + curDelta + ">, ";
						successorNodesSet.add(successorNode);
					}
				}

			} else { // implementation for macro actions
				throw new NotImplementedException("NOT implementation for macro actions", null, null);
			}

		}
		// }
		return successorNodesSet;
	}

	// private double getTotalQualityDividedByTotalTime(BeamSearchNode node) {
	// return node.quality / node.totalTimeInMilliseconds;
	// }

	// private double
	// getDifferentialQualityWithPrevStateDividedByTime(BeamSearchNode node) {
	// if (node.parentNode != null) {
	// // if ((node.totalTimeInMilliseconds -
	// // node.parentNode.totalTimeInMilliseconds) <= 0) {
	// // System.err.println(
	// // "Child node has a time less than the parent node. It might be we
	// // don't update time of the parent nodes. ");
	// // }
	// return ((node.quality - node.parentNode.quality)
	// / (node.totalTimeInMilliseconds -
	// node.parentNode.totalTimeInMilliseconds));
	// } else {
	// return node.quality / node.totalTimeInMilliseconds;
	// }
	//
	// }
}
