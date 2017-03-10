package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedWriter;
import java.io.IOException;

import wsu.eecs.mlkd.KGQuery.TopKQuery.AnyTimeStarFramework;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;

public class Features {
	int queryIndex;
	int nodes; // # of query nodes
	int edges; // # of query edges
	int stars;// # of query stars
	int[] nodesInStar; // # of nodes in each star
	double[] avgPivotDegreeInDataGraph; // avg degree of all distinc pivots in
										// data graph
	int[] estimatedPA; // how many PA each star query has?
	// boolean areNeighborSimilar;
	int[] firstPQItemSize; // the size of all PQ's at first step.
	int[] possiblePivots; // possible number of pivots just based on label
							// similarity
	int[] joinableNodes; // the number of joinable nodes at each star query
	int[] anyTimeDepth; // the depth of the best final anytime answers

	// using for dynamic search procedure
	public Features(int nodes, int edges, int stars, int[] nodesInStar, double[] avgPivotDegreeInDataGraph,
			int[] estimatedPA, int[] firstPQItemSize, int[] possiblePivots, int[] joinableNodes) {

		this.nodes = nodes;
		this.edges = edges;
		this.stars = stars;
		this.nodesInStar = nodesInStar;
		this.avgPivotDegreeInDataGraph = avgPivotDegreeInDataGraph;
		this.estimatedPA = estimatedPA;
		this.firstPQItemSize = firstPQItemSize;
		this.possiblePivots = possiblePivots;
		this.joinableNodes = joinableNodes;

	}

	public Features(int queryIndex, int nodes, int edges, int stars, int[] nodesInStar,
			double[] avgPivotDegreeInDataGraph, int[] estimatedPA, int[] firstPQItemSize, int[] possiblePivots,
			int[] joinableNodes, int[] anyTimeDepth) {
		this.queryIndex = queryIndex;
		this.nodes = nodes;
		this.edges = edges;
		this.stars = stars;
		this.nodesInStar = nodesInStar;
		this.avgPivotDegreeInDataGraph = avgPivotDegreeInDataGraph;
		this.estimatedPA = estimatedPA;
		this.firstPQItemSize = firstPQItemSize;
		this.possiblePivots = possiblePivots;
		this.joinableNodes = joinableNodes;
		this.anyTimeDepth = anyTimeDepth;

	}

	public Features(int queryIndex, int numberOfStars, double[] avgPivotDegreeInDataGraph, int[] estimatedPA,
			int[] joinableNodes) {
		this.queryIndex = queryIndex;
		this.avgPivotDegreeInDataGraph = avgPivotDegreeInDataGraph;
		this.estimatedPA = estimatedPA;
		this.joinableNodes = joinableNodes;
	}

	public Features(int queryIndex, int edges, int numberOfStars, int[] nodesInStar, int[] estimatedPA,
			int[] firstPQItemSize, int[] joinableNodes) {
		this.edges = edges;
		this.stars = numberOfStars;
		this.nodesInStar = nodesInStar;
		this.estimatedPA = estimatedPA;
		this.firstPQItemSize = firstPQItemSize;
		this.joinableNodes = joinableNodes;
	}

	private void print(BufferedWriter bwStaticFeatures) throws Exception {

		bwStaticFeatures.write(String.valueOf(this.nodes) + ",");
		bwStaticFeatures.write(String.valueOf(this.edges) + ",");
		bwStaticFeatures.write(String.valueOf(this.stars) + ",");

		for (int i = 0; i < nodesInStar.length; i++) {
			bwStaticFeatures.write(String.valueOf(this.nodesInStar[i]) + ",");
		}

		for (int i = 0; i < avgPivotDegreeInDataGraph.length; i++) {
			bwStaticFeatures.write(String.valueOf(this.avgPivotDegreeInDataGraph[i]) + ",");
		}

		for (int i = 0; i < estimatedPA.length; i++) {
			bwStaticFeatures.write(String.valueOf(this.estimatedPA[i]) + ",");
		}

		for (int i = 0; i < firstPQItemSize.length; i++) {
			bwStaticFeatures.write(String.valueOf(this.firstPQItemSize[i]) + ",");
		}

		for (int i = 0; i < possiblePivots.length; i++) {
			bwStaticFeatures.write(String.valueOf(this.possiblePivots[i]) + ",");
		}

		for (int i = 0; i < joinableNodes.length; i++) {
			bwStaticFeatures.write(String.valueOf(this.joinableNodes[i]) + ",");
		}
	}

	public void printForDynamic(BufferedWriter bwStaticFeatures) throws Exception {
		print(bwStaticFeatures);
	}

	public void printForStatic(BufferedWriter bwStaticFeatures) throws Exception {

		bwStaticFeatures.write(String.valueOf(this.queryIndex) + ",");

		print(bwStaticFeatures);

		for (int i = 0; i < anyTimeDepth.length; i++) {
			bwStaticFeatures.write(String.valueOf(this.anyTimeDepth[i]) + ",");
		}

		bwStaticFeatures.newLine();
		bwStaticFeatures.flush();
	}

	public Object[] getStaticFeaturesArray(int numberOfStarQueries) {
		// excluding queryIndex and anyTimeDepth(s)

		int size = 3 + Dummy.DummyProperties.maxNumberOfSQ * 6;
		// int size = numberOfStarQueries * 3;
		Object[] staticFeatures = new Object[size];
		int i = 0;

		staticFeatures[i++] = nodes;
		staticFeatures[i++] = edges;
		staticFeatures[i++] = stars;

		for (int cnt = 0; cnt < nodesInStar.length; cnt++) {
			staticFeatures[i++] = nodesInStar[cnt];
		}

		for (int cnt = 0; cnt < avgPivotDegreeInDataGraph.length; cnt++) {
			staticFeatures[i++] = avgPivotDegreeInDataGraph[cnt];
		}

		for (int cnt = 0; cnt < estimatedPA.length; cnt++) {
			staticFeatures[i++] = estimatedPA[cnt];
		}

		for (int cnt = 0; cnt < firstPQItemSize.length; cnt++) {
			staticFeatures[i++] = firstPQItemSize[cnt];
		}

		for (int cnt = 0; cnt < possiblePivots.length; cnt++) {
			staticFeatures[i++] = possiblePivots[cnt];
		}
		for (int cnt = 0; cnt < joinableNodes.length; cnt++) {
			staticFeatures[i++] = joinableNodes[cnt];
		}
		return staticFeatures;
	}
}

class CommonFeatures {

}

class BaseFeatures {
	int queryIndex;
	int paParentSelected;
	int paParentExpansion;
	int[] pqParent;
	int[] pqRoot;
	double[] ubParent;
	double[] ubRoot;
	double lbRoot;
	double lbParent;

	public BaseFeatures(int queryIndex, int[] pqParent, int[] pqRoot, double[] ubParent, double[] ubRoot, double lbRoot,
			double lbParent, int paParentSelected, int paParentExpansion) {
		this.queryIndex = queryIndex;
		this.pqParent = pqParent;
		this.pqRoot = pqRoot;
		this.ubParent = ubParent;
		this.ubRoot = ubRoot;
		this.lbRoot = lbRoot;
		this.lbParent = lbParent;
		this.paParentSelected = paParentSelected;
		this.paParentExpansion = paParentExpansion;

	}

	public BaseFeatures(int queryIndex, int[] pqParent, int[] pqRoot, double lbRoot, double lbParent,
			int paParentSelected, int paParentExpansion) {
		this.queryIndex = queryIndex;
		this.pqParent = pqParent;
		this.pqRoot = pqRoot;
		this.lbRoot = lbRoot;
		this.lbParent = lbParent;
		this.paParentSelected = paParentSelected;
		this.paParentExpansion = paParentExpansion;
	}

	public BaseFeatures(int queryIndex, double lbRoot, double lbParent, double[] ubRoot, double[] ubParent,
			int paParentSelected, int paParentExpansion) {
		this.queryIndex = queryIndex;
		this.ubParent = ubParent;
		this.ubRoot = ubRoot;
		this.lbRoot = lbRoot;
		this.lbParent = lbParent;
		this.paParentSelected = paParentSelected;
		this.paParentExpansion = paParentExpansion;
	}

}

class SelectionFeatures {
	// groundTruth
	public int starQuerySelectedIndex; // which star query is selected?

	int queryIndex; // the query id for future identifications

	int[] pqCurrent; // the size of all star queries PQ's
	double[] ubCurrent; // current value of all upperbounds
	double lbCurrent; // current value of the lowerbound

	int[] pqDiffThisFromParent; // the difference between PQ size of this state
								// and parent's state
	int[] pqDiffThisFromRoot;// the difference between PQ size of this state and
								// root's state
	int[] generateNextBestMatchQueued; // the items waiting in the lattice
										// structure
	double[] ubDifferenceFromCurrentLB; // difference of all upperbounds and
										// current lowerbound
	double[] ubDifferenceFromParentUB;// difference of all upperbounds of this
										// state with all upperbounds of
										// previous state
	double[] ubDifferenceFromRootUB;// difference of all upperbounds of this
									// state with all upperbounds of root's
									// state
	double lbDifferenceFromRootLB;// difference of current state's lowerbound
									// and root's state lowerbound
	double lbDifferenceFromParentLB;// difference of current state's lowerbound
									// and previous state lowerbound
	int previousPASelected;
	int[] howManyTimesSelectedBefore;
	int[] contributionToCurrentAnswer;
	int[] sqCalcTreeDepth;
	int[] currentDepth;
	boolean[] isStarkIsEnough;

	int[] remainingPA;

	// helper properties
	// public int minCurrentDepthHelper = 0;
	// public int minCurrentDepthSQIndexHelper = 0;

	public SelectionFeatures(int queryIndex, int[] pqCurrent, double[] ubCurrent, double lbCurrent,
			int[] pqDiffThisFromParent, int[] pqDiffThisFromRoot, int[] generateNextBestMatchQueued,
			double[] ubDifferenceFromCurrentLB, double[] ubDifferenceFromParentUB, double[] ubDifferenceFromRootUB,
			double lbDifferenceFromRootLB, double lbDifferenceFromParentLB, int previousPASelected,
			int[] howManyTimesSelectedBefore, int[] currentDepth, boolean[] isStarkIsEnough, int[] remainingPA,
			int starQuerySelectedIndex) {

		this.queryIndex = queryIndex;
		this.pqCurrent = pqCurrent;
		this.ubCurrent = ubCurrent;
		this.lbCurrent = lbCurrent;
		this.pqDiffThisFromParent = pqDiffThisFromParent;
		this.pqDiffThisFromRoot = pqDiffThisFromRoot;
		this.generateNextBestMatchQueued = generateNextBestMatchQueued;
		this.ubDifferenceFromCurrentLB = ubDifferenceFromCurrentLB;
		this.ubDifferenceFromParentUB = ubDifferenceFromParentUB;
		this.ubDifferenceFromRootUB = ubDifferenceFromRootUB;
		this.lbDifferenceFromRootLB = lbDifferenceFromRootLB;
		this.lbDifferenceFromParentLB = lbDifferenceFromParentLB;
		this.previousPASelected = previousPASelected;
		this.howManyTimesSelectedBefore = howManyTimesSelectedBefore;
		this.currentDepth = currentDepth;
		this.isStarkIsEnough = isStarkIsEnough;
		this.remainingPA = remainingPA;
		this.starQuerySelectedIndex = starQuerySelectedIndex;

	}

	public SelectionFeatures(int queryIndex, int[] pqCurrent, double[] ubCurrent, int[] pqDiffThisFromParent,
			int[] pqDiffThisFromRoot, double[] ubDifferenceFromCurrentLB, int previousPASelected,
			boolean[] isStarkIsEnough, int[] remainingPA, int starQuerySelectedIndex) {
		this.queryIndex = queryIndex;
		this.pqCurrent = pqCurrent;
		this.ubCurrent = ubCurrent;
		this.pqDiffThisFromParent = pqDiffThisFromParent;
		this.pqDiffThisFromRoot = pqDiffThisFromRoot;
		this.ubDifferenceFromCurrentLB = ubDifferenceFromCurrentLB;
		this.previousPASelected = previousPASelected;
		this.isStarkIsEnough = isStarkIsEnough;
		this.remainingPA = remainingPA;
		this.starQuerySelectedIndex = starQuerySelectedIndex;
	}

	public SelectionFeatures(int queryIndex, int[] pqCurrent, double[] ubCurrent, double lbCurrent,
			double[] ubDifferenceFromCurrentLB, double[] ubDifferenceFromParentUB, double lbDifferenceFromParentLB,
			int[] currentDepth, boolean[] isStarkIsEnough, int previousPASelected, int starQuerySelectedIndex) {

		this.queryIndex = queryIndex;
		this.pqCurrent = pqCurrent;
		this.ubCurrent = ubCurrent;
		this.lbCurrent = lbCurrent;
		this.ubDifferenceFromCurrentLB = ubDifferenceFromCurrentLB;
		this.ubDifferenceFromParentUB = ubDifferenceFromParentUB;
		this.lbDifferenceFromParentLB = lbDifferenceFromParentLB;
		this.previousPASelected = previousPASelected;
		this.currentDepth = currentDepth;
		this.isStarkIsEnough = isStarkIsEnough;
		this.starQuerySelectedIndex = starQuerySelectedIndex;
	}

	public void print(Features baseStaticFeatures, BufferedWriter bufferedWriter) throws Exception {

		// bufferedWriter.write(String.valueOf(this.lbCurrent) + ",");

		bufferedWriter.write(String.valueOf(this.queryIndex) + ",");

		baseStaticFeatures.printForDynamic(bufferedWriter);

		for (int i = 0; i < pqCurrent.length; i++) {
			bufferedWriter.write(String.valueOf(this.pqCurrent[i]) + ",");
		}

		for (int i = 0; i < ubCurrent.length; i++) {
			bufferedWriter.write(String.valueOf(this.ubCurrent[i]) + ",");
		}

		bufferedWriter.write(String.valueOf(this.lbCurrent) + ",");

		for (int i = 0; i < pqDiffThisFromParent.length; i++) {
			bufferedWriter.write(String.valueOf(this.pqDiffThisFromParent[i]) + ",");
		}

		for (int i = 0; i < pqDiffThisFromRoot.length; i++) {
			bufferedWriter.write(String.valueOf(this.pqDiffThisFromRoot[i]) + ",");
		}

		for (int i = 0; i < generateNextBestMatchQueued.length; i++) {
			bufferedWriter.write(String.valueOf(this.generateNextBestMatchQueued[i]) + ",");
		}

		for (int i = 0; i < ubDifferenceFromCurrentLB.length; i++) {
			bufferedWriter.write(String.valueOf(this.ubDifferenceFromCurrentLB[i]) + ",");
		}

		for (int i = 0; i < ubDifferenceFromParentUB.length; i++) {
			bufferedWriter.write(String.valueOf(this.ubDifferenceFromParentUB[i]) + ",");
		}

		for (int i = 0; i < ubDifferenceFromRootUB.length; i++) {
			bufferedWriter.write(String.valueOf(this.ubDifferenceFromRootUB[i]) + ",");
		}

		bufferedWriter.write(String.valueOf(this.lbDifferenceFromRootLB) + ",");

		bufferedWriter.write(String.valueOf(this.lbDifferenceFromParentLB) + ",");

		for (int i = 0; i < howManyTimesSelectedBefore.length; i++) {
			bufferedWriter.write(String.valueOf(this.howManyTimesSelectedBefore[i]) + ",");
		}

//		for (int i = 0; i < contributionToCurrentAnswer.length; i++) {
//			bufferedWriter.write(String.valueOf(this.contributionToCurrentAnswer[i]) + ",");
//		}

//		for (int i = 0; i < sqCalcTreeDepth.length; i++) {
//			bufferedWriter.write(String.valueOf(this.sqCalcTreeDepth[i]) + ",");
//		}

		for (int i = 0; i < currentDepth.length; i++) {
			bufferedWriter.write(String.valueOf(this.currentDepth[i]) + ",");
		}

		for (int i = 0; i < isStarkIsEnough.length; i++) {
			bufferedWriter.write(String.valueOf(this.isStarkIsEnough[i]) + ",");
		}

		for (int i = 0; i < remainingPA.length; i++) {
			bufferedWriter.write(String.valueOf(this.remainingPA[i]) + ",");
		}

		bufferedWriter.write(String.valueOf(this.previousPASelected) + ",");

		// groundtruth
		bufferedWriter.write(String.valueOf(this.starQuerySelectedIndex) + ",");

		bufferedWriter.newLine();
		bufferedWriter.flush();
	}

	public Object[] getSelectionFeaturesArray(Object[] baseFeaturesArray, int numberOfStarQueries) {

		// computed by considering class also.
		int size = baseFeaturesArray.length + 5 + DummyProperties.maxNumberOfSQ * 7
				+ (2 * DummyProperties.maxNumberOfSQ - 1) * 4;
		// int size = baseFeaturesArray.length + 2 + numberOfStarQueries * 5 +
		// ubCurrent.length * 2;
		Object[] featuresArr = new Object[size];
		int i = 0;
		// featuresArr[i++] = queryIndex;
		for (int cnt = 0; cnt < baseFeaturesArray.length; cnt++) {
			featuresArr[i++] = baseFeaturesArray[cnt];
		}

		for (int cnt = 0; cnt < pqCurrent.length; cnt++) {
			featuresArr[i++] = pqCurrent[cnt];
		}

		for (int cnt = 0; cnt < ubCurrent.length; cnt++) {
			featuresArr[i++] = ubCurrent[cnt];
		}
		featuresArr[i++] = lbCurrent;

		for (int cnt = 0; cnt < pqDiffThisFromParent.length; cnt++) {
			featuresArr[i++] = pqDiffThisFromParent[cnt];
		}

		for (int cnt = 0; cnt < pqDiffThisFromRoot.length; cnt++) {
			featuresArr[i++] = pqDiffThisFromRoot[cnt];
		}
		// for (int cnt = 0; cnt < generateNextBestMatchQueued.length; cnt++) {
		// featuresArr[i++] = generateNextBestMatchQueued[cnt];
		// }

		for (int cnt = 0; cnt < ubDifferenceFromCurrentLB.length; cnt++) {
			featuresArr[i++] = ubDifferenceFromCurrentLB[cnt];
		}
		for (int cnt = 0; cnt < ubDifferenceFromParentUB.length; cnt++) {
			featuresArr[i++] = ubDifferenceFromParentUB[cnt];
		}

		for (int cnt = 0; cnt < ubDifferenceFromRootUB.length; cnt++) {
			featuresArr[i++] = ubDifferenceFromRootUB[cnt];
		}

		featuresArr[i++] = lbDifferenceFromRootLB;

		featuresArr[i++] = lbDifferenceFromParentLB;

		for (int cnt = 0; cnt < howManyTimesSelectedBefore.length; cnt++) {
			featuresArr[i++] = howManyTimesSelectedBefore[cnt];
		}

		for (int cnt = 0; cnt < currentDepth.length; cnt++) {
			featuresArr[i++] = currentDepth[cnt];
		}

		for (int cnt = 0; cnt < isStarkIsEnough.length; cnt++) {
			featuresArr[i++] = isStarkIsEnough[cnt];
		}
		for (int cnt = 0; cnt < remainingPA.length; cnt++) {
			featuresArr[i++] = remainingPA[cnt];
		}

		featuresArr[i++] = previousPASelected;
		featuresArr[i++] = starQuerySelectedIndex;

		return featuresArr;
	}
}

class ExpansionFeatures {
	int queryIndex;
	// groundtruth
	int expandValue;

	// context
	int previousExpansionValue;

	int paSelected;
	double currentThisLB;
	double currentThisUB;
	int currentPQ;
	// parent calcNode ub
	double currentParentUB;
	int pqDiffThisFromParent;
	int pqDiffThisFromRoot;
	int generateNextBestMatchQueued;
	double ubDifferenceFromCurrentLB;
	double ubDifferenceFromParentUB;
	double ubDifferenceFromRootUB;
	double lbDifferenceFromRootLB;
	double lbDifferenceFromParentLB;
	int previousPASelected;
	int howManyTimesSelectedBefore;
	int contributionToCurrentAnswer;
	int sqCalcTreeDepth;
	int currentDepth;
	boolean isStarkIsEnough;
	int remainingPA;
	int searchLevel;
	double currentRank;
	// getting the maximum k*ub - sum(current answers scores)
	double diffMaxPossibleRankCurrentRank;
	boolean isPreviouslySelected;
	double maxUB;

	public ExpansionFeatures(int queryIndex, int currentPQ, double currentThisLB, double currentThisUB,
			double currentParentUB, int pqDiffThisFromParent, int pqDiffThisFromRoot, int generateNextBestMatchQueued,
			double ubDifferenceFromCurrentLB, double ubDifferenceFromParentUB, double ubDifferenceFromRootUB,
			double lbDifferenceFromRootLB, double lbDifferenceFromParentLB, int previousPASelected,
			int howManyTimesSelectedBefore, int currentDepth, boolean isStarkIsEnough, int remainingPA, int searchLevel,
			double diffMaxPossibleRankCurrentRank, boolean isPreviouslySelected, double maxUB, double currentRank,
			int previousExpansionValue, int expandValue) {
		this.queryIndex = queryIndex;
		this.currentPQ = currentPQ;
		this.currentThisLB = currentThisLB;
		this.currentThisUB = currentThisUB;
		this.currentParentUB = currentParentUB;
		this.pqDiffThisFromParent = pqDiffThisFromParent;
		this.pqDiffThisFromRoot = pqDiffThisFromRoot;
		this.generateNextBestMatchQueued = generateNextBestMatchQueued;
		this.ubDifferenceFromCurrentLB = ubDifferenceFromCurrentLB;
		this.ubDifferenceFromParentUB = ubDifferenceFromParentUB;
		this.ubDifferenceFromRootUB = ubDifferenceFromRootUB;
		this.lbDifferenceFromRootLB = lbDifferenceFromRootLB;
		this.lbDifferenceFromParentLB = lbDifferenceFromParentLB;
		this.previousPASelected = previousPASelected;
		this.howManyTimesSelectedBefore = howManyTimesSelectedBefore;
		this.currentDepth = currentDepth;
		this.isStarkIsEnough = isStarkIsEnough;
		this.remainingPA = remainingPA;
		this.searchLevel = searchLevel;
		this.diffMaxPossibleRankCurrentRank = diffMaxPossibleRankCurrentRank;
		this.isPreviouslySelected = isPreviouslySelected;
		this.maxUB = maxUB;
		this.currentRank = currentRank;
		this.previousExpansionValue = previousExpansionValue;
		this.expandValue = expandValue;
	}

	public ExpansionFeatures(int queryIndex, double currentThisLB, double currentThisUB,
			double lbDifferenceFromParentLB, int searchLevel, double currentRank, int previousExpansionValue,
			int expandValue) {
		this.queryIndex = queryIndex;
		this.currentThisLB = currentThisLB;
		this.currentThisUB = currentThisUB;
		this.lbDifferenceFromParentLB = lbDifferenceFromParentLB;
		this.searchLevel = searchLevel;
		this.currentRank = currentRank;
		this.previousExpansionValue = previousExpansionValue;
		this.expandValue = expandValue;
	}

	public ExpansionFeatures(int queryIndex, int currentPQ, double currentThisLB, double currentThisUB,
			double ubDifferenceFromCurrentLB, double lbDifferenceFromParentLB, int currentDepth, int searchLevel,
			double diffMaxPossibleRankCurrentRank, boolean isPreviouslySelected, double currentRank, int paSelected,
			int previousExpansionValue, int expandValue) {

		this.queryIndex = queryIndex;
		this.currentPQ = currentPQ;
		this.currentThisLB = currentThisLB;
		this.currentThisUB = currentThisUB;
		this.ubDifferenceFromCurrentLB = ubDifferenceFromCurrentLB;
		this.lbDifferenceFromParentLB = lbDifferenceFromParentLB;
		this.currentDepth = currentDepth;
		this.searchLevel = searchLevel;
		this.diffMaxPossibleRankCurrentRank = diffMaxPossibleRankCurrentRank;
		this.isPreviouslySelected = isPreviouslySelected;
		this.currentRank = currentRank;
		this.paSelected = paSelected;
		this.previousExpansionValue = previousExpansionValue;
		this.expandValue = expandValue;
	}

	public void print(Features baseStaticFeatures, BufferedWriter bufferedWriter) throws Exception {

		bufferedWriter.write(String.valueOf(this.queryIndex) + ",");

		baseStaticFeatures.printForDynamic(bufferedWriter);

		bufferedWriter.write(String.valueOf(this.currentPQ) + ",");
		bufferedWriter.write(String.valueOf(this.currentThisLB) + ",");
		bufferedWriter.write(String.valueOf(this.currentThisUB) + ",");
		bufferedWriter.write(String.valueOf(this.currentParentUB) + ",");
		bufferedWriter.write(String.valueOf(this.pqDiffThisFromParent) + ",");
		bufferedWriter.write(String.valueOf(this.pqDiffThisFromRoot) + ",");
		bufferedWriter.write(String.valueOf(this.generateNextBestMatchQueued) + ",");
		bufferedWriter.write(String.valueOf(this.ubDifferenceFromCurrentLB) + ",");
		bufferedWriter.write(String.valueOf(this.ubDifferenceFromParentUB) + ",");
		bufferedWriter.write(String.valueOf(this.ubDifferenceFromRootUB) + ",");
		bufferedWriter.write(String.valueOf(this.lbDifferenceFromRootLB) + ",");
		bufferedWriter.write(String.valueOf(this.lbDifferenceFromParentLB) + ",");

		bufferedWriter.write(String.valueOf(this.howManyTimesSelectedBefore) + ",");
		//bufferedWriter.write(String.valueOf(this.contributionToCurrentAnswer) + ",");
		//bufferedWriter.write(String.valueOf(this.sqCalcTreeDepth) + ",");
		bufferedWriter.write(String.valueOf(this.currentDepth) + ",");
		bufferedWriter.write(String.valueOf(this.isStarkIsEnough) + ",");
		bufferedWriter.write(String.valueOf(this.remainingPA) + ",");
		bufferedWriter.write(String.valueOf(this.searchLevel) + ",");
		bufferedWriter.write(String.valueOf(this.diffMaxPossibleRankCurrentRank) + ",");
		bufferedWriter.write(String.valueOf(this.isPreviouslySelected) + ",");
		bufferedWriter.write(String.valueOf(this.maxUB) + ",");
		bufferedWriter.write(String.valueOf(this.currentRank) + ",");
		bufferedWriter.write(String.valueOf(this.previousPASelected) + ",");
		bufferedWriter.write(String.valueOf(this.paSelected) + ",");
		bufferedWriter.write(String.valueOf(this.previousExpansionValue) + ",");
		bufferedWriter.write(String.valueOf(this.expandValue) + ",");

		bufferedWriter.newLine();
		bufferedWriter.flush();

	}

	public Object[] getExpansionFeaturesArray(Object[] baseFeaturesArray, int numberOfStarQueries) {
		int size = baseFeaturesArray.length + 17;
		// int size = baseFeaturesArray.length + 7;
		Object[] featuresArr = new Object[size];
		int i = 0;
		// featuresArr[i++] = queryIndex;
		for (int cnt = 0; cnt < baseFeaturesArray.length; cnt++) {
			featuresArr[i++] = baseFeaturesArray[cnt];
		}

		featuresArr[i++] = currentPQ;
		featuresArr[i++] = currentThisLB;
		featuresArr[i++] = currentThisUB;
		featuresArr[i++] = currentParentUB;
		// featuresArr[i++] = pqDiffThisFromParent;
		// featuresArr[i++] = pqDiffThisFromRoot;
		// featuresArr[i++] = generateNextBestMatchQueued;

		featuresArr[i++] = ubDifferenceFromCurrentLB;
		featuresArr[i++] = ubDifferenceFromParentUB;
		featuresArr[i++] = ubDifferenceFromRootUB;
		featuresArr[i++] = lbDifferenceFromRootLB;
		featuresArr[i++] = lbDifferenceFromParentLB;

		// featuresArr[i++] = howManyTimesSelectedBefore;

		featuresArr[i++] = currentDepth;
		featuresArr[i++] = isStarkIsEnough;

		featuresArr[i++] = remainingPA;
		featuresArr[i++] = searchLevel;
		// featuresArr[i++] = diffMaxPossibleRankCurrentRank;
		featuresArr[i++] = isPreviouslySelected;
		// featuresArr[i++] = maxUB;

		// featuresArr[i++] = currentRank;
		featuresArr[i++] = previousPASelected;
		// featuresArr[i++] = paSelected;
		featuresArr[i++] = previousExpansionValue;
		featuresArr[i++] = expandValue;

		return featuresArr;
	}

}

// class StoppingFeatures extends SelectionFeatures {
//
// public StoppingFeatures(int queryIndex, int[] pqCurrent, double[] ubCurrent,
// double lbCurrent, int[] pqDiffThisFromParent,
// int[] pqDiffThisFromRoot, int[] generateNextBestMatchQueued, double[]
// ubDifferenceFromCurrentLB,
// double[] ubDifferenceFromParentUB, double[] ubDifferenceFromRootUB, double
// lbDifferenceFromRootLB,
// double lbDifferenceFromParentLB, int previousPASelected, int[]
// howManyTimesSelectedBefore,
// int[] contributionToCurrentAnswer, int[] sqCalcTreeDepth, int[] currentDepth,
// boolean[] isStarkIsEnough,
// int[] remainingPA, int starQuerySelectedIndex) {
//
// super(queryIndex, pqCurrent, ubCurrent, lbCurrent, pqDiffThisFromParent,
// pqDiffThisFromRoot,
// generateNextBestMatchQueued, ubDifferenceFromCurrentLB,
// ubDifferenceFromParentUB,
// ubDifferenceFromRootUB, lbDifferenceFromRootLB, lbDifferenceFromParentLB,
// previousPASelected,
// howManyTimesSelectedBefore, contributionToCurrentAnswer, sqCalcTreeDepth,
// currentDepth, isStarkIsEnough,
// remainingPA, starQuerySelectedIndex);
//
// }
// }
