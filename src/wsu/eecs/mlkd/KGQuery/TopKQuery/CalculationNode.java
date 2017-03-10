package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Node;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;

public class CalculationNode {

	public Set<Node> joinableQueryGraphNodes;
	public Set<Node> allThisQueryGraphNodes;
	public StarGraphQuery starQuery;
	public int levelInCalcTree = -1;
	public int depthOfDigging = 0;
	public int howManyTimesSelectedBefore;
	public int nodeIndex;
	public ArrayList<GraphResult> graphResult;
	public float upperBound = Float.MAX_VALUE;
	// public float lowerBound = Float.MIN_VALUE;
	// public int cursorIndex;
	public boolean isStarQuery = false;
	public boolean callStarKIsEnough = false; // the results totally finished
	// firstItem F'
	float lastItemValueFPrime = 1000000;
	float firstItemValueFPrime = 1000000;
	public int numberOfPartialAnswersShouldBeFetched;
	public HashMap<String, ArrayList<GraphResult>> hashedResult;
	public int lastVistitedItem = -1;
	//public ArrayList<String> resultsInStringArr;
	/// boolean hasUnvisitedResults = false;
	public int firstPQItemSize = 0;
	// for debug
	public String allThisQueryGraphNodeForDebug = "";
	public String joinableQueryGraphNodesForDebug = "";
	public int numberOfNodes;
	public int numberOfEdges;
	public double anytimeUpperBound = 1000000;

	public CalculationNode copy() {
		CalculationNode newCalculationNode;
		if (this.isStarQuery) {
			newCalculationNode = new CalculationNode(this.allThisQueryGraphNodes, this.starQuery,
					this.numberOfPartialAnswersShouldBeFetched, this.nodeIndex);

		} else {
			newCalculationNode = new CalculationNode(this.allThisQueryGraphNodes, this.numberOfEdges,
					this.numberOfPartialAnswersShouldBeFetched, this.nodeIndex);

		}

		newCalculationNode.joinableQueryGraphNodes = this.joinableQueryGraphNodes;
		newCalculationNode.levelInCalcTree = this.levelInCalcTree;
		newCalculationNode.depthOfDigging = this.depthOfDigging;
		// newCalculationNode.upperBound = this.upperBound;
		newCalculationNode.callStarKIsEnough = this.callStarKIsEnough;
		newCalculationNode.isStarQuery = this.isStarQuery;
		newCalculationNode.lastItemValueFPrime = this.lastItemValueFPrime;
		newCalculationNode.firstItemValueFPrime = this.firstItemValueFPrime;
		newCalculationNode.howManyTimesSelectedBefore = this.howManyTimesSelectedBefore;

		newCalculationNode.allThisQueryGraphNodeForDebug = this.allThisQueryGraphNodeForDebug;
		newCalculationNode.joinableQueryGraphNodesForDebug = this.joinableQueryGraphNodesForDebug;
		newCalculationNode.firstPQItemSize = this.firstPQItemSize;
		newCalculationNode.lastVistitedItem = this.lastVistitedItem;

		newCalculationNode.numberOfEdges = this.numberOfEdges;
		newCalculationNode.numberOfNodes = this.numberOfNodes;
		newCalculationNode.anytimeUpperBound = this.anytimeUpperBound;

		for (GraphResult oldGraphResult : this.graphResult) {
			newCalculationNode.graphResult.add(oldGraphResult.copy());
		}

		for (String key : this.hashedResult.keySet()) {

			// TODO: it's written for patching a bug!
			// HashSet<String> visitedSet = new HashSet<String>();
			ArrayList<GraphResult> newGraphResults = new ArrayList<GraphResult>();
			for (GraphResult gr : this.hashedResult.get(key)) {
				// if (!visitedSet.contains(gr.nodesInThisGraphResultForDebug))
				// {

				newGraphResults.add(gr);
				// visitedSet.add(gr.nodesInThisGraphResultForDebug);
				// } else {
				// System.err.println("repeated hashed results:" +
				// gr.nodesInThisGraphResultForDebug);
				// }

			}

			newCalculationNode.hashedResult.put(key, newGraphResults);
		}

//		for (String res : this.resultsInStringArr) {
//			newCalculationNode.resultsInStringArr.add(res);
//		}

		return newCalculationNode;
	}

	private void initialize() {

		graphResult = new ArrayList<GraphResult>();
		joinableQueryGraphNodes = new HashSet<Node>();
		//resultsInStringArr = new ArrayList<String>();
		hashedResult = new HashMap<String, ArrayList<GraphResult>>();
		// allThisQueryGraphNodes = new HashSet<Node>();
	}

	public CalculationNode(Set<Node> allThisQueryGraphNodes, int numberOfEdges,
			int numberOfPartialAnswersShouldBeFetched, int nodeIndex) {
		initialize();
		// compute joinable nodes
		this.allThisQueryGraphNodes = allThisQueryGraphNodes;
		this.numberOfPartialAnswersShouldBeFetched = numberOfPartialAnswersShouldBeFetched;
		this.nodeIndex = nodeIndex;
		// for debug
		for (Node node : allThisQueryGraphNodes) {
			allThisQueryGraphNodeForDebug += node.getId() + "_";
		}
		numberOfNodes = allThisQueryGraphNodes.size();
		this.numberOfEdges = numberOfEdges;

	}

	public void computeJoinableQueryGraphNodesForDebug() {
		for (Node node : joinableQueryGraphNodes) {
			joinableQueryGraphNodesForDebug += node.getId() + "_";
		}
	}

	public CalculationNode(Set<Node> allThisQueryGraphNodes, StarGraphQuery starQuery,
			int numberOfPartialAnswersShouldBeFetched, int nodeIndex) {
		initialize();
		// compute joinable nodes
		this.allThisQueryGraphNodes = allThisQueryGraphNodes;
		this.numberOfPartialAnswersShouldBeFetched = numberOfPartialAnswersShouldBeFetched;
		this.starQuery = starQuery;
		this.isStarQuery = true;
		this.nodeIndex = nodeIndex;
		// updateLastAndFirstItems();

		// for debug
		for (Node node : allThisQueryGraphNodes) {
			allThisQueryGraphNodeForDebug += node.getId() + "_";
		}
		numberOfNodes = allThisQueryGraphNodes.size();
		numberOfEdges = numberOfNodes - 1;

		this.starQuery.starQueryCalcNodeIndex = nodeIndex;
	}

	public void updateLastAndFirstItems(HashMap<Node, Integer> queryNodeIntersectMap) {

		if (graphResult.size() > 0) {
			sortGraphResultInDescendingOrder();

			int lastItemIndex = graphResult.size() - 1;

			GraphResult lastItem = graphResult.get(lastItemIndex);

			if (firstItemValueFPrime >= 1000000) {
				firstItemValueFPrime = 0;
				GraphResult firstItem = graphResult.get(0);
				for (Node qNode : queryNodeIntersectMap.keySet()) {
					if (firstItem.assembledResult.containsKey(qNode)) {
						firstItemValueFPrime += firstItem.assembledResult.get(qNode).simValue
								/ queryNodeIntersectMap.get(qNode);
					}
				}
			}

			lastItemValueFPrime = 0;
			for (Node qNode : queryNodeIntersectMap.keySet()) {
				if (lastItem.assembledResult.containsKey(qNode)) {
					lastItemValueFPrime += lastItem.assembledResult.get(qNode).simValue
							/ queryNodeIntersectMap.get(qNode);
				}
			}

			if (isStarQuery) {
				anytimeUpperBound = lastItemValueFPrime + numberOfEdges + numberOfNodes;
			}
			// firstItemValueFPrime = firstItem.nonJoinableNodesTotalValue
			// + alphaOrOneMinusAlpha * firstItem.joinableNodesTotalValue;

			// lastItemValueFPrime = lastItem.nonJoinableNodesTotalValue
			// + alphaOrOneMinusAlpha * lastItem.joinableNodesTotalValue;

		}
	}

	public void anytimeUpperBoundUpdate(CalculationNode lNodeData, CalculationNode rNodeData,
			CalculationNode parentNodeData) {

		if (parentNodeData != null) {
			double prefix = parentNodeData.numberOfNodes + parentNodeData.numberOfEdges;

			if (lNodeData.isStarQuery) {
				prefix += Math.max(lNodeData.firstItemValueFPrime + rNodeData.lastItemValueFPrime,
						lNodeData.lastItemValueFPrime + rNodeData.firstItemValueFPrime);
			} else {
				prefix += lNodeData.anytimeUpperBound + rNodeData.firstItemValueFPrime;
			}

			parentNodeData.anytimeUpperBound = prefix;

		}

	}

	private void sortGraphResultInDescendingOrder() {
		Collections.sort(graphResult);
	}

	public boolean hasAtLeastOneUnvisitedReuslt() {
		if (graphResult.size() == 0) {
			return false;
		}
		if ((graphResult.size() - 1) > lastVistitedItem) {
			// e.g size=2
			// lastVisitedItem 1
			return true;
		}
		// int cnt = 0;
		// for (int i = 0; i < graphResult.size(); i++) {
		//
		// if (!graphResult.get(i).isVisited) {
		// cnt++;
		// }
		// }
		// if (cnt >= 1) {
		// return true;
		// }
		return false;
	}

	// public boolean hasKUnvisitedResults(int k) {
	// if (graphResult.size() == 0) {
	// return false;
	// }
	// int cnt = 0;
	// for (int i = 0; i < graphResult.size(); i++) {
	//
	// if (!graphResult.get(i).isVisited) {
	// cnt++;
	// }
	// }
	// if (cnt >= k) {
	// return true;
	// }
	// return false;
	// }

	// kth in the graphresult graph
	// lowerbound for leaf isn't useful.
	// public void updateLowerBound(int k) {
	// // assumption arrayList should be sorted in this stage.
	// if (this.graphResult.size() >= k) {
	// sortGraphResultInDescendingOrder();
	// // we should k kTh as a lowerbound
	// GraphResult grKth;
	// if (this.graphResult.size() > k)
	// grKth = this.graphResult.get(k);
	// else
	// grKth = this.graphResult.get(k - 1);
	// // ?
	//
	// DummyFunctions.printIfItIsInDebuggedMode("lowerbound updated for " +
	// this.allThisQueryGraphNodeForDebug
	// + " from: " + lowerBound + " to: " + grKth.totalValue);
	//
	// lowerBound = grKth.totalValue;
	//
	// }
	//
	// }

	// public void updateUpperbound(CalculationNode otherNodeData) {
	//
	// DummyFunctions
	// .printIfItIsInDebuggedMode("upperbound updated for " +
	// this.allThisQueryGraphNodeForDebug + " from: "
	// + this.upperBound + " to: " + (this.lastItemValueFPrime +
	// otherNodeData.firstItemValueFPrime));
	// // if (this.upperBound < (this.lastItemValueFPrime +
	// // otherNodeData.firstItemValueFPrime)) {
	// // System.err.println("upperbound will be increased?!");
	// // }
	//
	// this.upperBound = this.lastItemValueFPrime +
	// otherNodeData.firstItemValueFPrime;
	//
	// }

}
