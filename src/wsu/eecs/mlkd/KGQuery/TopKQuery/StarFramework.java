// TODO:
// nowhere should we have literal codes like 0 for similarities! we should define a constant and use that.

package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;

public class StarFramework {
	// public GraphDatabaseService queryGraph;
	// public GraphDatabaseService knowledgeGraph;
	public int k;
	public float alpha;
	public final int maxK = 1000;
	public final int numberOfPartialMatches;
	public Levenshtein levenshtein;
	public boolean noAnswerForAtLeastOneOfStarQueris = false;
	public boolean noAnswerForAtLeastOneOfTreeNodes = false;
	public TreeNode<CalculationNode> rootTreeNode;

	// CalculationTree calculationTree = new CalculationTree();
	public ArrayList<StarGraphQuery> starQueries;
	// the key is knowledgegraph pivot node and the value is hashMap of
	// querynode and their possible lists of nodes.
	// it should be initialize for each star query we have
	HashMap<StarGraphQuery, HashMap<NodeWithValue, HashMap<Node, ArrayListWithCursor>>> possibleMatchesForThisKnowledgePivotInThisStarQuery;
	public boolean noOtherAnswer = false;
	// we should maintain all of the top-1 matches for all different matched
	// pivot nodes because may be we cannot find complete matches by the first
	// top-k
	HashMap<StarGraphQuery, HashMap<NodeWithValue, PriorityQueue<StarGraphResult>>> currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery;

	HashMap<StarGraphQuery, HashSet<String>> latticeVisitedResultsOfStarkForGenerateNextBestMatch;

	// PriorityQueue<GraphResult> finalResults;
	public ArrayList<GraphResult> finalResultsArrayList;

	HashMap<StarGraphQuery, PriorityQueue<StarGraphResult>> starkForLeafPQResults;

	// Set<TreeNode<CalculationNode>>

	// writing the values of partial matches in a file.
	// BufferedWriter bwTime;
	public HashMap<Integer, TreeNode<CalculationNode>> calcTreeNodeMap;
	public HashMap<Integer, TreeNode<CalculationNode>> calcTreeStarQueriesNodeMap;
	public HashMap<Integer, CalculationTreeSiblingNodes> joinLevelSiblingNodesMap;
	public int maxJoinLevel = 0;
	double finalResultLowerbound = Double.MIN_VALUE;
	double startTime;
	public boolean timeOut = false;
	int numberOfQueryEdges = 0;

	public HashMap<Node, Integer> queryNodeIntersectMap;

	public StarFramework copy(GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph) {

		StarFramework newStarFramework = new StarFramework(queryGraph, knowledgeGraph, this.k, this.alpha,
				this.levenshtein);

		newStarFramework.k = this.k;
		newStarFramework.numberOfQueryEdges = this.numberOfQueryEdges;
		newStarFramework.alpha = this.alpha;
		newStarFramework.levenshtein = this.levenshtein;
		newStarFramework.noAnswerForAtLeastOneOfStarQueris = this.noAnswerForAtLeastOneOfStarQueris;
		newStarFramework.noAnswerForAtLeastOneOfTreeNodes = this.noAnswerForAtLeastOneOfTreeNodes;

		newStarFramework.rootTreeNode = this.rootTreeNode.copy();
		newStarFramework.rootTreeNode.levelInCalcTree = this.rootTreeNode.levelInCalcTree;

		newStarFramework.starQueries = this.starQueries;

		newStarFramework.noOtherAnswer = this.noOtherAnswer;

		for (GraphResult graphResult : finalResultsArrayList) {
			newStarFramework.finalResultsArrayList.add(graphResult.copy());
		}

		// starkForLeafPQResults
		newStarFramework.starkForLeafPQResults = new HashMap<StarGraphQuery, PriorityQueue<StarGraphResult>>();
		for (StarGraphQuery starGraphQuery : this.starkForLeafPQResults.keySet()) {
			newStarFramework.starkForLeafPQResults.put(starGraphQuery, new PriorityQueue<StarGraphResult>());
		}
		for (StarGraphQuery starGraphQuery : this.starkForLeafPQResults.keySet()) {
			StarGraphResult[] starGraphResults = new StarGraphResult[this.starkForLeafPQResults.get(starGraphQuery)
					.size()];
			this.starkForLeafPQResults.get(starGraphQuery).toArray(starGraphResults);

			for (StarGraphResult sgr : starGraphResults)
				newStarFramework.starkForLeafPQResults.get(starGraphQuery).add(sgr.copy());
		}

		// latticeVisitedResultsOfStarkForGenerateNextBestMatch
		// HashMap<StarGraphQuery, HashSet<String>>
		newStarFramework.latticeVisitedResultsOfStarkForGenerateNextBestMatch = new HashMap<StarGraphQuery, HashSet<String>>();
		for (StarGraphQuery thisSGQ : this.latticeVisitedResultsOfStarkForGenerateNextBestMatch.keySet()) {

			HashSet<String> oldVisitedStr = this.latticeVisitedResultsOfStarkForGenerateNextBestMatch.get(thisSGQ);

			HashSet<String> newVisitedStr = new HashSet<String>();
			for (String str : oldVisitedStr) {
				newVisitedStr.add(str);
			}

			newStarFramework.latticeVisitedResultsOfStarkForGenerateNextBestMatch.put(thisSGQ, newVisitedStr);

		}

		// currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
		// HashMap<StarGraphQuery, HashMap<NodeWithValue,
		// PriorityQueue<StarGraphResult>>>
		newStarFramework.currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery = new HashMap<StarGraphQuery, HashMap<NodeWithValue, PriorityQueue<StarGraphResult>>>();
		for (StarGraphQuery sgq : this.currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery.keySet()) {
			HashMap<NodeWithValue, PriorityQueue<StarGraphResult>> currentNwvPQsgr = this.currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
					.get(sgq);

			HashMap<NodeWithValue, PriorityQueue<StarGraphResult>> newNwvPQsgr = new HashMap<NodeWithValue, PriorityQueue<StarGraphResult>>();

			for (NodeWithValue nwv : currentNwvPQsgr.keySet()) {
				PriorityQueue<StarGraphResult> oldSgrPQ = currentNwvPQsgr.get(nwv);
				StarGraphResult[] sgrArr = new StarGraphResult[oldSgrPQ.size()];

				oldSgrPQ.toArray(sgrArr);

				PriorityQueue<StarGraphResult> newSgrPQ = new PriorityQueue<StarGraphResult>(maxK,
						new StarGraphResultComparator());

				for (StarGraphResult oldSgrItem : sgrArr) {
					newSgrPQ.add(oldSgrItem.copy());
				}
				newNwvPQsgr.put(nwv, newSgrPQ);
			}

			newStarFramework.currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery.put(sgq, newNwvPQsgr);

		}

		// possibleMatchesForThisKnowledgePivotInThisStarQuery
		// HashMap<StarGraphQuery, HashMap<NodeWithValue, HashMap<Node,
		// ArrayListWithCursor>>>
		newStarFramework.possibleMatchesForThisKnowledgePivotInThisStarQuery = new HashMap<StarGraphQuery, HashMap<NodeWithValue, HashMap<Node, ArrayListWithCursor>>>();
		for (StarGraphQuery sgq : this.possibleMatchesForThisKnowledgePivotInThisStarQuery.keySet()) {
			HashMap<NodeWithValue, HashMap<Node, ArrayListWithCursor>> oldNwvNALWC = this.possibleMatchesForThisKnowledgePivotInThisStarQuery
					.get(sgq);
			HashMap<NodeWithValue, HashMap<Node, ArrayListWithCursor>> newNwvNALWC = new HashMap<NodeWithValue, HashMap<Node, ArrayListWithCursor>>();
			for (NodeWithValue oldNwv : oldNwvNALWC.keySet()) {
				HashMap<Node, ArrayListWithCursor> oldNodeArrLstWC = oldNwvNALWC.get(oldNwv);
				HashMap<Node, ArrayListWithCursor> newNodeArrLstWC = new HashMap<Node, ArrayListWithCursor>();
				for (Node oldNode : oldNodeArrLstWC.keySet()) {
					ArrayListWithCursor newArrLisWCPQ = oldNodeArrLstWC.get(oldNode).copy();

					newNodeArrLstWC.put(oldNode, newArrLisWCPQ);
				}
				newNwvNALWC.put(oldNwv, newNodeArrLstWC);
			}
			newStarFramework.possibleMatchesForThisKnowledgePivotInThisStarQuery.put(sgq, newNwvNALWC);
		}

		newStarFramework.queryNodeIntersectMap = this.queryNodeIntersectMap;

		TreeNode<CalculationNode> tempNode = newStarFramework.rootTreeNode;
		newStarFramework.calcTreeNodeMap = new HashMap<Integer, TreeNode<CalculationNode>>();
		newStarFramework.joinLevelSiblingNodesMap = new HashMap<Integer, CalculationTreeSiblingNodes>();

		newStarFramework.calcTreeNodeMap.put(tempNode.getData().nodeIndex, tempNode);
		while (tempNode != null) {

			if (tempNode.getRightChild() != null)
				newStarFramework.calcTreeNodeMap.put(tempNode.getRightChild().getData().nodeIndex,
						tempNode.getRightChild());

			if (tempNode.getLeftChild() != null)
				newStarFramework.calcTreeNodeMap.put(tempNode.getLeftChild().getData().nodeIndex,
						tempNode.getLeftChild());

			if (tempNode.getLeftChild() != null && tempNode.getRightChild() != null) {
				newStarFramework.joinLevelSiblingNodesMap.put(tempNode.levelInCalcTree,
						new CalculationTreeSiblingNodes(tempNode.getLeftChild(), tempNode.getRightChild()));
			}

			tempNode = tempNode.getLeftChild();
		}

		for (Integer index : newStarFramework.calcTreeNodeMap.keySet()) {
			if (newStarFramework.calcTreeNodeMap.get(index).getData().isStarQuery) {
				newStarFramework.calcTreeStarQueriesNodeMap.put(index, newStarFramework.calcTreeNodeMap.get(index));
			}
		}

		newStarFramework.startTime = this.startTime;
		newStarFramework.timeOut = this.timeOut;

		return newStarFramework;
	}

	public StarFramework(GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph, int k, float alpha,
			Levenshtein levenshtein) {
		// this.queryGraph = queryGraph;
		// this.knowledgeGraph = knowledgeGraph;
		this.k = k;

		this.alpha = alpha;
		// finalResults = new PriorityQueue<GraphResult>(k, new
		// GraphResultComparator());
		finalResultsArrayList = new ArrayList<GraphResult>();
		starkForLeafPQResults = new HashMap<StarGraphQuery, PriorityQueue<StarGraphResult>>();
		latticeVisitedResultsOfStarkForGenerateNextBestMatch = new HashMap<StarGraphQuery, HashSet<String>>();
		currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery = new HashMap<StarGraphQuery, HashMap<NodeWithValue, PriorityQueue<StarGraphResult>>>();
		this.levenshtein = levenshtein;

		// numberOfPartialMatches = k;
		numberOfPartialMatches = 10;

		// File foutTime = new File("partialMatch.txt");
		// FileOutputStream fosTime;
		// try {
		// fosTime = new FileOutputStream(foutTime, true);
		// bwTime = new BufferedWriter(new OutputStreamWriter(fosTime));
		// } catch (FileNotFoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		calcTreeNodeMap = new HashMap<Integer, TreeNode<CalculationNode>>();
		calcTreeStarQueriesNodeMap = new HashMap<Integer, TreeNode<CalculationNode>>();
		joinLevelSiblingNodesMap = new HashMap<Integer, CalculationTreeSiblingNodes>();
		queryNodeIntersectMap = new HashMap<Node, Integer>();

	}

	public HashMap<Long, Integer> decomposeQuery(GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph,
			NeighborIndexing neighborIndexingInstance, CacheServer cacheServer)

	{
		// decompose Q to a star query set Q;
		QueryDecomposition qd = new QueryDecomposition(levenshtein);

		long start_time = System.nanoTime();
		starQueries = qd.getStarQueriesFromGraphQueryBasedOnKnowledgeGraphClarksonGreedyAlgorithm(queryGraph,
				knowledgeGraph, neighborIndexingInstance, cacheServer);

		long end_time = System.nanoTime();
		double difference = (end_time - start_time) / 1e6;
		DummyFunctions.printIfItIsInDebuggedMode("QueryDecomposition finished in " + difference
				+ "miliseconds! starQueries size is: " + starQueries.size());

		for (StarGraphQuery sq : starQueries) {
			for (Node node : sq.allStarGraphQueryNodes) {
				if (queryNodeIntersectMap.containsKey(node)) {
					queryNodeIntersectMap.put(node, queryNodeIntersectMap.get(node) + 1);
				} else {
					queryNodeIntersectMap.put(node, 1);
				}
			}
		}
		initialize(queryGraph);

		return qd.nodeEstimationMap;

	}

	public void initialize(GraphDatabaseService queryGraph) {
		for (StarGraphQuery starQuery : starQueries) {
			DummyFunctions.printIfItIsInDebuggedMode("pivote node for the starQuery: " + starQuery.pivotNode.getId());
			for (Node node : starQuery.allStarGraphQueryNodes) {
				DummyFunctions.printIfItIsInDebuggedMode("all node for the starQuery: " + node.getId());
			}
			DummyFunctions.printIfItIsInDebuggedMode("end of the starQuery");
		}
		// stargraph => pivote nodes => each pivote nodes have some lists of
		// neighbors and each neighbor has a PQ with a cursor
		possibleMatchesForThisKnowledgePivotInThisStarQuery = new HashMap<StarGraphQuery, HashMap<NodeWithValue, HashMap<Node, ArrayListWithCursor>>>();
		latticeVisitedResultsOfStarkForGenerateNextBestMatch = new HashMap<StarGraphQuery, HashSet<String>>();

		// new all PQ's for all starQueries.
		// some initialization
		for (StarGraphQuery starQuery : starQueries) {

			starkForLeafPQResults.put(starQuery,
					new PriorityQueue<StarGraphResult>(maxK, new StarGraphResultComparator()));

			possibleMatchesForThisKnowledgePivotInThisStarQuery.put(starQuery,
					new HashMap<NodeWithValue, HashMap<Node, ArrayListWithCursor>>());

			currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery.put(starQuery,
					new HashMap<NodeWithValue, PriorityQueue<StarGraphResult>>());

			latticeVisitedResultsOfStarkForGenerateNextBestMatch.put(starQuery, new HashSet<String>());

		}
		// do some heuristics
		starQueries = sortStarQueriesForCalcTree(starQueries);
		// Generatnig the tree
		rootTreeNode = GenerateCalculationTree(queryGraph, starQueries);

		// for debug start
		DummyFunctions.printIfItIsInDebuggedMode("");
		DummyFunctions.printIfItIsInDebuggedMode("print decomposed tree");
		DummyFunctions.printIfItIsInDebuggedMode("rootNode: " + rootTreeNode.getData().allThisQueryGraphNodeForDebug);
		TreeNode<CalculationNode> lNode = rootTreeNode.getLeftChild();
		TreeNode<CalculationNode> rNode = rootTreeNode.getRightChild();

		while (lNode != null) {
			DummyFunctions.printIfItIsInDebuggedMode("lNode: " + lNode.getData().allThisQueryGraphNodeForDebug
					+ " in level: " + lNode.levelInCalcTree + " nodeIndex:" + lNode.getData().nodeIndex);
			DummyFunctions.printIfItIsInDebuggedMode("rNode: " + rNode.getData().allThisQueryGraphNodeForDebug
					+ " in level: " + rNode.levelInCalcTree + " nodeIndex:" + rNode.getData().nodeIndex);

			rNode = lNode.getRightChild();
			lNode = lNode.getLeftChild();

		}
		// for debug end
	}

	public ArrayList<GraphResult> starRun(GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph,
			NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) {
		startTime = System.nanoTime();
		starRun(knowledgeGraph, rootTreeNode, neighborIndexingInstance, cacheServer);
		// Collections.sort(finalResultsArrayList);
		// return finalResults;
		return finalResultsArrayList;
	}

	public ArrayList<GraphResult> starRoundRobinRun(GraphDatabaseService queryGraph,
			GraphDatabaseService knowledgeGraph, NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) {
		startTime = System.nanoTime();
		int cnt = 0;

		ArrayList<Integer> allStarNodeIndices = new ArrayList<Integer>();
		for (Integer nodeIndex : calcTreeStarQueriesNodeMap.keySet()) {
			allStarNodeIndices.add(nodeIndex);
		}
		// calcTreeStarQueriesNodeMap.get(1).getData().numberOfPartialAnswersShouldBeFetched
		// = 1;
		// calcTreeStarQueriesNodeMap.get(2).getData().numberOfPartialAnswersShouldBeFetched
		// = 1;
		// calcTreeStarQueriesNodeMap.get(5).getData().numberOfPartialAnswersShouldBeFetched
		// = 407;
		// calcTreeStarQueriesNodeMap.get(6).getData().numberOfPartialAnswersShouldBeFetched
		// = 5972;

		int allStarNodeIndicesSize = allStarNodeIndices.size();
		int depthJoinLevel = 0;
		do {
			depthJoinLevel = 0;
			int nodeIndexToBeFetched = -1;
			if (allStarNodeIndicesSize > 0) {
				nodeIndexToBeFetched = allStarNodeIndices.get(cnt % allStarNodeIndicesSize);

				TreeNode<CalculationNode> thisCalcNode = calcTreeNodeMap.get(nodeIndexToBeFetched);
				if (!thisCalcNode.getData().callStarKIsEnough) {
					starkForLeaf(knowledgeGraph, thisCalcNode, neighborIndexingInstance, cacheServer);
					depthJoinLevel = thisCalcNode.levelInCalcTree - 1;
				}
			}

			for (; depthJoinLevel >= 0; depthJoinLevel--) {
				CalculationTreeSiblingNodes calculationTreeSiblingNodes = joinLevelSiblingNodesMap.get(depthJoinLevel);
				twoWayHashJoin(calculationTreeSiblingNodes.leftNode, calculationTreeSiblingNodes.rightNode, k);
			}
			cnt++;

		}
		// check to see if the algorithm is finished?
		while (!algorithmShouldFinish());

		// Collections.sort(finalResultsArrayList);
		return finalResultsArrayList;
	}

	public boolean algorithmShouldFinish() {
		if (noAnswerForAtLeastOneOfStarQueris || noAnswerForAtLeastOneOfTreeNodes) {
			return true;
		}
		for (GraphResult gr : rootTreeNode.getData().graphResult) {
			if (!gr.isVisited) {
				// finalResults.add(gr);
				finalResultsArrayList.add(gr);
				gr.isVisited = true;
			}
		}

		if (rootTreeNode.getData().callStarKIsEnough) {
			return true;
		}
		// if (finalResults.size() >= k) {
		if (finalResultsArrayList.size() >= k) {
			if (starQueries.size() == 1) {
				return true;
			}
			if (starQueries.size() > 1 && satisfactionOfAlphaSchema()) {
				return true;
			}
			return false;
		}
		return false;
	}

	private void starRun(GraphDatabaseService knowledgeGraph, TreeNode<CalculationNode> rootTreeCalculationNode,
			NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) {

		if (rootTreeCalculationNode == null) {
			System.err.println("decompose query didn't call!");
		}
		// while top-k matches are not identified do
		while (!rootTreeCalculationNode.getData().callStarKIsEnough) {
			DummyFunctions.printIfItIsInDebuggedMode(
					"stark call from root finalResults.size so far is " + finalResultsArrayList.size());
			stark(knowledgeGraph, rootTreeCalculationNode, neighborIndexingInstance, cacheServer);

			// timeout
			// if (((System.nanoTime() - startTime) / 1e6) > 5500) {
			// this.timeOut = true;
			// noAnswerForAtLeastOneOfStarQueris = true; // alaki
			// noAnswerForAtLeastOneOfTreeNodes = true; // alaki
			// return;
			// }

			if (noAnswerForAtLeastOneOfStarQueris) {
				System.out.println("noAnswerForAtLeastOneOfStarQueris");
			}
			if (noAnswerForAtLeastOneOfTreeNodes) {
				System.out.println("noAnswerForAtLeastOneOfTreeNodes");
			}

			if (algorithmShouldFinish()) {
				break;
			}

		}

	}

	private ArrayList<StarGraphQuery> sortStarQueriesForCalcTree(ArrayList<StarGraphQuery> starQueries) {
		for (int i = 0; i < starQueries.size(); i++) {
			starQueries.get(i).calcTreeHeuristicValue = i;
		}
		Collections.sort(starQueries, new Comparator<StarGraphQuery>() {
			public int compare(StarGraphQuery f1, StarGraphQuery f2) {
				return Float.compare(f2.calcTreeHeuristicValue, f1.calcTreeHeuristicValue);
			}
		});
		return starQueries;
	}

	private TreeNode<CalculationNode> GenerateCalculationTree(GraphDatabaseService queryGraph,
			ArrayList<StarGraphQuery> starQueries) {
		int nodeIndex = 0;
		int tempNumberOfQueryEdges = numberOfQueryEdges;
		TreeNode<CalculationNode> rootTreeNode = new TreeNode<CalculationNode>();

		Set<Node> allThisQueryGraphNodes = new HashSet<Node>();
		for (Node node : GlobalGraphOperations.at(queryGraph).getAllNodes()) {
			allThisQueryGraphNodes.add(node);
		}

		CalculationNode rootCalculationNode = new CalculationNode(allThisQueryGraphNodes, tempNumberOfQueryEdges, k,
				nodeIndex++);
		// if we have just one starQuery
		if (starQueries.size() == 1) {
			rootCalculationNode.isStarQuery = true;
			rootCalculationNode.starQuery = starQueries.get(0);
		}

		rootTreeNode.setData(rootCalculationNode);
		calcTreeNodeMap.put(rootCalculationNode.nodeIndex, rootTreeNode);

		TreeNode<CalculationNode> parentTreeNode = new TreeNode<CalculationNode>();
		parentTreeNode = rootTreeNode;

		for (int i = 0; i < (starQueries.size() - 1); i++) {
			// for (StarGraphQuery starQuery : starQueries) {
			Set<Node> rightChildQueryNodes = new HashSet<Node>();
			rightChildQueryNodes.addAll(starQueries.get(i).allStarGraphQueryNodes);
			CalculationNode rChildNode = new CalculationNode(rightChildQueryNodes, starQueries.get(i),
					numberOfPartialMatches, nodeIndex++);

			tempNumberOfQueryEdges -= starQueries.get(i).allStarGraphQueryNodes.size() - 1;

			TreeNode<CalculationNode> rChildTreeNode = new TreeNode<CalculationNode>(rChildNode);
			calcTreeNodeMap.put(rChildNode.nodeIndex, rChildTreeNode);

			Set<Node> leftChildQueryNodes = new HashSet<Node>();
			for (int j = i + 1; j < starQueries.size(); j++) {
				leftChildQueryNodes.addAll(starQueries.get(j).allStarGraphQueryNodes);
			}
			CalculationNode lChildNode;

			if (i == (starQueries.size() - 2)) {
				lChildNode = new CalculationNode(leftChildQueryNodes, starQueries.get(starQueries.size() - 1),
						numberOfPartialMatches, nodeIndex++);
			} else {
				lChildNode = new CalculationNode(leftChildQueryNodes, tempNumberOfQueryEdges, numberOfPartialMatches,
						nodeIndex++);

			}
			TreeNode<CalculationNode> lChildTreeNode = new TreeNode<CalculationNode>(lChildNode);
			calcTreeNodeMap.put(lChildNode.nodeIndex, lChildTreeNode);

			for (Node node : GlobalGraphOperations.at(queryGraph).getAllNodes()) {
				if (rChildNode.allThisQueryGraphNodes.contains(node)
						&& lChildNode.allThisQueryGraphNodes.contains(node)) {
					parentTreeNode.getData().joinableQueryGraphNodes.add(node);
				}
			}
			parentTreeNode.getData().computeJoinableQueryGraphNodesForDebug();

			parentTreeNode.addChildren(lChildTreeNode, rChildTreeNode);
			joinLevelSiblingNodesMap.put(parentTreeNode.levelInCalcTree,
					new CalculationTreeSiblingNodes(lChildTreeNode, rChildTreeNode));
			maxJoinLevel = Math.max(parentTreeNode.levelInCalcTree, maxJoinLevel);
			parentTreeNode = lChildTreeNode;
		}

		for (Integer index : calcTreeNodeMap.keySet()) {
			if (calcTreeNodeMap.get(index).getData().isStarQuery) {
				calcTreeStarQueriesNodeMap.put(index, calcTreeNodeMap.get(index));
			}
		}
		return rootTreeNode;
	}

	private void stark(GraphDatabaseService knowledgeGraph, TreeNode<CalculationNode> calculationNode,
			NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) {
		// if (!calculationNode.getData().hasAtLeastOneUnvisitedReuslt()) {
		if (calculationNode.isLeaf()) {

			boolean hasResult = starkForLeaf(knowledgeGraph, calculationNode, neighborIndexingInstance, cacheServer);

			if (hasResult == false) {
				return;
			}

		} else {
			DummyFunctions.printIfItIsInDebuggedMode("starjoin called level:" + calculationNode.levelInCalcTree
					+ " queryGraphNode: " + calculationNode.getData().allThisQueryGraphNodeForDebug);
			starjoin(knowledgeGraph, calculationNode.getLeftChild(), calculationNode.getRightChild(), k,
					neighborIndexingInstance, cacheServer);

			if (noAnswerForAtLeastOneOfStarQueris || noAnswerForAtLeastOneOfTreeNodes) {
				return;
			}
			// if the alpha schema is not satisfied (less than k results and
			// upperbounds < parent's lowerbound)
			// stark has any other answers
			// we should call starjoin for these children again
			while (!calculationNode.getData().hasAtLeastOneUnvisitedReuslt()
					&& (!calculationNode.getLeftChild().getData().callStarKIsEnough
							|| !calculationNode.getRightChild().getData().callStarKIsEnough)) {
				DummyFunctions.printIfItIsInDebuggedMode("starjoin called itself!");

				starjoin(knowledgeGraph, calculationNode.getLeftChild(), calculationNode.getRightChild(), k,
						neighborIndexingInstance, cacheServer);

				if (calculationNode.getLeftChild().getData().graphResult.size() == 0
						|| calculationNode.getRightChild().getData().graphResult.size() == 0) {
					noAnswerForAtLeastOneOfTreeNodes = true;
					break;
				}

				// timeout
				// if (((System.nanoTime() - startTime) / 1e6) > 5500) {
				// this.timeOut = true;
				// noAnswerForAtLeastOneOfStarQueris = true; // alaki
				// noAnswerForAtLeastOneOfTreeNodes = true; // alaki
				// return;
				// }
			}
		}
		// }
	}

	public void starjoin(GraphDatabaseService knowledgeGraph, TreeNode<CalculationNode> leftChild,
			TreeNode<CalculationNode> rightChild, int k, NeighborIndexing neighborIndexingInstance,
			CacheServer cacheServer) {
		if (!rightChild.getData().callStarKIsEnough)
			stark(knowledgeGraph, rightChild, neighborIndexingInstance, cacheServer); // b
		// nazar
		// mirese
		// mituni
		// starkForLeaf seda
		// konim
		// hamishe baraye in bache

		if (!leftChild.getData().callStarKIsEnough)
			stark(knowledgeGraph, leftChild, neighborIndexingInstance, cacheServer);

		if (noAnswerForAtLeastOneOfStarQueris) {
			return;
		}

		// if both of us don't have any other answers, stark call for our
		// parents also is enough.
		if (leftChild.getData().callStarKIsEnough && rightChild.getData().callStarKIsEnough) {
			leftChild.getParent().getData().callStarKIsEnough = true;

		}

		if (leftChild.getData().graphResult.size() == 0 || rightChild.getData().graphResult.size() == 0) {
			return;
		}
		if (leftChild.isLeaf() || leftChild.getData().hasAtLeastOneUnvisitedReuslt()
				|| rightChild.getData().hasAtLeastOneUnvisitedReuslt()) {

			twoWayHashJoin(leftChild, rightChild, k);

		}

	}

	private boolean satisfactionOfAlphaSchema() {
		// when we come here we have at least k answers
		Collections.sort(finalResultsArrayList);
		finalResultLowerbound = finalResultsArrayList.get(k - 1).totalValue;

		for (Integer nodeIndex : calcTreeStarQueriesNodeMap.keySet()) {
			calcTreeStarQueriesNodeMap.get(nodeIndex).getData().updateLastAndFirstItems(queryNodeIntersectMap);
		}

		for (Integer mainNodeIndex : calcTreeStarQueriesNodeMap.keySet()) {
			// itself's lastItem
			calcTreeStarQueriesNodeMap.get(mainNodeIndex).getData().upperBound = calcTreeStarQueriesNodeMap
					.get(mainNodeIndex).getData().lastItemValueFPrime;

			for (Integer othersNodeIndex : calcTreeStarQueriesNodeMap.keySet()) {
				if (mainNodeIndex != othersNodeIndex) {
					calcTreeStarQueriesNodeMap.get(mainNodeIndex).getData().upperBound += calcTreeStarQueriesNodeMap
							.get(othersNodeIndex).getData().firstItemValueFPrime;
				}
			}
			DummyFunctions.printIfItIsInDebuggedMode("upperbound for nodeIndex -> " + mainNodeIndex + ": "
					+ calcTreeStarQueriesNodeMap.get(mainNodeIndex).getData().upperBound);
			if (calcTreeStarQueriesNodeMap.get(mainNodeIndex).getData().upperBound <= finalResultLowerbound) {
				calcTreeStarQueriesNodeMap.get(mainNodeIndex).getData().callStarKIsEnough = true;
			}
		}

		boolean shouldFinish = true;
		for (Integer nodeIndex : calcTreeStarQueriesNodeMap.keySet()) {
			if (!calcTreeStarQueriesNodeMap.get(nodeIndex).getData().callStarKIsEnough) {
				shouldFinish = false;
				break;
			}
		}
		return shouldFinish;
	}

	public void twoWayHashJoin(TreeNode<CalculationNode> leftChild, TreeNode<CalculationNode> rightChild, int k) {

		DummyFunctions.printIfItIsInDebuggedMode("two way hash join called");

		if (leftChild.getData().callStarKIsEnough && rightChild.getData().callStarKIsEnough) {
			leftChild.getParent().getData().callStarKIsEnough = true;

		}
		// bayad add bokone b qabli ha nabayad ruye qabli ha berize
		// these two relation will be join based on the joinable nodes.
		// the result will be stored in their parent.
		ArrayList<GraphResult> forParentResult = new ArrayList<GraphResult>();

		CalculationNode lNodeData = leftChild.getData();
		CalculationNode rNodeData = rightChild.getData();

		Set<Node> parentJoinableNode = leftChild.getParent().getData().joinableQueryGraphNodes;
		Set<Node> grandParentJoinableNode = null;
		if (leftChild.getParent().getParent() != null)
			grandParentJoinableNode = leftChild.getParent().getParent().getData().joinableQueryGraphNodes;

		// build phase:
		for (int i = lNodeData.lastVistitedItem + 1; i < lNodeData.graphResult.size(); i++) {

			GraphResult leftRes = lNodeData.graphResult.get(i);

			// if (!leftRes.isVisited) {
			// assumption: joinableQueryGraphNodes ordered the same in two
			// relation.
			// Iterator<Node> joinableNodesIterator =
			// parentJoinableNode.iterator();
			String joinableNodeIdsCsv = "";
			for (Node joinableNode : parentJoinableNode) {
				// while (joinableNodesIterator.hasNext()) {
				// Node joinableNode = joinableNodesIterator.next();
				joinableNodeIdsCsv += leftRes.assembledResult.get(joinableNode).node.getId() + ",";

			}
			leftRes.joinableKnowledgeNodeIds = joinableNodeIdsCsv;

			if (lNodeData.hashedResult.containsKey(joinableNodeIdsCsv)) {
				lNodeData.hashedResult.get(joinableNodeIdsCsv).add(leftRes);
			} else {
				ArrayList<GraphResult> grForAddToHashTable = new ArrayList<GraphResult>();
				grForAddToHashTable.add(leftRes);
				lNodeData.hashedResult.put(joinableNodeIdsCsv, grForAddToHashTable);
			}
			// }
		}

		// probe phase:
		for (int i = rNodeData.lastVistitedItem + 1; i < rNodeData.graphResult.size(); i++) {

			GraphResult rightRes = rNodeData.graphResult.get(i);
			// if (!rightRes.isVisited) {
			// Iterator<Node> joinableNodesIterator =
			// parentJoinableNode.iterator();
			String joinableNodeIdsCsv = "";
			for (Node joinableNode : parentJoinableNode) {
				// while (joinableNodesIterator.hasNext()) {
				// Node joinableNode = joinableNodesIterator.next();
				joinableNodeIdsCsv += rightRes.assembledResult.get(joinableNode).node.getId() + ",";

			}
			rightRes.joinableKnowledgeNodeIds = joinableNodeIdsCsv;
			// rNodeData.resultsInStringArr.add(joinableNodeIdsCsv);
			// }
		}
		for (int i = 0; i < rNodeData.graphResult.size(); i++) {

			GraphResult rightRes = rNodeData.graphResult.get(i);
			// String joinableNodeIdsCsv =
			// rightChild.getData().resultsInStringArr.get(i);
			// is it possible to find a complete match?
			if (lNodeData.hashedResult.containsKey(rightRes.joinableKnowledgeNodeIds)) {
				// if yes...
				// getting results in this bucket
				ArrayList<GraphResult> graphsInThisBucket = lNodeData.hashedResult
						.get(rightRes.joinableKnowledgeNodeIds);
				for (int graphResultDroppedIntoThisBucketIndex = 0; graphResultDroppedIntoThisBucketIndex < graphsInThisBucket
						.size(); graphResultDroppedIntoThisBucketIndex++) {
					GraphResult leftRes = graphsInThisBucket.get(graphResultDroppedIntoThisBucketIndex);
					HashSet<Long> checkForRepeatedNodes = new HashSet<Long>();
					boolean repeatedAnswerCatched = false;

					if (rightRes.joinableKnowledgeNodeIds.equals(leftRes.joinableKnowledgeNodeIds)) {

						// because avoiding repeated answers.
						if (!rightRes.isVisited || !leftRes.isVisited) {
							HashMap<Node, NodeWithValue> assembledResult = new HashMap<Node, NodeWithValue>();

							// for debug oracle
							HashMap<Integer, Integer> starQueryIndexDepthMap = new HashMap<Integer, Integer>();

							for (Integer starQueryIndex : leftRes.starQueryIndexDepthMap.keySet()) {
								starQueryIndexDepthMap.put(starQueryIndex,
										leftRes.starQueryIndexDepthMap.get(starQueryIndex));
							}

							// because right child is always a star query
							starQueryIndexDepthMap.put(rNodeData.nodeIndex,
									rightRes.starQueryIndexDepthMap.get(rNodeData.nodeIndex));

							for (Node qlNode : leftRes.assembledResult.keySet()) {
								NodeWithValue node = leftRes.assembledResult.get(qlNode);
								checkForRepeatedNodes.add(node.node.getId());
								assembledResult.put(qlNode, node);
							}
							for (Node qrNode : rightRes.assembledResult.keySet()) {
								NodeWithValue node = rightRes.assembledResult.get(qrNode);
								if (!parentJoinableNode.contains(qrNode)
										&& checkForRepeatedNodes.contains(node.node.getId())) {
									repeatedAnswerCatched = true;
									break;
								}
								assembledResult.put(qrNode, node);
							}
							if (!repeatedAnswerCatched) {
								GraphResult forAddingToFinal = new GraphResult(assembledResult, grandParentJoinableNode,
										starQueryIndexDepthMap);
								// if
								// (forAddingToFinal.nodesInThisGraphResultForDebug
								// .contains("417809_17657_2476577_711976_")) {
								// System.err.println("test1");
								// }
								DummyFunctions.printIfItIsInDebuggedMode(
										" forAddingToFinal: " + forAddingToFinal.nodesInThisGraphResultForDebug
												+ " totalValue -> " + forAddingToFinal.totalValue);
								forParentResult.add(forAddingToFinal);
							}
						}
					}
				}
			}
		}
		if (forParentResult != null && forParentResult.size() > 0) {
			// set the result in the parent node
			leftChild.getParent().getData().graphResult.addAll(forParentResult);
		}

		// if (rNodeData.resultsInStringArr.size() !=
		// rNodeData.graphResult.size()) {
		// System.err.println("rNodeData.resultsInStringArr.size()!=rNodeData.graphResult.size()");
		// }

		// leftChild.getParent().getData().updateLowerBound(k);
		// leftChild.getData().updateLastAndFirstItems(alpha);
		// rightChild.getData().updateLastAndFirstItems(1 - alpha);
		// leftChild.getData().updateUpperbound(rightChild.getData());
		// rightChild.getData().updateUpperbound(leftChild.getData());
		// makeAllPreviousResultsVisited(leftChild.getParent());

		for (int i = lNodeData.lastVistitedItem + 1; i < lNodeData.graphResult.size(); i++) {
			lNodeData.graphResult.get(i).isVisited = true;
		}
		for (int i = rNodeData.lastVistitedItem + 1; i < rNodeData.graphResult.size(); i++) {
			rNodeData.graphResult.get(i).isVisited = true;
		}

		lNodeData.lastVistitedItem = lNodeData.graphResult.size() - 1;
		rNodeData.lastVistitedItem = rNodeData.graphResult.size() - 1;
	}

	// private void makeAllPreviousResultsVisited(TreeNode<CalculationNode>
	// parent) {
	// // if set true for all of them it's faster than check if it's false then
	// // set it to true
	// for (int i = parent.getRightChild().getData().lastVistitedItem
	// + 1; i < parent.getRightChild().getData().graphResult.size(); i++) {
	// parent.getRightChild().getData().graphResult.get(i).isVisited = true;
	// }
	// for (int i = parent.getLeftChild().getData().lastVistitedItem
	// + 1; i < parent.getLeftChild().getData().graphResult.size(); i++) {
	// parent.getLeftChild().getData().graphResult.get(i).isVisited = true;
	// }
	//
	// parent.getLeftChild().getData().lastVistitedItem =
	// parent.getLeftChild().getData().graphResult.size() - 1;
	// parent.getRightChild().getData().lastVistitedItem =
	// parent.getRightChild().getData().graphResult.size() - 1;
	// }

	public boolean starkForLeaf(GraphDatabaseService knowledgeGraph, TreeNode<CalculationNode> treeNode,
			NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) {

		double starkForLeafStartTime, starkForLeafEndTime;
		starkForLeafStartTime = System.nanoTime();

		CalculationNode calcNodeData = treeNode.getData();
		DummyFunctions
				.printIfItIsInDebuggedMode("starkForLeaf was called! " + calcNodeData.allThisQueryGraphNodeForDebug);

		StarGraphQuery starQuery = calcNodeData.starQuery;
		// data ro chetori negah darim k baraye hashjoin khub bashe?
		// assembled data bar migardune va har node possible matched meqdar
		// similarity moshakhasi dare k ba khodesh hast.

		// joinable node haro add konam baraye khode in va baqalish

		// initializes set R=∅;
		ArrayList<StarGraphResult> resultStarGraphs = new ArrayList<StarGraphResult>();

		// copied priority queue from hashmap;
		PriorityQueue<StarGraphResult> priorityMatches = starkForLeafPQResults.get(starQuery);

		if (priorityMatches.isEmpty() && !calcNodeData.callStarKIsEnough) {
			long start_time = System.nanoTime();
			// find top-1 match pivoted at each node v in G
			ArrayList<StarGraphResult> topOneStarResults;

			if (DummyProperties.semanticChecking) {
				topOneStarResults = findTopOneMatchesPivotedAtVInG(knowledgeGraph, starQuery, neighborIndexingInstance,
						cacheServer);
			} else {
				topOneStarResults = findTopOneMatchesPivotedAtVInGWithoutSemanticChecking(knowledgeGraph, starQuery,
						neighborIndexingInstance);
			}
			long end_time = System.nanoTime();
			double difference = (end_time - start_time) / 1e6;
			DummyFunctions.printIfItIsInDebuggedMode("findTopOneMatchesPivotedAtVInG finished in " + difference
					+ "miliseconds! v id is:" + starQuery.pivotNode.getId());

			// add all best matches among the top-1 matches to P;
			priorityMatches.addAll(topOneStarResults);

			if (priorityMatches.size() == 0) {
				DummyFunctions.printIfItIsInDebuggedMode(
						"findTopOneMatchesPivotedAtVInG couldn't find any possible match for this!");
				noAnswerForAtLeastOneOfStarQueris = true;
				calcNodeData.callStarKIsEnough = true;
				return false;
			}
			// if(priorityMatches.size()==2 && starQuery.pivotNode.getId()==4){
			// System.out.println("");
			// }
			DummyFunctions.printIfItIsInDebuggedMode("priorityMatches size: " + priorityMatches.size() + " k:" + k
					+ " for query by pivot: " + calcNodeData.starQuery.pivotNode.getId());
			calcNodeData.firstPQItemSize = priorityMatches.size();
		}
		// while |R| < k do
		while (resultStarGraphs.size() < calcNodeData.numberOfPartialAnswersShouldBeFetched) {
			// pop the best match M (pivoted at v) from P; R = R U {M}
			if (priorityMatches.size() == 0) {
				calcNodeData.callStarKIsEnough = true;
				break;
			}

			StarGraphResult selectedAsResult = priorityMatches.poll();
			selectedAsResult.depthOfThisAnswerOfStarQuery = calcNodeData.depthOfDigging;
			// try {
			// bwTime.write(calcNode.nodeIndex + ";" +
			// selectedAsResult.pivotNode + ";" + selectedAsResult.value + ";
			// \n");
			// } catch (IOException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			calcNodeData.depthOfDigging++;

			// for debug start
			DummyFunctions.printIfItIsInDebuggedMode("priorityMatches: " + priorityMatches.size()
					+ "| selectedAsResult: " + selectedAsResult.allStarGraphResutlsNodesWithTheirValueForDebug);

			// float tempValue = selectedAsResult.value;
			// for debug end

			resultStarGraphs.add(selectedAsResult);

			// we should add this result into visited results;
			latticeVisitedResultsOfStarkForGenerateNextBestMatch.get(starQuery)
					.add(selectedAsResult.allStarGraphResutlsNodesWithTheirValueForDebug);

			// generate next best match M' (pivoted at v)
			StarGraphResult nextBestMatch;
			if (DummyProperties.semanticChecking) {
				nextBestMatch = generateNextBestMatch(knowledgeGraph, starQuery, selectedAsResult,
						neighborIndexingInstance, cacheServer);
			} else {
				nextBestMatch = generateNextBestMatchWithoutSemanticChecking(knowledgeGraph, starQuery,
						selectedAsResult, neighborIndexingInstance);

			}

			// insert M′ to P;
			if (nextBestMatch != null) {
				// for debug start
				// if (tempValue < nextBestMatch.value) {
				// System.err.println("nextBestMatch: " + nextBestMatch.value
				// + " has greater value than before match: " + tempValue);
				// }
				// for debug end
				priorityMatches.add(nextBestMatch);
				nextBestMatch.depthOfThisAnswerOfStarQuery = calcNodeData.depthOfDigging;

			}

		}

		if (priorityMatches.size() == 0) {
			calcNodeData.callStarKIsEnough = true;
			DummyFunctions
					.printIfItIsInDebuggedMode("callStarKIsEnough for: " + calcNodeData.allThisQueryGraphNodeForDebug);
		}
		starkForLeafPQResults.put(starQuery, priorityMatches);
		// return R as Q(G; k);

		for (StarGraphResult starGraphResult : resultStarGraphs) {
			HashMap<Node, NodeWithValue> assembledResult = new HashMap<Node, NodeWithValue>();
			assembledResult.put(starQuery.pivotNode, starGraphResult.pivotNode);
			for (Node qNode : starGraphResult.neighborsOfPivot.keySet()) {
				assembledResult.put(qNode, starGraphResult.neighborsOfPivot.get(qNode));
			}

			// for debug: oracle.
			HashMap<Integer, Integer> starQueryIndexDepthMap = new HashMap<Integer, Integer>();

			starQueryIndexDepthMap.put(calcNodeData.nodeIndex, starGraphResult.depthOfThisAnswerOfStarQuery + 1);

			if (treeNode.getParent() != null) {
				calcNodeData.graphResult.add(new GraphResult(assembledResult,
						treeNode.getParent().getData().joinableQueryGraphNodes, starQueryIndexDepthMap));
			} else {
				calcNodeData.graphResult.add(new GraphResult(assembledResult, null, starQueryIndexDepthMap));
			}

		}
		starkForLeafEndTime = System.nanoTime();
		DummyFunctions.printIfItIsInDebuggedMode("starkForLeaf nodeIndex:" + treeNode.getData().nodeIndex
				+ ", miliseconds: " + (starkForLeafEndTime - starkForLeafStartTime) / 1e6);

		// start for debug repeated answers
		// HashSet<String> visitedSet = new HashSet<String>();
		// for (GraphResult gr : calcNodeData.graphResult) {
		// if (visitedSet.contains(gr.nodesInThisGraphResultForDebug)) {
		// System.err.println("test3");
		// }
		// visitedSet.add(gr.nodesInThisGraphResultForDebug);
		// }
		// end for debug repeated answers

		return !resultStarGraphs.isEmpty();
	}

	private StarGraphResult generateNextBestMatch(GraphDatabaseService knowledgeGraph, StarGraphQuery starQuery,
			StarGraphResult selectedAsResult, NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) {

		NodeWithValue currentPivotInKnowledgeGraph = selectedAsResult.pivotNode;
		Node pivoteQueryNode = starQuery.pivotNode;
		ArrayList<Node> queryNeighborNodesInArrayList = new ArrayList<Node>();
		for (Relationship qEdge : starQuery.graphQuery.edgesOf(pivoteQueryNode)) {
			queryNeighborNodesInArrayList.add(qEdge.getOtherNode(pivoteQueryNode));
		}
		HashMap<NodeWithValue, HashMap<Node, ArrayListWithCursor>> possibleMatchesForThisKnowledgePivot = possibleMatchesForThisKnowledgePivotInThisStarQuery
				.get(starQuery);

		// boolean firstTimeGeneratingNextBestMatch = false;
		// if we didn't generate next best match for this pivot node.
		if (!possibleMatchesForThisKnowledgePivot.containsKey(currentPivotInKnowledgeGraph)) {
			// firstTimeGeneratingNextBestMatch = true;

			HashMap<Node, ArrayListWithCursor> hashMapOfTheQueryNodeWithItsPriorityQueues = new HashMap<Node, ArrayListWithCursor>();

			// int numberOfNeighbors = 0;
			int maxPossibleMatchNodeInAList = 0;
			for (Relationship qEdge : starQuery.graphQuery.edgesOf(pivoteQueryNode)) {
				// numberOfNeighbors++;
				Node neighborQNode = qEdge.getOtherNode(pivoteQueryNode);

				ArrayListWithCursor possibleNodeMatchesForThisQueryNode = new ArrayListWithCursor();

				HashSet<Long> pivotRelationsInKnowledgeGraphSet;

				if (qEdge.getStartNode().getId() == neighborQNode.getId()) {
					pivotRelationsInKnowledgeGraphSet = neighborIndexingInstance.graphInNeighborIndicesMap
							.get(currentPivotInKnowledgeGraph.node.getId());
				} else {
					pivotRelationsInKnowledgeGraphSet = neighborIndexingInstance.graphOutNeighborIndicesMap
							.get(currentPivotInKnowledgeGraph.node.getId());
				}

				for (Long neighborInKnowledgeGraphId : pivotRelationsInKnowledgeGraphSet) {
					Node neighborInKnowledgeGraph = knowledgeGraph.getNodeById(neighborInKnowledgeGraphId);

					float curSimValue = levenshtein.HowMuchTwoNodesAreSimilar(knowledgeGraph, neighborQNode,
							neighborInKnowledgeGraph, neighborIndexingInstance, cacheServer);

					if (curSimValue > DummyProperties.similarityThreshold) {
						NodeWithValue thisNode = new NodeWithValue(neighborInKnowledgeGraph, curSimValue);
						possibleNodeMatchesForThisQueryNode.nodeWithValueInDescOrder.add(thisNode);
					}
				}
				Collections.sort(possibleNodeMatchesForThisQueryNode.nodeWithValueInDescOrder);
				// start for debug
				// if (Dummy.DummyProperties.debuggMode) {
				// Iterator<NodeWithValue> candidateMatchIterator =
				// possibleNodeMatchesForThisQueryNode.nodeWithValueInDescOrder
				// .iterator();
				// String candidateMatch = "possible match for pivoteQueryNode:"
				// + pivoteQueryNode.getId()
				// + " matched with: " +
				// currentPivotInKnowledgeGraph.node.getId() + " | ";
				// candidateMatch += " , for neighborQNode: " +
				// neighborQNode.getId() + " | ";
				// int howManyPossibleNeighbors = 0;
				// float minSimilarity = Float.MAX_VALUE;
				// while (candidateMatchIterator.hasNext()) {
				// NodeWithValue n = candidateMatchIterator.next();
				// candidateMatch += ", " + n.node.getId();
				// howManyPossibleNeighbors++;
				// if (minSimilarity > n.simValue) {
				// minSimilarity = n.simValue;
				// }
				// }
				// DummyFunctions.printIfItIsInDebuggedMode(candidateMatch);
				// DummyFunctions.printIfItIsInDebuggedMode("howManyPossibleNeighbors:"
				// + howManyPossibleNeighbors);
				// DummyFunctions.printIfItIsInDebuggedMode("minSimilarity
				// between them:" + minSimilarity);
				// }
				// end for debug

				hashMapOfTheQueryNodeWithItsPriorityQueues.put(neighborQNode, possibleNodeMatchesForThisQueryNode);

				if (maxPossibleMatchNodeInAList < possibleNodeMatchesForThisQueryNode.nodeWithValueInDescOrder.size()) {
					maxPossibleMatchNodeInAList = possibleNodeMatchesForThisQueryNode.nodeWithValueInDescOrder.size();
				}
			}

			possibleMatchesForThisKnowledgePivot.put(currentPivotInKnowledgeGraph,
					hashMapOfTheQueryNodeWithItsPriorityQueues);

			possibleMatchesForThisKnowledgePivotInThisStarQuery.put(starQuery, possibleMatchesForThisKnowledgePivot);

			// for debug

		}

		// we seen this results before
		HashSet<String> sSeenGraphResult = latticeVisitedResultsOfStarkForGenerateNextBestMatch.get(starQuery);

		// these result are in queue to see but may be we can generate a better
		// one
		HashMap<NodeWithValue, PriorityQueue<StarGraphResult>> currentLatticeResultsOfStarkForGenerateNextBestMatchOfThisPivot = currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
				.get(starQuery);

		PriorityQueue<StarGraphResult> soFarResultsForThisPivoteNode = currentLatticeResultsOfStarkForGenerateNextBestMatchOfThisPivot
				.get(currentPivotInKnowledgeGraph);

		for (int i = 0; i < queryNeighborNodesInArrayList.size(); i++) {

			HashMap<Node, NodeWithValue> neighborsOfPivot = new HashMap<Node, NodeWithValue>();
			HashMap<Node, Integer> readFromTheseCursors = new HashMap<Node, Integer>();

			// float tempValue = 0;
			ArrayListWithCursor pqWithCursorSumOne = possibleMatchesForThisKnowledgePivot
					.get(currentPivotInKnowledgeGraph).get(queryNeighborNodesInArrayList.get(i));
			if ((pqWithCursorSumOne.cursor + 1) >= pqWithCursorSumOne.nodeWithValueInDescOrder.size()) {
				continue;
			}

			neighborsOfPivot.put(queryNeighborNodesInArrayList.get(i),
					pqWithCursorSumOne.nodeWithValueInDescOrder.get(pqWithCursorSumOne.cursor + 1));

			readFromTheseCursors.put(queryNeighborNodesInArrayList.get(i), pqWithCursorSumOne.cursor + 1);

			for (int j = 0; j < queryNeighborNodesInArrayList.size(); j++) {
				if (i != j) {
					ArrayListWithCursor pqWithCursor = possibleMatchesForThisKnowledgePivot
							.get(currentPivotInKnowledgeGraph).get(queryNeighborNodesInArrayList.get(j));

					neighborsOfPivot.put(queryNeighborNodesInArrayList.get(j),
							pqWithCursor.nodeWithValueInDescOrder.get(pqWithCursor.cursor));

					readFromTheseCursors.put(queryNeighborNodesInArrayList.get(j), pqWithCursor.cursor);
				}

			}

			// if we could find any next best match for this
			if (!neighborsOfPivot.isEmpty()) {
				StarGraphResult sgr = new StarGraphResult(currentPivotInKnowledgeGraph, neighborsOfPivot,
						readFromTheseCursors);

				if (!sSeenGraphResult.contains(sgr.allStarGraphResutlsNodesWithTheirValueForDebug)) {

					// shoud add to this:
					// latticeVisitedResultsOfStarkForGenerateNextBestMatch
					sSeenGraphResult.add(sgr.allStarGraphResutlsNodesWithTheirValueForDebug);

					latticeVisitedResultsOfStarkForGenerateNextBestMatch.put(starQuery, sSeenGraphResult);

					// should add to this:
					// currentLatticeResultsOfStarkForGenerateNextBestMatchOfThisPivot
					soFarResultsForThisPivoteNode.add(sgr);
					currentLatticeResultsOfStarkForGenerateNextBestMatchOfThisPivot.put(currentPivotInKnowledgeGraph,
							soFarResultsForThisPivoteNode);

				}
			}

		}
		// } //else
		currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery.put(starQuery,
				currentLatticeResultsOfStarkForGenerateNextBestMatchOfThisPivot);

		PriorityQueue<StarGraphResult> nextMatchPQ = currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
				.get(starQuery).get(currentPivotInKnowledgeGraph);

		// we couldn't find any next best match for this
		if (nextMatchPQ.size() == 0) {
			return null;
		}

		StarGraphResult topSgr = nextMatchPQ.poll();

		// cursors should update based on this result:
		for (Node qNode : topSgr.readKnowledgeNodesFromTheseCursorsBasedOnQNode.keySet()) {
			possibleMatchesForThisKnowledgePivot.get(currentPivotInKnowledgeGraph)
					.get(qNode).cursor = topSgr.readKnowledgeNodesFromTheseCursorsBasedOnQNode.get(qNode);
		}

		return topSgr;

	}

	public ArrayList<StarGraphResult> findTopOneMatchesPivotedAtVInG(GraphDatabaseService knowledgeGraph,
			StarGraphQuery starQuery, NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) {

		// for feature test start
		double sumDegreeOfPossiblePivots = 0d;
		int possiblePivotsCnt = 0;
		int numberOfPossiblePivotSet = 0;
		// for feature test end

		// we will fill top one results in this array
		ArrayList<StarGraphResult> topOneResults = new ArrayList<StarGraphResult>();

		// we want to traverse on all knowledgegraph nodes
		// ResourceIterable<Node> allKnowledgeGraphNodes =
		// GlobalGraphOperations.at(knowledgeGraph).getAllNodes();

		Node pivoteQueryNode = starQuery.pivotNode;
		HashSet<Long> possibleNodeIdSet = levenshtein.nodeLabelsIndex.get(neighborIndexingInstance.queryNodeLabelMap
				.get(starQuery.pivotNode.getId()).substring(0, DummyProperties.numberOfPrefixChars));

		numberOfPossiblePivotSet = possibleNodeIdSet.size();

		Set<Relationship> pivoteNeighborRelationships = starQuery.graphQuery.edgesOf(pivoteQueryNode);
		int totalNumberOfPAForThisSQ = 0;
		for (Long gpNodeId : possibleNodeIdSet) {

			Node currentKnowledgeGraphNode = knowledgeGraph.getNodeById(gpNodeId);

			HashMap<Node, NodeWithValue> neighborsOfPivot = new HashMap<Node, NodeWithValue>();
			NodeWithValue currentKnowledgeGraphNodeWithValue;

			float pivotSim = levenshtein.HowMuchTwoNodesAreSimilar(knowledgeGraph, pivoteQueryNode,
					currentKnowledgeGraphNode, neighborIndexingInstance, cacheServer);

			if (pivotSim > DummyProperties.similarityThreshold) {
				possiblePivotsCnt++;

				int totalNumberOfPAForThisPivot = 0;

				currentKnowledgeGraphNodeWithValue = new NodeWithValue(currentKnowledgeGraphNode, pivotSim);
				int neighborsCnt = 0;
				HashSet<Long> usedBeforeNodeIdSet = new HashSet<Long>();
				for (Relationship qEdge : pivoteNeighborRelationships) {
					int totalNumberOfPAForThisRelationOfThisPivot = 0;
					neighborsCnt++;
					// find adjacent node of pivotnoode in the query for this
					// relation
					Node neighborQNode = qEdge.getOtherNode(pivoteQueryNode);

					// finding all pivotNeighborsInG
					// possibly i should change this to a set. for bidirectional
					// edges. then we have repeated nodes.

					HashSet<Long> pivotRelationsInKnowledgeGraph;

					if (qEdge.getStartNode().getId() == neighborQNode.getId()) {
						pivotRelationsInKnowledgeGraph = neighborIndexingInstance.graphInNeighborIndicesMap
								.get(currentKnowledgeGraphNode.getId());
					} else {
						pivotRelationsInKnowledgeGraph = neighborIndexingInstance.graphOutNeighborIndicesMap
								.get(currentKnowledgeGraphNode.getId());
					}

					sumDegreeOfPossiblePivots += pivotRelationsInKnowledgeGraph.size();

					for (Long neighborInGId : pivotRelationsInKnowledgeGraph) {

						Node neighborInG = knowledgeGraph.getNodeById(neighborInGId);
						if (!usedBeforeNodeIdSet.contains(neighborInG.getId())) {
							float curSimValue = levenshtein.HowMuchTwoNodesAreSimilar(knowledgeGraph, neighborQNode,
									neighborInG, neighborIndexingInstance, cacheServer);

							if (curSimValue > DummyProperties.similarityThreshold) {
								totalNumberOfPAForThisRelationOfThisPivot++;
								// either it isn't add before for any node or
								// the
								// previos one has smaller similarity.
								// should add to the best neighbors of pivot.
								if (!neighborsOfPivot.containsKey(neighborQNode)
										|| neighborsOfPivot.get(neighborQNode).simValue < curSimValue) {
									neighborsOfPivot.put(neighborQNode, new NodeWithValue(neighborInG, curSimValue));

									usedBeforeNodeIdSet.remove(neighborsOfPivot.get(neighborQNode).node.getId());
									usedBeforeNodeIdSet.add(neighborInG.getId());

								}
							}

							// for training start
							// starQuery.potentialCounterMap.put(neighborQNode.getId(),
							// starQuery.potentialCounterMap.get(neighborQNode.getId())
							// + 1);
							// for training end
						}
					}

					if (neighborsOfPivot.get(neighborQNode) == null) {
						break;
					}

					if (totalNumberOfPAForThisPivot == 0) {
						totalNumberOfPAForThisPivot = totalNumberOfPAForThisRelationOfThisPivot;
					} else {
						totalNumberOfPAForThisPivot = totalNumberOfPAForThisPivot
								* totalNumberOfPAForThisRelationOfThisPivot;
					}
				} // other relation

				if (neighborsOfPivot.keySet().size() == neighborsCnt) {
					HashMap<Node, Integer> readKnowledgeNodesFromTheseCursorsBasedOnQNode = new HashMap<Node, Integer>();
					for (Node qNode : neighborsOfPivot.keySet()) {
						readKnowledgeNodesFromTheseCursorsBasedOnQNode.put(qNode, 0); // because
																						// it's
																						// top-1
					}
					StarGraphResult newStarGraphResult = new StarGraphResult(currentKnowledgeGraphNodeWithValue,
							neighborsOfPivot, readKnowledgeNodesFromTheseCursorsBasedOnQNode);

					// we shouldn't add this result to it, because it will be
					// visited by the orig PQ.

					HashMap<NodeWithValue, PriorityQueue<StarGraphResult>> resultOfStark = currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
							.get(starQuery);
					resultOfStark.put(currentKnowledgeGraphNodeWithValue,
							new PriorityQueue<StarGraphResult>(maxK, new StarGraphResultComparator()));
					currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery.put(starQuery, resultOfStark);
					topOneResults.add(newStarGraphResult);

					// for training start
					// starQuery.potentialCounterMap.put(starQuery.pivotNode.getId(),
					// starQuery.potentialCounterMap.get(starQuery.pivotNode.getId())
					// + 1);
					// for training end

					totalNumberOfPAForThisSQ += totalNumberOfPAForThisPivot;
				}

			}
		}

		// for training start
		starQuery.numberOfPAEstimate = totalNumberOfPAForThisSQ;
		starQuery.avgDegreeOfPossiblePivots = (sumDegreeOfPossiblePivots / possiblePivotsCnt);
		starQuery.numberOfPossiblePivots = numberOfPossiblePivotSet;
		// for training end

		if (Dummy.DummyProperties.debuggMode) {
			System.out.println("numberOfPossiblePivotMatchedLabel: " + possiblePivotsCnt);
			System.out.println("avgDegreeOfPossiblePivots: " + starQuery.avgDegreeOfPossiblePivots);
			System.out.println("numberOfPivotEstimatePrefix: " + numberOfPossiblePivotSet);
		}
		return topOneResults;
	}

	public ArrayList<StarGraphResult> findTopOneMatchesPivotedAtVInGWithoutSemanticChecking(
			GraphDatabaseService knowledgeGraph, StarGraphQuery starQuery, NeighborIndexing neighborIndexingInstance) {

		// we will fill top one results in this array
		ArrayList<StarGraphResult> topOneResults = new ArrayList<StarGraphResult>();

		// we want to traverse on all knowledgegraph nodes
		ResourceIterable<Node> allKnowledgeGraphNodes = GlobalGraphOperations.at(knowledgeGraph).getAllNodes();

		Node pivoteQueryNode = starQuery.pivotNode;

		// for (Node currentKnowledgeGraphNode : allKnowledgeGraphNodes) {
		Set<Relationship> pivoteNeighborRelationships = starQuery.graphQuery.edgesOf(pivoteQueryNode);
		for (Node currentKnowledgeGraphNode : allKnowledgeGraphNodes) {

			HashMap<Node, NodeWithValue> neighborsOfPivot = new HashMap<Node, NodeWithValue>();
			NodeWithValue currentKnowledgeGraphNodeWithValue;

			// float pivotSim =
			// levenshtein.HowMuchTwoNodesAreSimilar(pivoteQueryNode,
			// currentKnowledgeGraphNode);
			if (neighborIndexingInstance.queryInDegreeMap
					.get(pivoteQueryNode.getId()) <= neighborIndexingInstance.graphInDegreeMap
							.get(currentKnowledgeGraphNode.getId())
					&& neighborIndexingInstance.queryOutDegreeMap
							.get(pivoteQueryNode.getId()) <= neighborIndexingInstance.graphOutDegreeMap
									.get(currentKnowledgeGraphNode.getId())) {

				// if (currentKnowledgeGraphNodeWithValue. >
				// DummyProperties.similarityThreshold) {
				currentKnowledgeGraphNodeWithValue = new NodeWithValue(currentKnowledgeGraphNode, 1);
				int neighborsCnt = 0;
				HashSet<Long> usedBeforeNodeIdSet = new HashSet<Long>();
				for (Relationship qEdge : pivoteNeighborRelationships) {
					neighborsCnt++;
					// find adjacent node of pivotnoode in the query for this
					// relation
					Node neighborQNode = qEdge.getOtherNode(pivoteQueryNode);

					// finding all pivotNeighborsInG
					// possibly i should change this to a set. for bidirectional
					// edges. then we have repeated nodes.

					Iterator<Long> pivotRelationsInKnowledgeGraph;

					if (qEdge.getStartNode().getId() == neighborQNode.getId()) {
						pivotRelationsInKnowledgeGraph = neighborIndexingInstance.graphInNeighborIndicesMap
								.get(currentKnowledgeGraphNode.getId()).iterator();
					} else {
						pivotRelationsInKnowledgeGraph = neighborIndexingInstance.graphOutNeighborIndicesMap
								.get(currentKnowledgeGraphNode.getId()).iterator();
					}

					while (pivotRelationsInKnowledgeGraph.hasNext()) {
						Long neighborInGId = pivotRelationsInKnowledgeGraph.next();
						Node neighborInG = knowledgeGraph.getNodeById(neighborInGId);

						// float curSimValue =
						// levenshtein.HowMuchTwoNodesAreSimilar(neighborQNode,
						// neighborInG);
						if (neighborIndexingInstance.queryInDegreeMap
								.get(neighborQNode.getId()) <= neighborIndexingInstance.graphInDegreeMap
										.get(neighborInG.getId())
								&& neighborIndexingInstance.queryOutDegreeMap
										.get(neighborQNode.getId()) <= neighborIndexingInstance.graphOutDegreeMap
												.get(neighborInG.getId())) {

							if (!neighborsOfPivot.containsKey(neighborQNode)) {
								if (!usedBeforeNodeIdSet.contains(neighborInG.getId())) {
									neighborsOfPivot.put(neighborQNode, new NodeWithValue(neighborInG, 1));

									usedBeforeNodeIdSet.add(neighborInG.getId());
									break;
								}
							}
						}
						if (neighborsOfPivot.size() == pivoteQueryNode.getDegree()) {
							break;
						}
					}

					if (neighborsOfPivot.get(neighborQNode) == null) {
						break;
					}
				}

				if (neighborsOfPivot.keySet().size() == neighborsCnt) {
					HashMap<Node, Integer> readKnowledgeNodesFromTheseCursorsBasedOnQNode = new HashMap<Node, Integer>();
					for (Node qNode : neighborsOfPivot.keySet()) {
						readKnowledgeNodesFromTheseCursorsBasedOnQNode.put(qNode, 0); // because
																						// it's
																						// top-1
					}
					StarGraphResult newStarGraphResult = new StarGraphResult(currentKnowledgeGraphNodeWithValue,
							neighborsOfPivot, readKnowledgeNodesFromTheseCursorsBasedOnQNode);

					// we shouldn't add this result to it, because it will be
					// visited by the orig PQ.

					HashMap<NodeWithValue, PriorityQueue<StarGraphResult>> resultOfStark = currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
							.get(starQuery);
					resultOfStark.put(currentKnowledgeGraphNodeWithValue,
							new PriorityQueue<StarGraphResult>(maxK, new StarGraphResultComparator()));
					currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery.put(starQuery, resultOfStark);
					topOneResults.add(newStarGraphResult);

				}

			}
			if ((topOneResults.size() / 3000000) >= 1) {
				System.out.println("topOneResults.size(): " + topOneResults.size());
				break;
			}
		}
		return topOneResults;
	}

	private StarGraphResult generateNextBestMatchWithoutSemanticChecking(GraphDatabaseService knowledgeGraph,
			StarGraphQuery starQuery, StarGraphResult selectedAsResult, NeighborIndexing neighborIndexingInstance) {

		NodeWithValue currentPivotInKnowledgeGraph = selectedAsResult.pivotNode;
		Node pivoteQueryNode = starQuery.pivotNode;
		ArrayList<Node> queryNeighborNodesInArrayList = new ArrayList<Node>();
		for (Relationship qEdge : starQuery.graphQuery.edgesOf(pivoteQueryNode)) {
			queryNeighborNodesInArrayList.add(qEdge.getOtherNode(pivoteQueryNode));
		}
		HashMap<NodeWithValue, HashMap<Node, ArrayListWithCursor>> possibleMatchesForThisKnowledgePivot = possibleMatchesForThisKnowledgePivotInThisStarQuery
				.get(starQuery);

		// boolean firstTimeGeneratingNextBestMatch = false;
		// if we didn't generate next best match for this pivot node.
		if (!possibleMatchesForThisKnowledgePivot.containsKey(currentPivotInKnowledgeGraph)) {
			// firstTimeGeneratingNextBestMatch = true;

			HashMap<Node, ArrayListWithCursor> hashMapOfTheQueryNodeWithItsPriorityQueues = new HashMap<Node, ArrayListWithCursor>();

			// int numberOfNeighbors = 0;
			int maxPossibleMatchNodeInAList = 0;
			HashSet<Long> usedBeforeForAnotherNode = new HashSet<Long>();
			int allQPivotNeighbors = starQuery.graphQuery.edgesOf(pivoteQueryNode).size();
			for (Relationship qEdge : starQuery.graphQuery.edgesOf(pivoteQueryNode)) {
				// numberOfNeighbors++;
				Node neighborQNode = qEdge.getOtherNode(pivoteQueryNode);

				ArrayListWithCursor possibleNodeMatchesForThisQueryNode = new ArrayListWithCursor();

				Iterator<Long> pivotRelationsInKnowledgeGraphIterator;

				if (qEdge.getStartNode().getId() == neighborQNode.getId()) {
					pivotRelationsInKnowledgeGraphIterator = neighborIndexingInstance.graphInNeighborIndicesMap
							.get(currentPivotInKnowledgeGraph.node.getId()).iterator();
				} else {
					pivotRelationsInKnowledgeGraphIterator = neighborIndexingInstance.graphOutNeighborIndicesMap
							.get(currentPivotInKnowledgeGraph.node.getId()).iterator();
				}

				while (pivotRelationsInKnowledgeGraphIterator.hasNext()) {
					Long neighborInKnowledgeGraphId = pivotRelationsInKnowledgeGraphIterator.next();
					Node neighborInKnowledgeGraph = knowledgeGraph.getNodeById(neighborInKnowledgeGraphId);

					// float curSimValue =
					// levenshtein.HowMuchTwoNodesAreSimilar(neighborQNode,
					// neighborInKnowledgeGraph);
					// if (curSimValue > DummyProperties.similarityThreshold) {
					if (neighborIndexingInstance.queryInDegreeMap
							.get(neighborQNode.getId()) <= neighborIndexingInstance.graphInDegreeMap
									.get(neighborInKnowledgeGraph.getId())
							&& neighborIndexingInstance.queryOutDegreeMap
									.get(neighborQNode.getId()) <= neighborIndexingInstance.graphOutDegreeMap
											.get(neighborInKnowledgeGraph.getId())) {
						NodeWithValue thisNode = new NodeWithValue(neighborInKnowledgeGraph, 1);
						if (Math.random() <= (1 / allQPivotNeighbors)
								&& !usedBeforeForAnotherNode.contains(thisNode.node.getId())) {
							possibleNodeMatchesForThisQueryNode.nodeWithValueInDescOrder.add(thisNode);
							usedBeforeForAnotherNode.add(thisNode.node.getId());
						}
					}
				}
				Collections.sort(possibleNodeMatchesForThisQueryNode.nodeWithValueInDescOrder);
				// start for debug
				if (Dummy.DummyProperties.debuggMode) {
					Iterator<NodeWithValue> candidateMatchIterator = possibleNodeMatchesForThisQueryNode.nodeWithValueInDescOrder
							.iterator();
					String candidateMatch = "possible match for pivoteQueryNode: " + pivoteQueryNode.getId()
							+ " matched with: " + currentPivotInKnowledgeGraph.node.getId() + " | ";
					candidateMatch += " ,  for neighborQNode: " + neighborQNode.getId() + " | ";
					int howManyPossibleNeighbors = 0;
					float minSimilarity = Float.MAX_VALUE;
					while (candidateMatchIterator.hasNext()) {
						NodeWithValue n = candidateMatchIterator.next();
						candidateMatch += ", " + n.node.getId();
						howManyPossibleNeighbors++;
						if (minSimilarity > n.simValue) {
							minSimilarity = n.simValue;
						}
					}
					DummyFunctions.printIfItIsInDebuggedMode(candidateMatch);
					DummyFunctions.printIfItIsInDebuggedMode("howManyPossibleNeighbors:" + howManyPossibleNeighbors);
					DummyFunctions.printIfItIsInDebuggedMode("minSimilarity between them:" + minSimilarity);
				}
				// end for debug

				hashMapOfTheQueryNodeWithItsPriorityQueues.put(neighborQNode, possibleNodeMatchesForThisQueryNode);

				if (maxPossibleMatchNodeInAList < possibleNodeMatchesForThisQueryNode.nodeWithValueInDescOrder.size()) {
					maxPossibleMatchNodeInAList = possibleNodeMatchesForThisQueryNode.nodeWithValueInDescOrder.size();
				}
			}

			possibleMatchesForThisKnowledgePivot.put(currentPivotInKnowledgeGraph,
					hashMapOfTheQueryNodeWithItsPriorityQueues);

			possibleMatchesForThisKnowledgePivotInThisStarQuery.put(starQuery, possibleMatchesForThisKnowledgePivot);

			// for debug

		}

		// we seen this results before
		HashSet<String> sSeenGraphResult = latticeVisitedResultsOfStarkForGenerateNextBestMatch.get(starQuery);

		// these result are in queue to see but may be we can generate a better
		// one
		HashMap<NodeWithValue, PriorityQueue<StarGraphResult>> currentLatticeResultsOfStarkForGenerateNextBestMatchOfThisPivot = currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
				.get(starQuery);

		PriorityQueue<StarGraphResult> soFarResultsForThisPivoteNode = currentLatticeResultsOfStarkForGenerateNextBestMatchOfThisPivot
				.get(currentPivotInKnowledgeGraph);

		for (int i = 0; i < queryNeighborNodesInArrayList.size(); i++) {

			HashMap<Node, NodeWithValue> neighborsOfPivot = new HashMap<Node, NodeWithValue>();
			HashMap<Node, Integer> readFromTheseCursors = new HashMap<Node, Integer>();

			// float tempValue = 0;
			ArrayListWithCursor pqWithCursorSumOne = possibleMatchesForThisKnowledgePivot
					.get(currentPivotInKnowledgeGraph).get(queryNeighborNodesInArrayList.get(i));
			if ((pqWithCursorSumOne.cursor + 1) >= pqWithCursorSumOne.nodeWithValueInDescOrder.size()) {
				continue;
			}

			neighborsOfPivot.put(queryNeighborNodesInArrayList.get(i),
					pqWithCursorSumOne.nodeWithValueInDescOrder.get(pqWithCursorSumOne.cursor + 1));

			readFromTheseCursors.put(queryNeighborNodesInArrayList.get(i), pqWithCursorSumOne.cursor + 1);

			for (int j = 0; j < queryNeighborNodesInArrayList.size(); j++) {
				if (i != j) {
					ArrayListWithCursor pqWithCursor = possibleMatchesForThisKnowledgePivot
							.get(currentPivotInKnowledgeGraph).get(queryNeighborNodesInArrayList.get(j));

					neighborsOfPivot.put(queryNeighborNodesInArrayList.get(j),
							pqWithCursor.nodeWithValueInDescOrder.get(pqWithCursor.cursor));

					readFromTheseCursors.put(queryNeighborNodesInArrayList.get(j), pqWithCursor.cursor);
				}

			}

			// if we could find any next best match for this
			if (!neighborsOfPivot.isEmpty()) {
				StarGraphResult sgr = new StarGraphResult(currentPivotInKnowledgeGraph, neighborsOfPivot,
						readFromTheseCursors);

				if (!sSeenGraphResult.contains(sgr.allStarGraphResutlsNodesWithTheirValueForDebug)) {

					// shoud add to this:
					// latticeVisitedResultsOfStarkForGenerateNextBestMatch
					sSeenGraphResult.add(sgr.allStarGraphResutlsNodesWithTheirValueForDebug);

					latticeVisitedResultsOfStarkForGenerateNextBestMatch.put(starQuery, sSeenGraphResult);

					// should add to this:
					// currentLatticeResultsOfStarkForGenerateNextBestMatchOfThisPivot
					soFarResultsForThisPivoteNode.add(sgr);
					currentLatticeResultsOfStarkForGenerateNextBestMatchOfThisPivot.put(currentPivotInKnowledgeGraph,
							soFarResultsForThisPivoteNode);

				}
			}

		}
		// } //else
		currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery.put(starQuery,
				currentLatticeResultsOfStarkForGenerateNextBestMatchOfThisPivot);

		PriorityQueue<StarGraphResult> nextMatchPQ = currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery
				.get(starQuery).get(currentPivotInKnowledgeGraph);

		// we couldn't find any next best match for this
		if (nextMatchPQ.size() == 0) {
			return null;
		}

		StarGraphResult topSgr = nextMatchPQ.poll();

		// cursors should update based on this result:
		for (Node qNode : topSgr.readKnowledgeNodesFromTheseCursorsBasedOnQNode.keySet()) {
			possibleMatchesForThisKnowledgePivot.get(currentPivotInKnowledgeGraph)
					.get(qNode).cursor = topSgr.readKnowledgeNodesFromTheseCursorsBasedOnQNode.get(qNode);
		}

		return topSgr;

	}

	public String getHowMuchResultAreInCalcNodes() {
		String str = "HowMuchResultAreInCalcNodes: ";
		for (Integer nodeIndex : this.calcTreeNodeMap.keySet()) {
			str += " <node:" + nodeIndex + ", " + this.calcTreeNodeMap.get(nodeIndex).getData().graphResult.size()
					+ ">,  ";
		}
		return str;
	}
}
