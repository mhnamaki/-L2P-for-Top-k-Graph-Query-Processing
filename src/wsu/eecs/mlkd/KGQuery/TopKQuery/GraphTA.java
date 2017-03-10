package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.impl.transaction.log.pruning.Threshold;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;
import wsu.eecs.mlkd.KGQuery.algo.VF2.*;

public class GraphTA {

	public GraphDatabaseService queryGraph;
	public GraphDatabaseService knowledgeGraph;
	private VF2Matcher vf2;
	public int k;
	private Map<Node, ArrayList<SingleMatchedNode>> aloneNodeMatchedListsMap;
	private Map<Node, Integer> cursorBasedOnQueryNode;
	private Map<Relationship, ArrayList<SingleMatchedEdge>> aloneEdgeMatchedListsMap;
	private Map<String, MatchableNodesWithValue> topKAnswers;
	private int queryNodesSize = 0;
	private int queryRelationshipSize = 0;
	private float threshold = 0;
	private float upperBound = Integer.MAX_VALUE;
	private int[][] resultMatrix;
	boolean endOfOneOfCursors = false;
	Levenshtein levenshtein;
	HashSet<String> soFarSeenAnswers = new HashSet<String>();

	public GraphTA(GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph, int k,
			Levenshtein levenshtein) {
		this.queryGraph = queryGraph;
		this.knowledgeGraph = knowledgeGraph;
		this.k = k;
		this.levenshtein = levenshtein;
		this.vf2 = new VF2Matcher(levenshtein);
		aloneEdgeMatchedListsMap = new HashMap<Relationship, ArrayList<SingleMatchedEdge>>();
		cursorBasedOnQueryNode = new HashMap<Node, Integer>();
		aloneNodeMatchedListsMap = new HashMap<Node, ArrayList<SingleMatchedNode>>();
		topKAnswers = new HashMap<String, MatchableNodesWithValue>();

	}

	public List<String> runGraphTA() {
		int debug_while_cnt = 0;
		initialize();
		while (!ShouldFinishNow()) {

			DummyFunctions.printIfItIsInDebuggedMode(debug_while_cnt++);

			ResourceIterable<Node> allNodes = GlobalGraphOperations.at(queryGraph).getAllNodes();
			for (Node queryNodeItem : allNodes) {

				int cursor = cursorBasedOnQueryNode.get(queryNodeItem);
				DummyFunctions.printIfItIsInDebuggedMode(queryNodeItem.getId() + ", cursor: " + cursor);
				if (aloneNodeMatchedListsMap.get(queryNodeItem).size() > cursor) {
					SingleMatchedNode currentKnowledgeNode = aloneNodeMatchedListsMap.get(queryNodeItem).get(cursor);
					// subgraph isomorphism including currentNode
					int queryNodeManualId = getUniqueManualIdByNode(queryNodeItem);
					int knowledgeNodeManualId = getUniqueManualIdByNode(currentKnowledgeNode.node);
					HashMap<Integer, Integer> isomorphismInitStateMap = new HashMap<Integer, Integer>();
					isomorphismInitStateMap.put(queryNodeManualId, knowledgeNodeManualId);
					// DummyFunctions.printIfItIsInDebuggedMode("start another
					// VF2 for initial match: qNodeUMId:"
					// + queryNodeManualId + " GNodeUMId" +
					// knowledgeNodeManualId);
					// long start_time = System.nanoTime();
					resultMatrix = vf2.matchWithInitialMappedNode(knowledgeGraph, queryGraph, isomorphismInitStateMap);
					// long end_time = System.nanoTime();
					// double difference = (end_time - start_time) / 1e6;
					// DummyFunctions.printIfItIsInDebuggedMode("VF2 finished in
					// " + difference + " miliseconds!");

					for (int matchNumber = 0; IsThereAMatchInResultMatrix(matchNumber); matchNumber++) {
						// there is a match in result[matchNumber]
						String matchString = "";
						MatchableNodesWithValue mnwv = new MatchableNodesWithValue();
						for (int i = 0; i < resultMatrix[matchNumber].length; i++) {
							if (resultMatrix[matchNumber][i] != -1) {

								matchString += DummyFunctions
										.getNodeByProperty(knowledgeGraph, resultMatrix[matchNumber][i]).getId() + "-";
								Node queryNodeMatched = DummyFunctions.getNodeByProperty(queryGraph, i);
								Node knowledgeNodeMatched = DummyFunctions.getNodeByProperty(knowledgeGraph,
										resultMatrix[matchNumber][i]);

								float valueOfKnowledgeGraphMatchedNode = findValueOfNodeInList(knowledgeNodeMatched,
										aloneNodeMatchedListsMap.get(queryNodeMatched));
								mnwv.SimilarityValueOfKnowledgeGraphNode.put(knowledgeNodeMatched,
										valueOfKnowledgeGraphMatchedNode);
								mnwv.sumValue += valueOfKnowledgeGraphMatchedNode;

							}

						}
						DummyFunctions.printIfItIsInDebuggedMode("found a matched: " + matchString);
						if (!soFarSeenAnswers.contains(matchString))
							insertAnswerInTopKAnswersIfNeededAndUpdateThreshold(matchString, mnwv);

					}

				}
				int newCursorIndex = cursorBasedOnQueryNode.get(queryNodeItem) + 1;
				UpdateUpperBound();

				cursorBasedOnQueryNode.put(queryNodeItem, newCursorIndex);

				if (upperBound <= 0) {
					System.out.println("upperbound is zero!");
					break;
				}

			}
		}
		return printResult();

	}

	private List<String> printResult() {
		List<String> results = new ArrayList<String>();

		// System.out.println();
		results.add("");

		// System.out.println("Final Result: top-" + k + " answers.");
		results.add("Final Result: top-" + k + " answers.");
		ArrayList<ResultsItem> arr = new ArrayList<ResultsItem>();
		for (String key : topKAnswers.keySet()) {
			ResultsItem ri = new ResultsItem();
			ri.match = key;
			ri.value = topKAnswers.get(key).sumValue;
			arr.add(ri);
		}
		Collections.sort(arr);
		for (int k = 0; k < arr.size(); k++) {
			// System.out.println(arr.get(k).match + " -> " + arr.get(k).value);
			results.add(arr.get(k).match + " -> " + arr.get(k).value);
		}
		return results;
	}

	private boolean ShouldFinishNow() {
		if (upperBound <= threshold && topKAnswers.size() >= k)
			return true;

		if (upperBound == 0) {
			return true;
		}
		// if (endOfOneOfCursors)
		// return true;

		return false;
	}

	private void UpdateUpperBound() {
		ResourceIterable<Node> allNodes = GlobalGraphOperations.at(queryGraph).getAllNodes();
		float sumValue = 0;
		for (Node queryNodeItem : allNodes) {
			int cursor = cursorBasedOnQueryNode.get(queryNodeItem);
			if (aloneNodeMatchedListsMap.get(queryNodeItem).size() > cursor) {
				SingleMatchedNode currentKnowledgeNode = aloneNodeMatchedListsMap.get(queryNodeItem).get(cursor);
				sumValue += currentKnowledgeNode.value;
			}
			// System.out.println("cursor:" + cursor + "for node: " +
			// queryNodeItem.getProperty("name"));

		}
		upperBound = sumValue;
		System.out.println("Upperbound: " + upperBound);
	}

	private void insertAnswerInTopKAnswersIfNeededAndUpdateThreshold(String matchString, MatchableNodesWithValue mnwv) {

		soFarSeenAnswers.add(matchString);

		// may be it is better to have a priority queue instead of map
		if (topKAnswers.size() < k || mnwv.sumValue > threshold) {
			float minTupleExistInResultTable = Float.MAX_VALUE;

			if (topKAnswers.size() >= k) {

				if (topKAnswers.size() > k) {
					System.out.println("Warning: your matching table rows is larger than k!");
				}

				boolean isRemoved = false; // keeping exact (k-1) items in this
											// step!

				for (String key : topKAnswers.keySet()) {

					if (topKAnswers.get(key).sumValue < threshold) {
						System.out.println("Warning: your matching table has a row with sumValue less than threshold!");
					}

					if (!isRemoved && topKAnswers.get(key).sumValue <= threshold) {
						topKAnswers.remove(key);
						isRemoved = true;
						break;
					}
				}
			}

			topKAnswers.put(matchString, mnwv);
			System.out.println(topKAnswers.size() + " of top-" + k + " answers were found so far!");
			for (String key : topKAnswers.keySet()) {
				if (topKAnswers.get(key).sumValue < minTupleExistInResultTable) {
					minTupleExistInResultTable = topKAnswers.get(key).sumValue;
				}

			}

			threshold = minTupleExistInResultTable;

			System.out.println("Threshold: " + threshold);
		}

	}

	private float findValueOfNodeInList(Node knowledgeNodeMatched, ArrayList<SingleMatchedNode> arrayList) {
		// System.out.println("knowledgeNodeMatched name is: " +
		// knowledgeNodeMatched.getProperty("name"));
		for (int index = 0; index < arrayList.size(); index++) {
			// System.out.println(
			// "node in arraylist for random access is: " +
			// arrayList.get(index).node.getProperty("name"));
			if (arrayList.get(index).node.getId() == knowledgeNodeMatched.getId()) {
				return arrayList.get(index).value;
			}

		}
		DummyFunctions.printIfItIsInDebuggedMode(
				"findValueOfNodeInList cannot find the true value of node " + knowledgeNodeMatched.getId());

		return 0;

	}

	private boolean IsThereAMatchInResultMatrix(int matchNumber) {
		for (int i = 0; i < resultMatrix[matchNumber].length; i++) {
			if (resultMatrix[matchNumber][i] != -1) {
				return true;
			}
		}
		return false;
	}

	public void initialize() {

		// Print properties
		// ResourceIterable<Node> allNodes =
		// GlobalGraphOperations.at(queryGraph).getAllNodes();
		// for (Node nodeItem : allNodes) {
		// Iterable<String> propertyItarators = nodeItem.getPropertyKeys();
		// for (String property : propertyItarators) {
		// System.out.println(property);
		// }
		// }

		// for VF2 implementations
		DummyFunctions.printIfItIsInDebuggedMode("createLists ");
		createLists();
		DummyFunctions.printIfItIsInDebuggedMode("addSimilarNodesToMapLists");
		addSimilarNodesToMapLists();
		// addSimilarEdgesToMapLists();
		DummyFunctions.printIfItIsInDebuggedMode("sorting");
		sorting();
		DummyFunctions.printIfItIsInDebuggedMode("initializeCursors");
		initializeCursors();

	}

	private void initializeCursors() {
		ResourceIterable<Node> allNodes = GlobalGraphOperations.at(queryGraph).getAllNodes();
		for (Node nodeItem : allNodes) {
			cursorBasedOnQueryNode.put(nodeItem, 0);
		}
	}

	// private void addSimilarEdgesToMapLists() {
	// Iterable<Relationship> allQueryRelationships =
	// GlobalGraphOperations.at(queryGraph).getAllRelationships();
	// Iterable<Relationship> allKnowledgeReleationships =
	// GlobalGraphOperations.at(knowledgeGraph)
	// .getAllRelationships();
	// for (Relationship qRel : allQueryRelationships) {
	// for (Relationship knldgRel : allKnowledgeReleationships) {
	// float val = HowMuchTwoEdgeAreSimilar(qRel, knldgRel);
	// if (val > 0) {
	// SingleMatchedEdge relMatch = new SingleMatchedEdge();
	//
	// relMatch.relationship = knldgRel;
	// aloneEdgeMatchedListsMap.get(qRel).add(relMatch);
	// }
	// }
	// }
	// }

	private float HowMuchTwoEdgeAreSimilar(Relationship qRel, Relationship knldgRel) {

		// TO:DO: based on nodes similarities;

		// String qProperties = "";
		// Iterable<String> qRelIterator = qRel.getPropertyKeys();
		// for (String property : qRelIterator) {
		// qProperties += property;
		// }
		//
		// String knldgProperties = "";
		// Iterable<String> knldglabelIterator = knldgRel.getPropertyKeys();
		// for (String property : knldglabelIterator) {
		// knldgProperties += property;
		// }
		//
		// return Levenshtein.normalizedDistance(qProperties, knldgProperties);
		return 1;
	}

	private void addSimilarNodesToMapLists() {
		int debugg_cnt_seen_knowledge_nodes = 0;
		int debugg_cnt_seen_query_nodes = 0;

		ResourceIterable<Node> allQueryNodes = GlobalGraphOperations.at(queryGraph).getAllNodes();
		ResourceIterable<Node> allKnowledgeNodes = GlobalGraphOperations.at(knowledgeGraph).getAllNodes();
		for (Node qNode : allQueryNodes) {
			debugg_cnt_seen_query_nodes++;
			debugg_cnt_seen_knowledge_nodes = 0;
			for (Node knldgNode : allKnowledgeNodes) {
				debugg_cnt_seen_knowledge_nodes++;
				float val = levenshtein.HowMuchTwoNodesAreSimilar(knowledgeGraph, qNode, knldgNode);

				// System.out.println(val);
				if (val > DummyProperties.similarityThreshold) {
					SingleMatchedNode nodeMatch = new SingleMatchedNode();
					nodeMatch.node = knldgNode;
					nodeMatch.value = val; // *
					// TODO: JUST FOR TEST! IT SHOULD BE DELETED
					// ((Integer)
					// knldgNode.getProperty(DummyProperties.uniqueManualIdString)
					// + 1);

					aloneNodeMatchedListsMap.get(qNode).add(nodeMatch);

					// Debugging Purpose
					// System.out.println("possible node matches: QNode:"+
					// qNode.getProperty("name") + ", ModelNode:" +
					// knldgNode.getProperty("name"));

				}
				// if (debugg_cnt_seen_knowledge_nodes % 1000000 == 0) {
				// DummyFunctions.printIfItIsInDebuggedMode(
				// "find possible matches: query node seen so far:" +
				// debugg_cnt_seen_query_nodes
				// + ", knowledge node seen so far: " +
				// debugg_cnt_seen_knowledge_nodes);
				// }
			}
		}

	}

	private void sorting() {
		ResourceIterable<Node> allNodes = GlobalGraphOperations.at(queryGraph).getAllNodes();
		for (Node nodeItem : allNodes) {
			// it should sort in desc order

			DummyFunctions.printIfItIsInDebuggedMode("number of possible matches for this node:" + nodeItem.getId()
					+ " is: " + aloneNodeMatchedListsMap.get(nodeItem).size());

			Collections.sort(aloneNodeMatchedListsMap.get(nodeItem));
			// for (SingleMatchedNode node :
			// aloneNodeMatchedListsMap.get(nodeItem)) {
			// System.out.println(
			// "qNodeId: " + nodeItem.getId() + " Id: " + node.node.getId() + "
			// value: " + node.value);
			// }
			DummyFunctions.printIfItIsInDebuggedMode(
					"maximum similarity:" + aloneNodeMatchedListsMap.get(nodeItem).get(0).value);
			DummyFunctions.printIfItIsInDebuggedMode("minimum similarity:" + aloneNodeMatchedListsMap.get(nodeItem)
					.get(aloneNodeMatchedListsMap.get(nodeItem).size() - 1).value);
		}

		// relationships
		// Iterable<Relationship> allRelationships =
		// GlobalGraphOperations.at(queryGraph).getAllRelationships();
		// for (Relationship relationship : allRelationships) {
		// // it should sort in desc order
		// Collections.sort(aloneEdgeMatchedListsMap.get(relationship));
		// }
	}

	private void createLists() {
		ResourceIterable<Node> allNodes = GlobalGraphOperations.at(queryGraph).getAllNodes();
		for (Node queryNodeItem : allNodes) {
			// System.out.println(queryNodeItem.getId());

			aloneNodeMatchedListsMap.put(queryNodeItem, new ArrayList<SingleMatchedNode>());
			queryNodesSize++;
		}
		DummyFunctions.printIfItIsInDebuggedMode("List created for " + queryNodesSize + " nodes.");
		// relationships
		// Iterable<Relationship> allRelationships =
		// GlobalGraphOperations.at(queryGraph).getAllRelationships();
		// for (Relationship relationship : allRelationships) {
		// aloneEdgeMatchedListsMap.put(relationship, new
		// ArrayList<SingleMatchedEdge>());
		// queryRelationshipSize++;
		// }
	}

	public int getUniqueManualIdByNode(Node nodeItem)// throws
	// Exception
	{
		// int uniqueManualId = (int)
		// nodeItem.getProperty(DummyProperties.uniqueManualIdString);
		// return uniqueManualId;
		return DummyProperties.uniqueManualIdByNodeIdMap.get(nodeItem.getGraphDatabase()).get(nodeItem.getId());

	}

	public static int safeLongToInt(long l) {
		int i = (int) l;
		if ((long) i != l) {
			throw new IllegalArgumentException(l + " cannot be cast to int without changing its value.");
		}
		return i;
	}

	private class SingleMatchedNode implements Comparable<SingleMatchedNode> {
		Node node;
		float value;

		// sorting descending
		@Override
		public int compareTo(SingleMatchedNode other) {
			return Float.compare(other.value, this.value);
		}
	}

	private class SingleMatchedEdge implements Comparable<SingleMatchedEdge> {
		Relationship relationship;
		float value;

		// sorting descending
		@Override
		public int compareTo(SingleMatchedEdge other) {
			return Float.compare(other.value, this.value);
		}
	}

	private class ResultsItem implements Comparable<ResultsItem> {
		String match;
		float value;

		// sorting descending
		@Override
		public int compareTo(ResultsItem other) {
			return Float.compare(other.value, this.value);
		}
	}

	private class MatchableNodesWithValue {
		Map<Node, Float> SimilarityValueOfKnowledgeGraphNode = new HashMap<Node, Float>();
		float sumValue;
	}

}

// public static void main(String [] args) {
// String [] data = { "kitten", "sitting", "saturday", "sunday",
// "rosettacode", "raisethysword" };
// for (int i = 0; i < data.length; i += 2)
// System.out.println("distance(" + data[i] + ", " + data[i+1] + ") = " +
// distance(data[i], data[i+1]));
// }
