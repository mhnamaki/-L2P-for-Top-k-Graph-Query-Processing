package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;

import org.jgrapht.alg.*;
import org.jgrapht.graph.*;
import org.jgrapht.*;

public class QueryDecomposition {
	// public GraphDatabaseService queryGraph;
	// public GraphDatabaseService knowledgeGraph;
	public Levenshtein levenshtein;
	public HashMap<Long, Integer> nodeEstimationMap;

	public QueryDecomposition(Levenshtein levenshtein) {
		// this.queryGraph = queryGraph;
		// this.knowledgeGraph = knowledgeGraph;
		this.levenshtein = levenshtein;
		nodeEstimationMap = new HashMap<Long, Integer>();
	}

	private ArrayList<NodePotentialMemberInIsomorphicEstimation> getPotentialEstimationOfIsomorphic(
			GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph,
			NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) {

		ArrayList<NodePotentialMemberInIsomorphicEstimation> nodeEstimation = new ArrayList<NodePotentialMemberInIsomorphicEstimation>();

		if (!DummyProperties.semanticChecking) {
			// Iterator<Long> allQueryNodesIterator =
			// neighborIndexingInstance.queryNodeIdSet.iterator();
			for (Long queryNodeId : neighborIndexingInstance.queryNodeIdSet) {
				// while (allQueryNodesIterator.hasNext()) {
				// Long queryNodeId = allQueryNodesIterator.next();
				Node queryNode = queryGraph.getNodeById(queryNodeId);
				NodePotentialMemberInIsomorphicEstimation npmiie = new NodePotentialMemberInIsomorphicEstimation(
						queryNode, 1);
				nodeEstimation.add(npmiie);
			}
			return nodeEstimation;
		}

		// Iterator<Long> allQueryNodesIterator =
		// neighborIndexingInstance.queryNodeIdSet.iterator();
		// ResourceIterable<Node> allKnowledgeNodes =
		// GlobalGraphOperations.at(knowledgeGraph).getAllNodes();

		for (Long queryNodeId : neighborIndexingInstance.queryNodeIdSet) {
			// while (allQueryNodesIterator.hasNext()) {
			// Long queryNodeId = allQueryNodesIterator.next();
			Node queryNode = queryGraph.getNodeById(queryNodeId); // can
																	// optimize
																	// if change
																	// howmuchtwonode
																	// similarity
																	// based on
																	// string
																	// and node
																	// not based
																	// on node
																	// and node
			int nodeEstimationCnt = 0;
			int qNodeInDegree = neighborIndexingInstance.queryInDegreeMap.get(queryNodeId);
			int qNodeOutDegree = neighborIndexingInstance.queryOutDegreeMap.get(queryNodeId);

			HashSet<Long> possibleNodeIdSet = levenshtein.nodeLabelsIndex.get(neighborIndexingInstance.queryNodeLabelMap
					.get(queryNodeId).substring(0, DummyProperties.numberOfPrefixChars));
			Iterator<Long> possibleNodeIdIterator = possibleNodeIdSet.iterator();

			while (possibleNodeIdIterator.hasNext()) {
				Node knowledgeNode = knowledgeGraph.getNodeById(possibleNodeIdIterator.next());
				Long knowledgeNodeId = knowledgeNode.getId();
				// Step1: Degree condition checking
				if (qNodeInDegree <= neighborIndexingInstance.graphInDegreeMap.get(knowledgeNodeId)
						&& qNodeOutDegree <= neighborIndexingInstance.graphOutDegreeMap.get(knowledgeNodeId)) {

					// Step2: Label similarity between primal nodes
					if (levenshtein.HowMuchTwoNodesAreSimilar(knowledgeGraph, queryNode, knowledgeNode,
							neighborIndexingInstance, cacheServer) > DummyProperties.similarityThreshold) {
						nodeEstimationCnt++;
						// Step3: Incoming feasibility
						// boolean incomingCorrespondingNodeIsFound = false;
						// Iterator<Long> queryInNeighborIndicesIterator =
						// NeighborIndexing.queryInNeighborIndicesMap
						// .get(queryNodeId).iterator();
						// while (queryInNeighborIndicesIterator.hasNext()) {
						// Long neighborQNodeId =
						// queryInNeighborIndicesIterator.next();
						// Node neighborQNode =
						// queryGraph.getNodeById(neighborQNodeId);
						//
						// incomingCorrespondingNodeIsFound = false;
						//
						// Iterator<Long> graphInNeighborIndicesIterator =
						// NeighborIndexing.graphInNeighborIndicesMap
						// .get(knowledgeNodeId).iterator();
						// while (graphInNeighborIndicesIterator.hasNext()) {
						// Long neighborGNodeId =
						// graphInNeighborIndicesIterator.next();
						// Node neighborGNode =
						// knowledgeGraph.getNodeById(neighborGNodeId);
						//
						// if
						// (levenshtein.HowMuchTwoNodesAreSimilar(knowledgeGraph,
						// neighborQNode,
						// neighborGNode) > DummyProperties.similarityThreshold)
						// {
						//
						// incomingCorrespondingNodeIsFound = true;
						// // if we can find one similar we can come
						// // out of the loop (performance)
						// break;
						// }
						// }
						// // if for this neighborGNode we couldn't find any
						// // similar neighbor node in knowledge we cannot
						// // consider primal node as a potential.
						// if (!incomingCorrespondingNodeIsFound) {
						// break;
						// }
						//
						// }
						//
						// if
						// (NeighborIndexing.queryInNeighborIndicesMap.get(queryNodeId).isEmpty())
						// incomingCorrespondingNodeIsFound = true;
						//
						// // Step4: OutGoing feasibility
						// if (incomingCorrespondingNodeIsFound) {
						// boolean outgoingCorrespondingNodeIsFound = false;
						//
						// Iterator<Long> queryOutNeighborIndicesIterator =
						// NeighborIndexing.queryOutNeighborIndicesMap
						// .get(queryNodeId).iterator();
						// while (queryOutNeighborIndicesIterator.hasNext()) {
						// Long neighborQNodeId =
						// queryOutNeighborIndicesIterator.next();
						// Node neighborQNode =
						// queryGraph.getNodeById(neighborQNodeId);
						// outgoingCorrespondingNodeIsFound = false;
						//
						// Iterator<Long> graphOutNeighborIndicesIterator =
						// NeighborIndexing.graphOutNeighborIndicesMap
						// .get(knowledgeNodeId).iterator();
						// while (graphOutNeighborIndicesIterator.hasNext()) {
						// Long neighborGNodeId =
						// graphOutNeighborIndicesIterator.next();
						// Node neighborGNode =
						// knowledgeGraph.getNodeById(neighborGNodeId);
						//
						// if
						// (levenshtein.HowMuchTwoNodesAreSimilar(knowledgeGraph,
						// neighborQNode,
						// neighborGNode) > 0) {
						// outgoingCorrespondingNodeIsFound = true;
						// break;
						// }
						// }
						// if (!outgoingCorrespondingNodeIsFound) {
						// break;
						// }
						//
						// }
						//
						// if
						// (NeighborIndexing.queryOutNeighborIndicesMap.get(queryNodeId).isEmpty())
						// outgoingCorrespondingNodeIsFound = true;
						//
						// // if both incoming and outgoing conditions are
						// // true, we can count primal node as a potential
						// // one!
						//
						// if (incomingCorrespondingNodeIsFound &&
						// outgoingCorrespondingNodeIsFound) {
						// nodeEstimationCnt++;
						// }
						// }

					}
				}
			}
			NodePotentialMemberInIsomorphicEstimation npmiie = new NodePotentialMemberInIsomorphicEstimation(queryNode,
					nodeEstimationCnt);
			nodeEstimation.add(npmiie);
		}

//		for (NodePotentialMemberInIsomorphicEstimation ne : nodeEstimation) {
//			nodeEstimationMap.put(ne.node.getId(), ne.value);
//			System.out.println(ne.node.getId() + ", " + ne.value);
//		}

		return nodeEstimation;
	}

	/// Note: it cannot understand relationship properties

	public ArrayList<StarGraphQuery> getStarQueriesFromGraphQuery(GraphDatabaseService queryGraph,
			ArrayList<NodePotentialMemberInIsomorphicEstimation> nodeEstimationList) {
		long start_time = System.nanoTime();
		HashMap<Node, Float> weightOfVertexes = new HashMap<Node, Float>();
		HashMap<Node, Integer> degreeOfVertexes = new HashMap<Node, Integer>();
		ArrayList<NodeWeightDivideByDegree> weightOfVertexesDivideByDegreeOfVertexes = new ArrayList<NodeWeightDivideByDegree>();

		for (int index = 0; index < nodeEstimationList.size(); index++) {
			NodePotentialMemberInIsomorphicEstimation nodeWithEstimation = nodeEstimationList.get(index);
			weightOfVertexes.put(nodeWithEstimation.node, (float) nodeWithEstimation.value);
			degreeOfVertexes.put(nodeWithEstimation.node, (nodeWithEstimation.node.getDegree()));
			NodeWeightDivideByDegree nodeWeightDivideByDegree = new NodeWeightDivideByDegree();
			nodeWeightDivideByDegree.node = nodeWithEstimation.node;
			nodeWeightDivideByDegree.value = ((float) nodeWithEstimation.value
					/ (float) nodeWithEstimation.node.getDegree());
			weightOfVertexesDivideByDegreeOfVertexes.add(nodeWeightDivideByDegree);
		}

		// packing function (Ye)
		HashMap<Relationship, Float> packingFunctionOfEdgesYe = new HashMap<Relationship, Float>(); // Ye
		Iterable<Relationship> allRelationships = GlobalGraphOperations.at(queryGraph).getAllRelationships();
		Set<Relationship> queryRelationships = new HashSet<Relationship>();
		for (Relationship rel : allRelationships) {
			packingFunctionOfEdgesYe.put(rel, (float) 0);
			queryRelationships.add(rel);
		}
		// output?!
		ArrayList<StarGraphQuery> decomposedGraphQueries = new ArrayList<StarGraphQuery>();

		Collections.sort(weightOfVertexesDivideByDegreeOfVertexes);

		while (!queryRelationships.isEmpty()) { // while E <> Empty
			NodeWeightDivideByDegree currentMinimizedNodeWeightDivideByDegree = weightOfVertexesDivideByDegreeOfVertexes
					.get(0);
			// getting minimized vertex in w/d

			org.jgrapht.DirectedGraph<Node, Relationship> directedGraph = new DefaultDirectedGraph<Node, Relationship>(
					Relationship.class);

			Iterator<Relationship> queryRelationshipsIterator = queryRelationships.iterator();
			while (queryRelationshipsIterator.hasNext()) {
				Relationship rel = queryRelationshipsIterator.next();
				Node endNode = rel.getEndNode(); // v
				Node startNode = rel.getStartNode();
				Node currentNodeV = null;
				Node adjacentNodeU = null;

				if (endNode.getId() == currentMinimizedNodeWeightDivideByDegree.node.getId()) {

					currentNodeV = endNode;
					adjacentNodeU = startNode;

				} else if (startNode.getId() == currentMinimizedNodeWeightDivideByDegree.node.getId()) {

					currentNodeV = startNode;
					adjacentNodeU = endNode;

				}

				if (currentNodeV != null && adjacentNodeU != null) {

					NodeWeightDivideByDegree nodeWeightDivideByDegreeForAdjecentNode = getNodeWeightDivideByDegreeFor(
							weightOfVertexesDivideByDegreeOfVertexes, currentNodeV);

					weightOfVertexes.put(adjacentNodeU,
							weightOfVertexes.get(adjacentNodeU) - nodeWeightDivideByDegreeForAdjecentNode.value);

					degreeOfVertexes.put(adjacentNodeU, degreeOfVertexes.get(adjacentNodeU) - 1);

					packingFunctionOfEdgesYe.put(rel, nodeWeightDivideByDegreeForAdjecentNode.value);

					// add these edges as a new directed/undirected graph
					directedGraph.addVertex(currentNodeV);
					directedGraph.addVertex(adjacentNodeU);
					// maintaining the edge direction
					directedGraph.addEdge(startNode, endNode, rel);

					queryRelationshipsIterator.remove();
				}
			}

			if (directedGraph.edgeSet().iterator().hasNext()) {
				decomposedGraphQueries
						.add(new StarGraphQuery(currentMinimizedNodeWeightDivideByDegree.node, directedGraph));
			}

			// remove current node.
			weightOfVertexesDivideByDegreeOfVertexes = removeObjectFromArrayList(
					weightOfVertexesDivideByDegreeOfVertexes, currentMinimizedNodeWeightDivideByDegree.node);

			weightOfVertexes.put(currentMinimizedNodeWeightDivideByDegree.node, (float) 0);
		}

		long end_time = System.nanoTime();
		double difference = (end_time - start_time) / 1e6;
		DummyFunctions.printIfItIsInDebuggedMode("getPotentialEstimationOfIsomorphic is finished " + difference
				+ " miliseconds! starQueries number is: " + decomposedGraphQueries.size());

		return decomposedGraphQueries;

	}

	// Clarkson's Greedy Algorithm:
	public ArrayList<StarGraphQuery> getStarQueriesFromGraphQueryBasedOnKnowledgeGraphClarksonGreedyAlgorithm(
			GraphDatabaseService queryGraph, GraphDatabaseService knowledgeGraph,
			NeighborIndexing neighborIndexingInstance, CacheServer cacheServer) {

		// DummyFunctions.createUniqueIdForEachNode(queryGraph, knowledgeGraph);

		long start_time = System.nanoTime();
		ArrayList<NodePotentialMemberInIsomorphicEstimation> nodeEstimationList = getPotentialEstimationOfIsomorphic(
				queryGraph, knowledgeGraph, neighborIndexingInstance, cacheServer);
		long end_time = System.nanoTime();
		double difference = (end_time - start_time) / 1e6;
		DummyFunctions.printIfItIsInDebuggedMode("nodeEstimationList filled in " + difference + " miliseconds! ");
		return getStarQueriesFromGraphQuery(queryGraph, nodeEstimationList);

	}

	private ArrayList<NodeWeightDivideByDegree> removeObjectFromArrayList(
			ArrayList<NodeWeightDivideByDegree> weightOfVertexesDivideByDegreeOfVertexes, Node node) {

		for (int j = weightOfVertexesDivideByDegreeOfVertexes.size() - 1; j >= 0; j--) {
			if (weightOfVertexesDivideByDegreeOfVertexes.get(j).node.getId() == node.getId()) {
				weightOfVertexesDivideByDegreeOfVertexes.remove(weightOfVertexesDivideByDegreeOfVertexes.get(j));
			}

		}
		return weightOfVertexesDivideByDegreeOfVertexes;
	}

	private NodeWeightDivideByDegree getNodeWeightDivideByDegreeFor(
			ArrayList<NodeWeightDivideByDegree> weightOfVertexesDivideByDegreeOfVertexes, Node endNodeV) {
		for (int i = 0; i < weightOfVertexesDivideByDegreeOfVertexes.size(); i++) {
			if (endNodeV.getId() == weightOfVertexesDivideByDegreeOfVertexes.get(i).node.getId()) {
				return weightOfVertexesDivideByDegreeOfVertexes.get(i);
			}
		}
		return null;
	}

}

class NodeWeightDivideByDegree implements Comparable<NodeWeightDivideByDegree> {
	Node node;
	float value;

	// sorting ascending
	@Override
	public int compareTo(NodeWeightDivideByDegree other) {
		return Float.compare(this.value, other.value);
	}
}
