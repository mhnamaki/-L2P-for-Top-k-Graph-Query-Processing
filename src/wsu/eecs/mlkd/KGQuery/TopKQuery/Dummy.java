package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.jgrapht.alg.DirectedNeighborIndex;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

public class Dummy {
	public static class DummyProperties {
		public static final String uniqueManualIdString = "uniqueManualId";
		public static boolean debuggMode = false;
		public static boolean semanticChecking = true;
		public static HashMap<GraphDatabaseService, HashMap<Integer, Long>> nodeIdByUniqueManualIdMap = new HashMap<GraphDatabaseService, HashMap<Integer, Long>>();
		public static HashMap<GraphDatabaseService, HashMap<Long, Integer>> uniqueManualIdByNodeIdMap = new HashMap<GraphDatabaseService, HashMap<Long, Integer>>();
		public static Float similarityThreshold = null;
		public static Integer patternSize = null;
		public static Integer knowledgeSize = null;
		public static int numberOfPrefixChars = 12;
		public static int maxNumberOfSQ = 5;
		public static final float DEFAULT_VALUE_FOR_MISSING = 9999F;

	}

	public static class DummyFunctions {
		public static int getSQIndexByCalcNodeIndexAndNumberOfStars(int sqCalcNodeIndex, int numberOfStars)
				throws Exception {
			// not zero-based.
			switch (numberOfStars) {
			case 2:
				return sqCalcNodeIndex;
			case 3:
				if (sqCalcNodeIndex == 1) {
					return sqCalcNodeIndex;
				} else if (sqCalcNodeIndex == 3) {
					return 2;
				} else if (sqCalcNodeIndex == 4) {
					return 3;
				}
				break;
			case 4:
				if (sqCalcNodeIndex == 1) {
					return sqCalcNodeIndex;
				} else if (sqCalcNodeIndex == 3) {
					return 2;
				} else if (sqCalcNodeIndex == 5) {
					return 3;
				} else if (sqCalcNodeIndex == 6) {
					return 4;
				}
				break;
			case 5:
				if (sqCalcNodeIndex == 1) {
					return sqCalcNodeIndex;
				} else if (sqCalcNodeIndex == 3) {
					return 2;
				} else if (sqCalcNodeIndex == 5) {
					return 3;
				} else if (sqCalcNodeIndex == 7) {
					return 4;
				} else if (sqCalcNodeIndex == 8) {
					return 5;
				}
				break;
			}
			throw new Exception("not defined calc node!");
		}

		// dummy functions
		public static void populatePatternAndModelSize(GraphDatabaseService queryGraph,
				GraphDatabaseService knowledgeGraph) {
			DummyProperties.knowledgeSize = 0;
			DummyProperties.patternSize = 0;
			GlobalGraphOperations globalOperation1 = GlobalGraphOperations.at(knowledgeGraph);
			for (Node n : globalOperation1.getAllNodes()) {
				DummyProperties.knowledgeSize++;
			}
			GlobalGraphOperations globalOperation2 = GlobalGraphOperations.at(queryGraph);
			for (Node n : globalOperation2.getAllNodes()) {
				DummyProperties.patternSize++;
			}

		}

		public static void populateIdUniqueManualIdMaps(GraphDatabaseService graph) {
			HashMap<Integer, Long> nodeIdByUniqueManualIdMapVal = new HashMap<Integer, Long>();
			HashMap<Long, Integer> uniqueManualIdByNodeIdMapVal = new HashMap<Long, Integer>();

			ResourceIterable<Node> allNodes = GlobalGraphOperations.at(graph).getAllNodes();
			for (Node nodeItem : allNodes) {
				Integer umId = Integer.parseInt(nodeItem.getProperty(DummyProperties.uniqueManualIdString).toString());
				nodeIdByUniqueManualIdMapVal.put(umId, nodeItem.getId());
				uniqueManualIdByNodeIdMapVal.put(nodeItem.getId(), umId);
			}
			DummyProperties.nodeIdByUniqueManualIdMap.put(graph, nodeIdByUniqueManualIdMapVal);
			DummyProperties.uniqueManualIdByNodeIdMap.put(graph, uniqueManualIdByNodeIdMapVal);
		}

		public static Node getNodeByProperty(GraphDatabaseService graph, int uniqueManualId)// throws
		// Exception
		{
			return graph.getNodeById(DummyProperties.nodeIdByUniqueManualIdMap.get(graph).get(uniqueManualId));
		}

		public static void printIfItIsInDebuggedMode(Object text) {
			if (DummyProperties.debuggMode)
				System.out.println(text);
		}

		public static void createUniqueIdForEachNode(
				GraphDatabaseService queryGraph/*
												 * , GraphDatabaseService
												 * knowledgeGraph
												 */) {
			ResourceIterable<Node> allNodes = GlobalGraphOperations.at(queryGraph).getAllNodes();
			int uniqueManualId = 0;
			for (Node nodeItem : allNodes) {
				nodeItem.setProperty(DummyProperties.uniqueManualIdString, uniqueManualId);
				// System.out.println(
				// nodeItem.getProperty("name") + " ->Id: " + uniqueManualId + "
				// OrigID:" + nodeItem.getId());
				uniqueManualId++;

			}
			// uniqueManualId = 0;
			// allNodes =
			// GlobalGraphOperations.at(knowledgeGraph).getAllNodes();
			// for (Node nodeItem : allNodes) {
			// nodeItem.setProperty(DummyProperties.uniqueManualIdString,
			// uniqueManualId);
			// System.out.println(
			// " ->uniqueManualId: " + uniqueManualId + " OrigID:" +
			// nodeItem.getId());
			// uniqueManualId++;
			//
			// }
		}

		public static String collectionToCommaDelimitedString(ArrayList<Double> input) {
			StringBuilder sb = new StringBuilder();
			for (Double str : input) {
				sb.append(str.toString()).append(",");
			}

			return sb.toString();
		}

		public static DirectedNeighborIndex<Node, Relationship> createDirectedNeighborIndex(
				GraphDatabaseService graphDatabaseService) {
			try (Transaction tx1 = graphDatabaseService.beginTx()) {
				org.jgrapht.DirectedGraph<Node, Relationship> directedGraph = new DefaultDirectedGraph<Node, Relationship>(
						Relationship.class);

				ResourceIterable<Node> allNodes = GlobalGraphOperations.at(graphDatabaseService).getAllNodes();
				for (Node queryNodeItem : allNodes) {
					directedGraph.addVertex(queryNodeItem);
				}

				Iterable<Relationship> allRelationship = GlobalGraphOperations.at(graphDatabaseService)
						.getAllRelationships();
				for (Relationship relItem : allRelationship) {
					directedGraph.addEdge(relItem.getStartNode(), relItem.getEndNode(), relItem);
				}
				tx1.success();
				return new DirectedNeighborIndex<Node, Relationship>(directedGraph);

			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}

		}

		public static Double computeNonOutlierAverage(ArrayList<Double> differenceTimes, int numberOfSameExperiment) {
			if (differenceTimes.size() == 0)
				return 0.0;

			int remaining = numberOfSameExperiment;
			if (numberOfSameExperiment == 2 || numberOfSameExperiment == 3) {
				remaining -= 1;
			} else if (numberOfSameExperiment == 5) {
				remaining = 3;
			} else if (numberOfSameExperiment == 7) {
				remaining = 4;
			} else if (numberOfSameExperiment > 7) {
				remaining = numberOfSameExperiment - 5;
			}

			Double totalSum = 0d;
			Collections.sort(differenceTimes);
			for (int r = 0; r < remaining; r++) {
				totalSum += differenceTimes.get(r);
			}
			return (totalSum / remaining);

		}

		public static double getRank(Collection<GraphResult> anyTimeResults) {
			double rank = 0d;

			// label sim + #ofNodes + #rels
			for (GraphResult gr : anyTimeResults) {
				rank += gr.anyTimeItemValue;
			}
			return rank;
		}

		public static void whatsGoingon(AnyTimeStarFramework sf) {
			for (Integer starId : sf.calcTreeStarQueriesNodeMap.keySet()) {
				System.out.print(starId + ":" + sf.calcTreeStarQueriesNodeMap.get(starId).data.depthOfDigging + ", ");
			}
		}
	}
}
