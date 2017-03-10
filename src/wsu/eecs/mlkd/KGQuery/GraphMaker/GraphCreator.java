package wsu.eecs.mlkd.KGQuery.GraphMaker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;

public class GraphCreator {

	public static HashSet<Node> nodeCreatedSoFar = new HashSet<Node>();

	public static int maxDegree = 5;
	public static int maxNumberOfLabels = 6;
	public static int numberOfNodes = 100;
	public static String dbPath = "SyntheticG.db";
	public static String wordFilePath = "allWords.txt";
	public static float edgeToNodesRatio = 4;
	public static int numberOfDistinctLabels = 5;

	private static enum RelTypes implements RelationshipType {
		NOTYPE
	}

	public static void main(String[] args) throws Exception {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-numberOfNodes")) {
				numberOfNodes = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-maxDegree")) {
				maxDegree = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-maxNumberOfLabels")) {
				maxNumberOfLabels = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-dbPath")) {
				dbPath = args[++i];
			} else if (args[i].equals("-wordFilePath")) {
				wordFilePath = args[++i];
			} else if (args[i].equals("-edgeToNodesRation")) {
				edgeToNodesRatio = Float.parseFloat(args[++i]);
			} else if (args[i].equals("-numberOfDistinctLabels")) {
				numberOfDistinctLabels = Integer.parseInt(args[++i]);
			}

		}

		if (dbPath == null || numberOfNodes == 0 || maxNumberOfLabels == 0 || maxDegree == 0) {
			throw new Exception("dbPath==null || numberOfNodes==0 ||  maxNumberOfLabels==0 || maxDegree ==0");
		}
		if (((numberOfNodes - 1) / 2) < edgeToNodesRatio) {
			throw new Exception(
					"cannot satisfy the edgeToNodesRatio the complete graph for this number of nodes has less edges than this ratio");
		}
		try {
			createSyntheticG();
		} catch (Exception exc) {
			exc.printStackTrace();
		}

	}

	private static void createSyntheticG() throws Exception {
		try {
			FileUtils.deleteDirectory(new File(dbPath));
		} catch (Exception exc) {
			exc.printStackTrace();
			return;
		}
		LinkedList<MyNode> nodesWaitingForNeighbors = new LinkedList<MyNode>();
		ArrayList<Long> nodeIdsInTheGraph = new ArrayList<Long>();
		ArrayList<String> wordsList = getAllPossibleWords(wordFilePath);
		System.out.println("all word size: " + wordsList.size());
		Random random = new Random();
		int sizeOfWordsList = wordsList.size();
		GraphDatabaseService newRandomGraph = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		registerShutdownHook(newRandomGraph);
		int numberOfRelationships = 0;
		try (Transaction tx = newRandomGraph.beginTx()) {
			int numberOfNodeCreated = 0;
			while (numberOfNodeCreated < numberOfNodes) {
				if (nodesWaitingForNeighbors.isEmpty()) {
					int degreeForThisNode = random.nextInt(maxDegree - 1) + 1;
					int numberOfLabels = random.nextInt(maxNumberOfLabels - 1) + 1;
					Node tempNode = newRandomGraph.createNode();
					tempNode.setProperty("uniqueManualId", tempNode.getId());
					nodeIdsInTheGraph.add(tempNode.getId());
					numberOfNodeCreated++;
					for (int labelIndex = 0; labelIndex < numberOfLabels; labelIndex++) {
						// for having more repeating labels.
						int wordIndex = random.nextInt(Math.min(sizeOfWordsList, numberOfDistinctLabels));
						tempNode.addLabel(DynamicLabel.label(wordsList.get(wordIndex)));
					}

					nodesWaitingForNeighbors.add(new MyNode(tempNode.getId(), degreeForThisNode));
				} else {
					MyNode nodeWaitingForNeighbor = nodesWaitingForNeighbors.getFirst();

					for (int neighborIndex = 0; neighborIndex < nodeWaitingForNeighbor.degree; neighborIndex++) {
						Node neighborNode = newRandomGraph.createNode();
						neighborNode.setProperty("uniqueManualId", neighborNode.getId());
						nodeIdsInTheGraph.add(neighborNode.getId());

						// random relation direction
						if (random.nextInt(2) % 2 == 1) {
							newRandomGraph.getNodeById(nodeWaitingForNeighbor.nodeId).createRelationshipTo(neighborNode,
									RelTypes.NOTYPE);
						} else {
							neighborNode.createRelationshipTo(newRandomGraph.getNodeById(nodeWaitingForNeighbor.nodeId),
									RelTypes.NOTYPE);
						}
						numberOfRelationships++;
						numberOfNodeCreated++;
						int degreeForThisNode = random.nextInt(maxDegree - 1) + 1;
						int numberOfLabels = random.nextInt(maxNumberOfLabels - 1) + 1;
						for (int labelIndex = 0; labelIndex < numberOfLabels; labelIndex++) {
							int wordIndex = random.nextInt(Math.min(sizeOfWordsList, numberOfDistinctLabels));
							neighborNode.addLabel(DynamicLabel.label(wordsList.get(wordIndex)));
						}
						// because it's already have a relationship.
						// (degreeForThisNode - 1)
						nodesWaitingForNeighbors.add(new MyNode(neighborNode.getId(), degreeForThisNode - 1));
					}
					nodesWaitingForNeighbors.remove(nodeWaitingForNeighbor);
				}
			}

			// for changing this tree like graph to cyclic graph
			int currentNumberOfNodes = nodeIdsInTheGraph.size();
			while ((numberOfRelationships / currentNumberOfNodes) < edgeToNodesRatio) {
				long firstNodeId = random.nextInt(currentNumberOfNodes);
				long secondNodeId = random.nextInt(currentNumberOfNodes);

				if (!existsRelationship(newRandomGraph, firstNodeId, secondNodeId)) {
					newRandomGraph.getNodeById(firstNodeId)
							.createRelationshipTo(newRandomGraph.getNodeById(secondNodeId), RelTypes.NOTYPE);
					numberOfRelationships++;
				}
			}

			System.out.println("program finished properly!");
			tx.success();
		} catch (Exception exc) {
			exc.printStackTrace();
			newRandomGraph.shutdown();
		}
		newRandomGraph.shutdown();
	}

	private static boolean existsRelationship(GraphDatabaseService randomGraph, long firstNodeId, long secondNodeId) {
		Node firstNode = randomGraph.getNodeById(firstNodeId);
		for (Relationship rel : firstNode.getRelationships()) {
			if (rel.getOtherNode(firstNode).getId() == secondNodeId) {
				return true;
			}
		}
		return false;
	}

	public static ArrayList<String> getAllPossibleWords(String filePath) throws Exception {
		ArrayList<String> wordsList = new ArrayList<String>();
		HashSet<String> wordsSeenSoFar = new HashSet<String>();
		FileReader fileReader = new FileReader(filePath);

		BufferedReader bufferedReader = new BufferedReader(fileReader);

		String line = null;
		while ((line = bufferedReader.readLine()) != null) {
			String[] wordInLine = line.split("\t");
			if (!wordsSeenSoFar.contains(wordInLine[1]) && wordInLine[1].length() > 4) {
				wordsList.add(wordInLine[1]);
				wordsSeenSoFar.add(wordInLine[1]);
			}
		}
		bufferedReader.close();
		return wordsList;
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
}

class MyNode {
	public long nodeId;
	public int degree;

	public MyNode(long nodeId, int degree) {
		this.nodeId = nodeId;
		this.degree = degree;
	}

}
