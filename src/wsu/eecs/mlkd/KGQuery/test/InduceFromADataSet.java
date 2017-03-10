package wsu.eecs.mlkd.KGQuery.test;

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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.TreeMap;

import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.Collect;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.tooling.GlobalGraphOperations;

import java_cup.sym;
import jdsl.core.ref.ComparableComparator;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;

public class InduceFromADataSet {

	// public static String dbPath =
	// "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.3.1/data/dbpedia_infobox_properties_en.db";
	public static String dbPath = "/Users/mnamaki/Desktop/freebase.db";
	public static int numberOfPrefixChars = 4;

	public static void main(String[] args) throws Exception {

		HashMap<Long, Integer> nodeFrequency = new HashMap<Long, Integer>();

		FileInputStream fis = new FileInputStream("outputQueryFreebase.txt");

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		int lineCnt = 0;
		while ((line = br.readLine()) != null) {
			if (line.contains("->")) {
				String[] queryLine = line.split("->");
				String[] nodeIds = queryLine[0].trim().split("-");
				for (int i = 0; i < nodeIds.length; i++) {
					if (!nodeIds[i].trim().equals("")) {
						Long nodeId = Long.parseLong(nodeIds[i].trim());
						if (!nodeFrequency.containsKey(nodeId)) {
							nodeFrequency.put(nodeId, 1);
						} else {
							nodeFrequency.put(nodeId, nodeFrequency.get(nodeId) + 1);
						}
					}
				}
				// System.out.println("lineCnt: " + lineCnt++);
			}

		}

		br.close();

		System.out.println("nodeFrequency size: " + nodeFrequency.size());

		Map<Long, Integer> nodeFreqMapSorted = sortByValue(nodeFrequency);

		System.out.println("after sorting ");

		File foutNodes = new File("nodes.txt");
		FileOutputStream fosNodes = new FileOutputStream(foutNodes);

		BufferedWriter bwNodes = new BufferedWriter(new OutputStreamWriter(fosNodes));
		bwNodes.write("unique:ID,:Label");
		bwNodes.newLine();

		File foutRels = new File("relationships.txt");
		FileOutputStream fosRels = new FileOutputStream(foutRels);

		BufferedWriter bwRels = new BufferedWriter(new OutputStreamWriter(fosRels));
		bwRels.write(":START_ID,:END_ID,:TYPE");
		bwRels.newLine();

		HashSet<Long> insertedNodes = new HashSet<Long>();
		HashSet<String> insertedRels = new HashSet<String>();

		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath)
				.newGraphDatabase();

		registerShutdownHook(knowledgeGraph);
		System.out.println("after knowledge graph initialization");

		try (Transaction tx1 = knowledgeGraph.beginTx()) {

			for (Long nodeId : nodeFreqMapSorted.keySet()) {

				if ((insertedNodes.size() % 1000) == 0) {
					System.out.println("insertedNodes.size(): " + insertedNodes.size());
				}
				Node thisNode = knowledgeGraph.getNodeById(nodeId);

				if (!insertedNodes.contains(thisNode.getId())) {
					insertNodeInFile(bwNodes, thisNode, insertedNodes);
				}

				for (Relationship rel : thisNode.getRelationships()) {
					Node otherNode = rel.getOtherNode(thisNode);
					if (!insertedNodes.contains(otherNode.getId())) {
						if (nodeFrequency.containsKey(otherNode.getId())) {
							insertNodeInFile(bwNodes, otherNode, insertedNodes);
						}
					}

					if (nodeFrequency.containsKey(otherNode.getId())) {
						String relStr = rel.getStartNode().getId() + "," + rel.getEndNode().getId() + ", NO_TYPE";

						if (!insertedRels.contains(relStr)) {
							bwRels.write(relStr);
							bwRels.newLine();
							insertedRels.add(relStr);
						}
					}
				}

			}

			int degree = 1000;
			ArrayList<Long> nodeIdsForRandomness = new ArrayList<Long>();
			for (Long nodeId : nodeFreqMapSorted.keySet()) {
				Node thisNode = knowledgeGraph.getNodeById(nodeId);
				if (thisNode.getDegree() < degree) {
					nodeIdsForRandomness.add(thisNode.getId());
				}
			}
			Random rnd = new Random();
			while (insertedNodes.size() < 4000000) {
				if ((insertedNodes.size() % 10000) == 0) {
					System.out.println("insertedNodes.size(): " + insertedNodes.size());
				}

				int randIndex = rnd.nextInt(nodeIdsForRandomness.size());
				Node thisNode = knowledgeGraph.getNodeById(nodeIdsForRandomness.get(randIndex));

				nodeIdsForRandomness.remove(randIndex);

				for (Relationship rel : thisNode.getRelationships()) {
					Node otherNode = rel.getOtherNode(thisNode);

					if (!insertedNodes.contains(otherNode.getId())) {
						insertNodeInFile(bwNodes, otherNode, insertedNodes);
					}

					String relStr = rel.getStartNode().getId() + "," + rel.getEndNode().getId() + ", NO_TYPE";

					if (!insertedRels.contains(relStr)) {
						bwRels.write(relStr);
						bwRels.newLine();
						insertedRels.add(relStr);
					}

					if (otherNode.getDegree() < degree) {
						nodeIdsForRandomness.add(otherNode.getId());
					}
				}
			}

			bwRels.close();
			bwNodes.close();
			tx1.success();
		} catch (

		Exception exc) {
			System.out.println("queryGraph Transaction failed");
			exc.printStackTrace();
		} finally {
			knowledgeGraph.shutdown();
		}

	}

	private static void insertNodeInFile(BufferedWriter bwNodes, Node theNode, HashSet<Long> insertedNodes)
			throws Exception {

		String labels = getLabelsOfNode(theNode);
		bwNodes.write(theNode.getId() + "," + labels);
		bwNodes.newLine();
		insertedNodes.add(theNode.getId());

	}

	private static String getLabelsOfNode(Node thisNode) {
		String labels = "";
		for (Label lbl : thisNode.getLabels()) {
			String tempLbl = lbl.toString();
			if (tempLbl.length() < 13) {
				tempLbl = "lbl_" + tempLbl;
			}

			labels += tempLbl + ";";
		}
		return labels;
	}

	static Map sortByValue(Map map) {
		List list = new LinkedList(map.entrySet());
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue());
			}
		});

		Map result = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
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
