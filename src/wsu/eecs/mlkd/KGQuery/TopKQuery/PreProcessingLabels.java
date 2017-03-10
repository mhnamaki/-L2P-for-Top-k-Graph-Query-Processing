package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

public class PreProcessingLabels {

	private static String MODELGRAPH_DB_PATH = "";
	public static String GDirectory = "";
	public static String GName = "";
	public static HashMap<String, HashSet<Long>> nodeLabelsIndex = new HashMap<String, HashSet<Long>>();

	public static HashMap<String, HashSet<Long>> getPrefixLabelsIndex(GraphDatabaseService knowledgeGraph,
			int numberOfPrefixChars) {
		try (Transaction tx1 = knowledgeGraph.beginTx()) {
			ResourceIterable<Node> allKnowledgeNodes = GlobalGraphOperations.at(knowledgeGraph).getAllNodes();
			int numberOfNodeWithoutLabel = 0;
			int numberOfNodeWithLabel = 0;
			for (Node knowledgeNode : allKnowledgeNodes) {
				// assumption: just if it doesn't have labels, we should add
				// properties.
				int numberOfLabels = 0;
				Iterable<Label> knldglabelIterator = knowledgeNode.getLabels();
				for (Label label : knldglabelIterator) {
					numberOfLabels++;
					if (label.toString().length() >= numberOfPrefixChars) {
						String abc = label.toString().substring(0, numberOfPrefixChars);
						if (nodeLabelsIndex.containsKey(abc)) {
							nodeLabelsIndex.get(abc).add(knowledgeNode.getId());
						} else {
							HashSet<Long> arr = new HashSet<Long>();
							arr.add(knowledgeNode.getId());
							nodeLabelsIndex.put(abc, arr);
						}
					}
				}
				if (numberOfLabels <= 0) {
					numberOfNodeWithoutLabel++;
					for (String key : knowledgeNode.getPropertyKeys()) {
						String value = knowledgeNode.getProperty(key).toString();
						if (value.length() >= numberOfPrefixChars) {
							String abc = value.substring(0, numberOfPrefixChars);
							if (nodeLabelsIndex.containsKey(abc)) {
								nodeLabelsIndex.get(abc).add(knowledgeNode.getId());
							} else {
								HashSet<Long> arr = new HashSet<Long>();
								arr.add(knowledgeNode.getId());
								nodeLabelsIndex.put(abc, arr);
							}
						}
					}
				} else {
					numberOfNodeWithLabel++;
				}
			}
			tx1.success();
			return nodeLabelsIndex;
			
		} catch (Exception exc) {
			System.out.println("queryGraph Transaction failed");
			exc.printStackTrace();
			return null;
		}

	}

//	public static void main(String[] args) {
//
//		for (int i = 0; i < args.length; i++) {
//			if (args[i].equals("-GName")) {
//				GName = args[++i];
//
//			} else if (args[i].equals("-GDirectory")) {
//				GDirectory = args[++i];
//			}
//		}
//		if (!GDirectory.endsWith("/")) {
//			GDirectory += "/";
//		}
//		MODELGRAPH_DB_PATH = GDirectory + GName;
//		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(MODELGRAPH_DB_PATH);
//		try (Transaction tx1 = knowledgeGraph.beginTx()) {
//
//			getPrefixLabelsIndex(knowledgeGraph, numberOfPrefixChars);
//			// System.out.println("numberOfNodeWithLabel: " +
//			// numberOfNodeWithLabel);
//			// System.out.println("numberOfNodeWithoutLabel: " +
//			// numberOfNodeWithoutLabel);
//			System.out.println("size of the different keysets: " + nodeLabelsIndex.keySet().size());
//			System.out.println("");
//			for (String key : nodeLabelsIndex.keySet()) {
//				System.out.println(key + "; " + nodeLabelsIndex.get(key).size());
//			}
//
//			tx1.success();
//		} catch (Exception exc) {
//			System.out.println("queryGraph Transaction failed");
//			exc.printStackTrace();
//		}
//		knowledgeGraph.shutdown();
//	}

}
