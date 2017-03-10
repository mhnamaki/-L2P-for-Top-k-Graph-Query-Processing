package wsu.eecs.mlkd.KGQuery.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

public class ExtractNonStarsFromQueries {

	public static String gDirectory = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.3.1/data/";
	public static String gName = "yago.db";

	public static String qPath = "convertedYago_";
	public static int querySizeFrom = 5;
	public static int querySizeTo = 5;
	public static QueryGenerator queryGenerator;

	public static void main(String[] args) throws Exception {

		// GraphDatabaseService knowledgeGraph = new
		// GraphDatabaseFactory().newEmbeddedDatabase(gPath);
		// registerShutdownHook(knowledgeGraph);

		queryGenerator = new QueryGenerator(gDirectory + gName);
		for (int querySize = querySizeFrom; querySize <= querySizeTo; querySize++) {
			List<QueryFromFile> queriesFromFile = queryGenerator.getQueryFromFile(qPath + querySize + ".txt");
			int queryIndex = 0;
			for (QueryFromFile queryFromFile : queriesFromFile) {
				queryIndex++;
				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph("/Users/mnamaki/Documents/workspace/wsu.eecs.mlkd.KGQuery/query.db", queryFromFile);
				registerShutdownHook(smallGraph);
				try (Transaction tx1 = smallGraph.beginTx()) {
					ResourceIterable<Node> nodes = GlobalGraphOperations.at(smallGraph).getAllNodes();
					boolean seenMoreThanOneDegreeNode = false;
					for (Node node : nodes) {
						if (node.getDegree() > 1 && seenMoreThanOneDegreeNode) {
							// it's a non-star query
							Iterable<Relationship> relationships = GlobalGraphOperations.at(smallGraph)
									.getAllRelationships();
							int relNumbers = 0;
							for (Relationship rel : relationships) {
								relNumbers++;
							}
							printQueryInTheCorrespondingFile(smallGraph, gName, querySize, relNumbers);
						}
						if (node.getDegree() > 1) {
							seenMoreThanOneDegreeNode = true;
						}

					}
					tx1.success();
				}
				smallGraph.shutdown();
			}
		}

	}

	public static void printQueryInTheCorrespondingFile(GraphDatabaseService smallGraph, String gName, int querySize,
			int relNumbers) throws IOException {
		HashSet<Node> set = new HashSet<Node>();
		ResourceIterable<Node> nodes = GlobalGraphOperations.at(smallGraph).getAllNodes();
		for (Node node : nodes) {
			set.add(node);
		}
		HashSet<Long> neighborSet = new HashSet<>();
		BufferedWriter output = new BufferedWriter(
				new FileWriter("nonStarQueries_" + gName + "_" + set.size() + "_" + relNumbers + ".txt", true));
		// traverse the graphDB using the node list from the tree/graph
		Node[] nodeArray = set.toArray(new Node[set.size()]);
		if (set.size() > 0) {
			output.write(set.size() + "\n");
		}
		// write the nodes labels
		for (Node node : nodeArray) {
			// output.write(String.valueOf(node.getPropertyKeys()));
			Iterable<Label> nodeLabel = node.getLabels();
			Iterator<Label> labelIterator = nodeLabel.iterator();
			// if (!labelIterator.hasNext())
			// output.write("No_Label_" + node.getId() + ";");
			output.write(node.getId() + ";");
			output.write(labelIterator.next().toString() + "\n");

		}

		for (Node node : nodeArray) {
			Iterable<Relationship> relations = node.getRelationships();
			Iterator<Relationship> relationIterator = relations.iterator();

			output.write(node.getId() + ";");
			// first = false;
			neighborSet.add(new Long(node.getId()));
			while (relationIterator.hasNext()) {
				Relationship rel = relationIterator.next();
				if (rel.getStartNode().getId() == node.getId()) {
					if (set.contains(rel.getOtherNode(node)) && !neighborSet.contains(rel.getOtherNode(node).getId())) {
						neighborSet.add(new Long(rel.getOtherNode(node).getId()));
						output.write(rel.getOtherNode(node).getId() + ";");
					}
				}
			}
			output.write("\n");
			neighborSet.clear();
		}
		output.close();
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
