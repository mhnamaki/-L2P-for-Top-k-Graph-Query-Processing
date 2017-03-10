package wsu.eecs.mlkd.KGQuery.GraphMaker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class GraphSampler {

	public static void main(String[] args) throws Exception {
		String dataGraphPath = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.3.1/data/";
		String graphName = "yago.db";
		double percentage = 2f;
		int maxNumberOfSamples = 5;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-dataGraphPath")) {
				dataGraphPath = args[++i];
			} else if (args[i].equals("-graphName")) {
				graphName = args[++i];
			} else if (args[i].equals("-percentage")) {
				percentage = Float.parseFloat(args[++i]);
			} else if (args[i].equals("-maxNumberOfSamples")) {
				maxNumberOfSamples = Integer.parseInt(args[++i]);
			}
		}

		File storeDir = new File(dataGraphPath + graphName);
		GraphDatabaseService dataGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(storeDir)
				.setConfig(GraphDatabaseSettings.pagecache_memory, "6g")
				.setConfig(GraphDatabaseSettings.allow_store_upgrade, "true").newGraphDatabase();

		System.out.println("dataset: " + dataGraphPath + graphName);

		ArrayList<Long> allRelations = new ArrayList<Long>();
		// HashSet<Long> allRelations = new HashSet<Long>();

		Transaction tx1 = dataGraph.beginTx();
		for (Node node : dataGraph.getAllNodes()) {
			for (Relationship rel : node.getRelationships(Direction.OUTGOING)) {
				allRelations.add(rel.getId());
			}
		}

		System.out.println("all relations: " + allRelations.size());
		tx1.success();
		dataGraph.shutdown();

		int allRels = allRelations.size();

		double step = (percentage / 100) * allRels;
		System.out.println("step: " + step);
		Random rnd = new Random();
		HashSet<Long> seenRels = new HashSet<Long>();
		for (int s = 0; s < maxNumberOfSamples; s++) {

			String newPath = copyDataSet(dataGraphPath, graphName, s);
			System.out.println("newPath: " + s + " " + newPath);
			GraphDatabaseService newDataGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(newPath)
					.setConfig(GraphDatabaseSettings.pagecache_memory, "6g")
					.setConfig(GraphDatabaseSettings.allow_store_upgrade, "true").newGraphDatabase();

			Transaction tx2 = newDataGraph.beginTx();
			int cntRemove = 0;
			ArrayList<Long> newRemovingEdges = new ArrayList<Long>();
			while (cntRemove < step) {
				int selectedRel = rnd.nextInt(allRelations.size());
				Long relId = allRelations.get(selectedRel);
				if (seenRels.add(relId)) {
					newRemovingEdges.add(relId);
					cntRemove++;
				}
			}

			for (Long relRemove : newRemovingEdges) {
				Relationship shouldRemoveRel = newDataGraph.getRelationshipById(relRemove);
				shouldRemoveRel.delete();
			}

			int allDegrees = 0;
			for (Node node : newDataGraph.getAllNodes()) {
				allDegrees += node.getDegree();
			}

			tx2.success();
			tx2.close();

			int relsNum = allDegrees / 2;
			System.out.println("s: " + s + " all rels: " + relsNum);
			System.out.println("percentage: " + (relsNum / allRels));

			newDataGraph.shutdown();
		}

	}

	private static String copyDataSet(String dataGraphPath, String graphName, int s) throws Exception {

		String destDirStr = "";
		if (s == 0) {
			File srcDir = new File(dataGraphPath + graphName);
			destDirStr = dataGraphPath + graphName + s;
			File destDir = new File(destDirStr);

			FileUtils.copyDirectory(srcDir, destDir);

		} else {
			int l = s - 1;
			File srcDir = new File(dataGraphPath + graphName + l);
			destDirStr = dataGraphPath + graphName + s;
			File destDir = new File(destDirStr);

			FileUtils.copyDirectory(srcDir, destDir);
		}

		return destDirStr;

	}

}
