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
import java.util.HashSet;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class FrequentPatternConvertToQuery {

	public static void main(String[] args) throws IOException {
		ArrayList<Query> queries = new ArrayList<Query>();

		File file = new File("yagoPatterns");
		FileInputStream fis = new FileInputStream(file);

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		while ((line = br.readLine()) != null) {
			String[] splittedLine = line.split("\t");
			if (splittedLine.length > 1) {
				Query query = new Query();
				query.nodes.add(new MyNode(Integer.parseInt(splittedLine[0]), splittedLine[1]));
				while ((line = br.readLine()) != null) {
					splittedLine = line.split("\t");
					if (splittedLine.length > 1) {
						query.nodes.add(new MyNode(Integer.parseInt(splittedLine[0]), splittedLine[1]));
					} else {
						break;
					}
				}
				int numberOfRelationships = Integer.parseInt(line);
				for (int i = 0; i < numberOfRelationships; i++) {
					line = br.readLine();
					splittedLine = line.split("\t");
					query.relationships.add(
							new MyRelationship(Integer.parseInt(splittedLine[0]), Integer.parseInt(splittedLine[1])));
				}
				queries.add(query);
			}

		}

		br.close();

		// HashSet<Integer> seenSizeSet = new HashSet<Integer>();
		// for (Query query : queries) {
		// if (!seenSizeSet.contains(query.nodes.size())) {
		// seenSizeSet.add(query.nodes.size());
		// File fout = new File("convertedDBPedia_" + query.nodes.size() +
		// ".txt");
		// FileOutputStream fos = new FileOutputStream(fout);
		//
		// BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		// bw.close();
		// }
		// }

		for (Query query : queries) {
			int size = query.nodes.size();
			File fout = new File("convertedYago_" + size + ".txt");
			FileOutputStream fos = new FileOutputStream(fout, true);

			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

			bw.write(Integer.toString(size));
			bw.newLine();
			for (MyNode node : query.nodes) {
				bw.write(node.nodeId + ";" + node.label);
				bw.newLine();
			}
			HashSet<Integer> seenSrcNodes = new HashSet<Integer>();
			for (MyRelationship srcRelationship : query.relationships) {

				if (!seenSrcNodes.contains(srcRelationship.srcNodeId)) {
					bw.write(srcRelationship.srcNodeId + ";");
					seenSrcNodes.add(srcRelationship.srcNodeId);
					for (MyRelationship destRelationship : query.relationships) {
						if (destRelationship.srcNodeId == srcRelationship.srcNodeId) {
							bw.write(destRelationship.destNodeId + ";");
						}
					}
					bw.newLine();
				}
			}
			for (MyNode node : query.nodes) {
				if (!seenSrcNodes.contains(node.nodeId)) {
					bw.write(node.nodeId + ";");
					seenSrcNodes.add(node.nodeId);
					bw.newLine();
				}
			}

			bw.close();
		}

	}

}

class Query {
	ArrayList<MyNode> nodes = new ArrayList<MyNode>();
	ArrayList<MyRelationship> relationships = new ArrayList<MyRelationship>();
}

class MyNode {
	Integer nodeId;
	String label;

	public MyNode(Integer nodeId, String label) {
		this.nodeId = nodeId;
		this.label = label;
	}
}

class MyRelationship {
	Integer srcNodeId;
	Integer destNodeId;

	public MyRelationship(Integer srcNodeId, Integer destNodeId) {
		this.srcNodeId = srcNodeId;
		this.destNodeId = destNodeId;
	}
}
