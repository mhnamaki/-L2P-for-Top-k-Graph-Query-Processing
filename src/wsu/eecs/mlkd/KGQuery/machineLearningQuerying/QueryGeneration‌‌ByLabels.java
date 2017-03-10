package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Random;

public class QueryGeneration‌‌ByLabels {

	public static void main(String[] args) throws Exception {
		Random rnd = new Random();

		File fout = new File("lblGeneratedQueries.txt");
		FileOutputStream fos = new FileOutputStream(fout);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		FileInputStream wordsFile = new FileInputStream("threeletterswords.csv");
		BufferedReader brWords = new BufferedReader(new InputStreamReader(wordsFile));
		ArrayList<String> words = new ArrayList<String>();
		String wordLine = null;
		while ((wordLine = brWords.readLine()) != null) {
			words.add(wordLine.trim());
		}

		brWords.close();

		FileInputStream fis = new FileInputStream("/Users/mnamaki/Documents/workspace/wsu.eecs.mlkd.KGQuery/dbPedia/queries/BasedOnNumberOfSQs0_0.txt");

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String queryLine = null;
		while ((queryLine = br.readLine()) != null) {

			ArrayList<QueryNode> nodeIdLabelArr = new ArrayList<QueryNode>();
			String relationsPart = "";

			int numberOfNodes = Integer.parseInt(queryLine.trim());
			int queryIndex = Integer.parseInt(br.readLine().split(":")[1]);
			for (int i = 0; i < numberOfNodes; i++) {
				String[] nodesLabels = br.readLine().split(";");
				nodeIdLabelArr.add(new QueryNode(Integer.parseInt(nodesLabels[0]), nodesLabels[1]));
			}
			for (int i = 0; i < numberOfNodes; i++) {
				queryLine = br.readLine();
				relationsPart += queryLine + "\n";
			}

			for (int i = 91; i < 95; i++) {

				String newQuery = "";
				newQuery += numberOfNodes + "\n";
				newQuery += "queryIndex:" + i;
				int digits = String.valueOf(queryIndex).length();
				for (int j = 0; j < (4 - digits); j++) {
					newQuery += "0";
				}
				newQuery += queryIndex + "\n";

				for (QueryNode qNode : nodeIdLabelArr) {
					int wordIndex = rnd.nextInt(words.size());
					newQuery += qNode.nodeId + ";" + qNode.nodeLabel.trim() + "_" + words.get(wordIndex).trim() + ";"
							+ "\n";
				}
				newQuery += relationsPart;

				bw.write(newQuery);
				bw.flush();
			}

		}

		br.close();
		bw.close();
	}

}

class QueryNode {
	Integer nodeId;
	String nodeLabel;

	public QueryNode(Integer nodeId, String nodeLabel) {
		this.nodeId = nodeId;
		this.nodeLabel = nodeLabel;
	}
}
