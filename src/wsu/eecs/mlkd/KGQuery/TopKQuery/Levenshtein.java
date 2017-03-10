package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;

//public class GraphItemSimilarity {

// normalization for that is not so good! I should find some other things
public class Levenshtein {
	public HashMap<String, HashSet<Long>> nodeLabelsIndex;
	public int numberOfPrefixChars;
	// public GraphDatabaseService gGraph;
	// public HashSet<Long> exactMatchedGNodeIds = new HashSet<Long>();

	public Levenshtein(HashMap<String, HashSet<Long>> nodeLabelsIndex, int numberOfPrefixChars) {
		this.nodeLabelsIndex = nodeLabelsIndex;
		this.numberOfPrefixChars = numberOfPrefixChars;
		// this.gGraph = gGraph;

		// exactMatchedGNodeIds.add((long) 3838702);
		// exactMatchedGNodeIds.add((long) 3838703);
		// exactMatchedGNodeIds.add((long) 235);
		// exactMatchedGNodeIds.add((long) 31286);
		// exactMatchedGNodeIds.add((long) 2969668);
	}

	// max-based
	// public float HowMuchTwoNodesAreSimilar(Node qNode, Node knldgNode) {
	// float cachServerValue = CacheServer.getSimilarity(qNode.getId(),
	// knldgNode.getId());
	// if (cachServerValue > -1) {
	// return cachServerValue;
	// }
	//
	// Iterator<Label> iterator = qNode.getLabels().iterator();
	//
	// String qLabelString = iterator.next().toString();
	//
	// HashSet<Long> nodePossibleMatchedIds =
	// nodeLabelsIndex.get(qLabelString.substring(0, numberOfPrefixChars));
	//
	// if (!nodePossibleMatchedIds.contains(knldgNode.getId())) {
	// CacheServer.addNodesSimilarity(qNode.getId(), knldgNode.getId(), 0F);
	// return 0;
	// }
	//
	// float maxSimilarity = 0;
	// int numberOfPairLabels = 0;
	// Iterable<Label> knldglabelIterator = knldgNode.getLabels();
	//
	// boolean hasLabel = false;
	// for (Label knldgLabel : knldglabelIterator) {
	// hasLabel = true;
	// float tempValue = Levenshtein.normalizedDistance(qLabelString,
	// knldgLabel.toString());
	// if (tempValue > 0) {
	// maxSimilarity = Math.max(tempValue, maxSimilarity);
	// if (maxSimilarity >= 1) {
	// CacheServer.addNodesSimilarity(qNode.getId(), knldgNode.getId(),
	// maxSimilarity);
	// return maxSimilarity;
	// }
	// numberOfPairLabels++;
	// }
	//
	// }
	// if (!hasLabel) {
	// for (String key : knldgNode.getPropertyKeys()) {
	// float tempValue = Levenshtein.normalizedDistance(qLabelString,
	// knldgNode.getProperty(key).toString());
	// if (tempValue > 0) {
	// maxSimilarity = Math.max(tempValue, maxSimilarity);
	// if (maxSimilarity >= 1) {
	// CacheServer.addNodesSimilarity(qNode.getId(), knldgNode.getId(),
	// maxSimilarity);
	// return maxSimilarity;
	// }
	//
	// numberOfPairLabels++;
	// }
	// }
	// }
	//
	// if (numberOfPairLabels == 0)
	//
	// {
	// CacheServer.addNodesSimilarity(qNode.getId(), knldgNode.getId(), 0F);
	// return 0;
	// }
	//
	// CacheServer.addNodesSimilarity(qNode.getId(), knldgNode.getId(),
	// maxSimilarity);
	//
	// return maxSimilarity;
	// }

	public float HowMuchALabelAndNodeAreSimilar(GraphDatabaseService gGraph, String qLabelString, Node knldgNode) {
		float sumSimilarity = 0;
		// float maxSimilarity = 0;
		int numberOfPairLabels = 0;
		Iterable<Label> knldglabelIterator = knldgNode.getLabels();
		// ArrayList<String> knldgPropertyValues = new ArrayList<String>();

		// if(qLabelString.length()<numberOfPrefixChars){
		// int iiii=0;
		// iiii++;
		// }

		boolean hasLabel = false;
		for (Label knldgLabel : knldglabelIterator) {
			hasLabel = true;
			// System.out.println(qlabel.toString().toLowerCase());
			float tempValue = Levenshtein.normalizedDistance(qLabelString, knldgLabel.toString());
			if (tempValue > 0) {
				sumSimilarity += tempValue;
				// maxSimilarity = Math.max(tempValue, maxSimilarity);
				// if (maxSimilarity >= 1) {
				// CacheServer.addNodesSimilarity(qNode.getId(),
				// knldgNode.getId(), maxSimilarity);
				// return maxSimilarity;
				// }
				// System.out.println("same labels: " +
				// qlabel.toString() + ", " + knldgLabel.toString());
				numberOfPairLabels++;
			}

		}
		if (!hasLabel) {
			for (String key : knldgNode.getPropertyKeys()) {
				float tempValue = Levenshtein.normalizedDistance(qLabelString, knldgNode.getProperty(key).toString());
				if (tempValue > 0) {
					sumSimilarity += tempValue;
					// maxSimilarity = Math.max(tempValue, maxSimilarity);
					// if (maxSimilarity >= 1) {
					// CacheServer.addNodesSimilarity(qNode.getId(),
					// knldgNode.getId(), maxSimilarity);
					// return maxSimilarity;
					// }
					// System.out.println(
					// "same prop: " + qlabel.toString() + ", " +
					// knldgNode.getProperty(key).toString());
					numberOfPairLabels++;
				}
			}
		}

		if (numberOfPairLabels == 0)

		{
			// System.out.println("IMPORTANT!! At least one of the nodes " +
			// qNode.getId() + ","
			// + knldgNode.getId() + " doesn't have label/property");
			// CacheServer.addNodesSimilarity(qNode.getId(), knldgNode.getId(),
			// 0F);
			return 0;
		}

		float returnValue = (sumSimilarity / numberOfPairLabels);

		return returnValue;
		// return maxSimilarity;
	}

	// exact based
	public float HowMuchTwoNodesAreSimilar(GraphDatabaseService gGraph, Node qNode, Node knldgNode,
			NeighborIndexing neighborIndexInstance, CacheServer cacheServer) {
		if (!Dummy.DummyProperties.semanticChecking) {
			return 1F;
		}
		float cachServerValue = cacheServer.getSimilarity(qNode.getId(), knldgNode.getId());
		if (cachServerValue > -1) {
			return cachServerValue;
		}

		// Iterator<Label> iterator = qNode.getLabels().iterator();
		//
		String qLabelString = neighborIndexInstance.queryNodeLabelMap.get(qNode.getId());

		HashSet<Long> nodePossibleMatchedIds = nodeLabelsIndex.get(qLabelString.substring(0, numberOfPrefixChars));

		if (!nodePossibleMatchedIds.contains(knldgNode.getId())) {
			cacheServer.addNodesSimilarity(qNode.getId(), knldgNode.getId(), 0F);
			return 0;
		}
		float returnValue = HowMuchALabelAndNodeAreSimilar(gGraph, qLabelString, knldgNode);
		cacheServer.addNodesSimilarity(qNode.getId(), knldgNode.getId(), returnValue);
		return returnValue;

	}

	public static float normalizedDistance(String a, String b) {
		return 1 - (float) distance(a, b) / (Math.max(a.length(), b.length()));
	}

	public static int distance(String a, String b) {
		a = a.toLowerCase();
		b = b.toLowerCase();
		// i == 0
		int[] costs = new int[b.length() + 1];
		for (int j = 0; j < costs.length; j++)
			costs[j] = j;
		for (int i = 1; i <= a.length(); i++) {
			// j == 0; nw = lev(i - 1, j)
			costs[0] = i;
			int nw = i - 1;
			for (int j = 1; j <= b.length(); j++) {
				int cj = Math.min(1 + Math.min(costs[j], costs[j - 1]),
						a.charAt(i - 1) == b.charAt(j - 1) ? nw : nw + 1);
				nw = costs[j];
				costs[j] = cj;
			}
		}
		return costs[b.length()];
	}

}
// }
