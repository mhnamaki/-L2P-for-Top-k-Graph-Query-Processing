package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.HashMap;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;

public class CacheServer {
	public HashMap<Long, HashMap<Long, Float>> nodesSimilarityQNodeIdGNodeIdValue;

	public void addNodesSimilarity(Long qNodeId, Long gNodeId, Float value) {
		if (nodesSimilarityQNodeIdGNodeIdValue.get(qNodeId) == null) {
			HashMap<Long, Float> temp = new HashMap<Long, Float>();
			temp.put(gNodeId, value);
			nodesSimilarityQNodeIdGNodeIdValue.put(qNodeId, temp);
		} else {
			nodesSimilarityQNodeIdGNodeIdValue.get(qNodeId).put(gNodeId, value);
		}
	}

	public float getSimilarity(long qNodeId, long gNodeId) {
		float temp = -1;
		if (nodesSimilarityQNodeIdGNodeIdValue.get(qNodeId) != null
				&& nodesSimilarityQNodeIdGNodeIdValue.get(qNodeId).get(gNodeId) != null) {
			temp = nodesSimilarityQNodeIdGNodeIdValue.get(qNodeId).get(gNodeId);
		}

		return temp;
	}

	public void clear() {
		if (nodesSimilarityQNodeIdGNodeIdValue != null)
			nodesSimilarityQNodeIdGNodeIdValue.clear();
		nodesSimilarityQNodeIdGNodeIdValue = null;
		nodesSimilarityQNodeIdGNodeIdValue = new HashMap<Long, HashMap<Long, Float>>();

	}
}
