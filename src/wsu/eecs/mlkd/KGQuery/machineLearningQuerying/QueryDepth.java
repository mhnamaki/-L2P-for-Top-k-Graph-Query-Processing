package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

public class QueryDepth {
	public int queryId;
	public HashMap<Integer, Integer> queryIndexDepthMap;

	public QueryDepth(int queryId, HashMap<Integer, Integer> queryIndexDepthMap) {
		this.queryId = queryId;
		this.queryIndexDepthMap = new HashMap<Integer, Integer>();
	}

	public static ArrayList<QueryDepth> queryDepthRead(String queryDepthAddress) throws Exception {
		ArrayList<QueryDepth> queriesInfo = new ArrayList<QueryDepth>();

		FileInputStream fis = new FileInputStream(queryDepthAddress);

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		while ((line = br.readLine()) != null) {
			String[] queryInfos = line.split(";");
			if (queryInfos.length > 2) {
				HashMap<Integer, Integer> queryIndexDepthMap = new HashMap<Integer, Integer>();
				String[] depthInfos = queryInfos[2].replace("(", "").replace(")", "").split(",");
				for (String qIndexAndDepthPair : depthInfos) {
					String[] qIndexAndDepth = qIndexAndDepthPair.split(":");
					queryIndexDepthMap.put(Integer.parseInt(qIndexAndDepth[0].trim()),
							Integer.parseInt(qIndexAndDepth[1].trim()));
				}
				queriesInfo.add(new QueryDepth(Integer.parseInt(queryInfos[0]), queryIndexDepthMap));
			}

		}

		br.close();

		return queriesInfo;
	}

}
