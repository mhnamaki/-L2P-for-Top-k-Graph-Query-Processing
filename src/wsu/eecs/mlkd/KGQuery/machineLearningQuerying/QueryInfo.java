package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class QueryInfo {
	// queryIndex + ";" + numberOfStars + ";" + time + ";" + totalSFDepth + ";"
	// + totalAnswersDepth
	// + ";" + totalFirstPQItems + ";" + totalPAEstimates + ";" + postFix
	public int queryId;
	public double numberOfStars;
	public double sfTime;
	public int sfTotalDepth;
	public int answersTotalDepth;
	public int totalFirstPQItems;
	public int totalPAEstimates;

	public QueryInfo(int queryId, int numberOfStars, double sfTime, int sfTotalDepth, int answersTotalDepth,
			int totalFirstPQItems, int totalPAEstimates) {
		this.queryId = queryId;
		this.numberOfStars = numberOfStars;
		this.sfTime = sfTime;
		this.sfTotalDepth = sfTotalDepth;
		this.answersTotalDepth = answersTotalDepth;
		this.totalFirstPQItems = totalFirstPQItems;
		this.totalPAEstimates = totalPAEstimates;

	}

	public static ArrayList<QueryInfo> queryInfoRead(String queryInfoAddress) throws Exception {
		ArrayList<QueryInfo> queriesInfo = new ArrayList<QueryInfo>();

		FileInputStream fis = new FileInputStream(queryInfoAddress);

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		while ((line = br.readLine()) != null) {
			String[] queryInfos = line.split(";");
			if (queryInfos.length > 1) {
				queriesInfo.add(new QueryInfo(Integer.parseInt(queryInfos[0]), Integer.parseInt(queryInfos[1]),
						Double.parseDouble(queryInfos[2]), Integer.parseInt(queryInfos[3]),
						Integer.parseInt(queryInfos[4]), Integer.parseInt(queryInfos[5]),
						Integer.parseInt(queryInfos[6])));
			}

		}

		br.close();

		return queriesInfo;
	}

}
