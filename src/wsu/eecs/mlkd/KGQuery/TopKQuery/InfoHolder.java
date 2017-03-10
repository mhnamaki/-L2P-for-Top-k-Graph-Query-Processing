package wsu.eecs.mlkd.KGQuery.TopKQuery;

public class InfoHolder {
	public int queryIndex;

	public int sfFetchesCalls;
	public int sfTotalRequestForFetches;
	public int sfJoinCalls;
	public double sfFetchesTime;
	public double sfJoinTime;
	public double sfCheckShouldFinishTime;

	public int oracleFetchesCalls;
	public int oracleJoinCalls;
	public int oracleTotalRequestForFetches;
	public double oracleFetchesTime;
	public double oracleJoinTime;
	public double oracleCheckShouldFinishTime;

	public int mlFetchesCalls;
	public int mlJoinCalls;
	public int mlTotalRequestForFetches;
	public double mlFetchesTime;
	public double mlJoinTime;
	public double mlFeaturesComputationalTime;
	public double mlClsInferenceComputationalTime;
	public double mlRegInferenceComputationalTime;
	public double mlCheckShouldFinishTime;

}