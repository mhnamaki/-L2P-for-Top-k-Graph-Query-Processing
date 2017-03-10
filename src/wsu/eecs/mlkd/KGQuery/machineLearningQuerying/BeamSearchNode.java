package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.util.ArrayList;
import java.util.HashMap;

import wsu.eecs.mlkd.KGQuery.TopKQuery.AnyTimeStarFramework;

//Node for oracle learning tree
public class BeamSearchNode {
//	public HashMap<String, Integer> SQSelectionfeaturesMap;
//	public HashMap<String, Integer> SQExpandfeaturesMap;
//	public HashMap<String, Integer> stoppingSFFeaturesMap;
	public BeamSearchNode firstAncestor;
	public BeamSearchNode parentNode;
	// public double quality;
	public double rank;
	public double farAwayPenalty;
	public double targetDistance;
	// public float timeFromStart;
	public double minObjective;
	public double diffTime;
	// public double differentialQualityWithPrevStateDividedByTime;
	public double totalTimeInMilliseconds;
	public ArrayList<ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>> generalArrNumberOfFetches;
	public HashMap<Integer, Integer> resultPartialAnswersShouldBeByNodeIndex;
	public int level = 0;
	public String sequenceForDebug = "";
	public AnyTimeStarFramework starframework;

	public BeamSearchNode(BeamSearchNode parentNode,
			ArrayList<ArrayList<StarQueryIndexWithNumberOfPartialAnswerToBeFetched>> generalArrNumberOfFetches) {
		this.generalArrNumberOfFetches = generalArrNumberOfFetches;
		this.parentNode = parentNode;
		resultPartialAnswersShouldBeByNodeIndex = new HashMap<Integer, Integer>();
		for (StarQueryIndexWithNumberOfPartialAnswerToBeFetched sq : generalArrNumberOfFetches
				.get(generalArrNumberOfFetches.size() - 1)) {
			resultPartialAnswersShouldBeByNodeIndex.put(sq.starQueryIndex, sq.numberOfPartialAnswersShouldBeFetched);
		}
		if (this.parentNode != null)
			level = this.parentNode.level + 1;

//		SQSelectionfeaturesMap = new HashMap<String, Integer>();
//		SQExpandfeaturesMap = new HashMap<String, Integer>();
//		stoppingSFFeaturesMap = new HashMap<String, Integer>();
	}
}
