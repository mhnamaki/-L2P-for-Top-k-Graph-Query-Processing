package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

public class WeightedRelationship {
	int srcId;
	int destId;
	double weight;

	public WeightedRelationship(Integer srcId, Integer destId, double weight) {
		this.srcId = srcId;
		this.destId = destId;
		this.weight = weight;
	}

}