package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

public class VectorTool {
	public double computeHeuristicValue(double[] weightVector, int[] wordFeaturesVector) {

		double dp = 0;
		for (int i = 0; i < weightVector.length; i++) {
			dp += weightVector[i] * wordFeaturesVector[i];
		}
		return dp;

	}

	public double[] sumVectors(double[] v1, int[] v2) {

		if (v1.length != v2.length) {
			return null;
		} else {
			double[] tempArr = new double[v1.length];
			for (int i = 0; i < v1.length; i++) {
				tempArr[i] = v1[i] + v2[i];
			}
			return tempArr;
		}
	}

	public double[] sumVectors(double[] v1, double[] v2) {

		if (v1.length != v2.length) {
			return null;
		} else {
			double[] tempArr = new double[v1.length];
			for (int i = 0; i < v1.length; i++) {
				tempArr[i] = v1[i] + v2[i];
			}
			return tempArr;
		}
	}

	public double[] divideVectorByScalar(double[] avgWVector, int dividend) {

		for (int i = 0; i < avgWVector.length; i++) {
			avgWVector[i] = avgWVector[i] / dividend;
		}
		return avgWVector;
	}

	public double[] scalarMultiply(double coeff, double[] inputData) {
		double[] scaledVector = new double[inputData.length];
		for (int i = 0; i < inputData.length; i++) {
			scaledVector[i] = coeff * inputData[i];
		}
		return scaledVector;
	}

	public double[] subtractVectors(double[] main, double[] other) {
		double[] sv = new double[main.length];
		for (int i = 0; i < main.length; i++) {
			sv[i] = main[i] - other[i];
		}
		return sv;
	}

}
