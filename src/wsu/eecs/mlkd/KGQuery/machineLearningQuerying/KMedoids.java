package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;

import info.aduna.text.StringUtil;
import wsu.eecs.mlkd.KGQuery.machineLearningQuerying.KMedoids.SmallGraphType;

public class KMedoids {

	public int numberOfClusters = 0;
	FeatureType[] featureTypes;
	HashMap<String, Integer> editDistanceMap;
	int maxEditDistance = 0;
	int numberOfFeatures = 0;
	double[] maxValueOfThisFeature;
	VectorTool vTool = new VectorTool();
	Random randomGenerator = new Random();
	int maxIterations = 0;

	public enum FeatureType {
		GraphDistance, Double
	};

	public enum SmallGraphType {
		banner, bool, butterfly, chair, cricket, dart, gem, house, net, paw
	};

	public KMedoids(int numberOfFeatures, int numberOfClusters, FeatureType[] featureTypes, int maxIterations)
			throws Exception {

		editDistanceMap = new HashMap<String, Integer>();
		this.numberOfClusters = numberOfClusters;
		this.numberOfFeatures = numberOfFeatures;
		// this.instances = instances;
		this.featureTypes = featureTypes;
		this.maxIterations = maxIterations;

		fillEditDistanceMap();

	}

	private void fillEditDistanceMap() throws Exception {
		FileInputStream fis = new FileInputStream(
				new File("/Users/mnamaki/Documents/workspace/wsu.eecs.mlkd.KGQuery/editDistance.csv"));

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		while ((line = br.readLine()) != null) {
			String[] items = line.split(",");
			// symmetry relation
			int distance = Integer.parseInt(items[2]);
			editDistanceMap.put(items[0] + "_" + items[1], distance);
			editDistanceMap.put(items[1] + "_" + items[0], distance);
			if (distance > maxEditDistance) {
				maxEditDistance = distance;
			}
		}

		br.close();

	}

	public Instance[] buildClusters(ArrayList<Instance> instances, double[] featuresImportance) {
		maxValueOfThisFeature = new double[numberOfFeatures];
		for (int i = 0; i < instances.size(); i++) {
			instances.get(i).assignedCluster = -1; // re-init
			for (int j = 0; j < numberOfFeatures; j++) {
				if (featureTypes[j] != FeatureType.GraphDistance) {
					if (instances.get(i).features[j] > maxValueOfThisFeature[j]) {
						maxValueOfThisFeature[j] = instances.get(i).features[j];
					}
				}
			}
		}

		// choose k random vectors to start our clusters
		ArrayList<Integer> list = new ArrayList<Integer>();
		for (int i = 0; i < instances.size(); i++) {
			list.add(new Integer(i));
		}
		Collections.shuffle(list);

		Instance[] medoids = new Instance[numberOfClusters];
		for (int i = 0; i < numberOfClusters; i++) {
			System.out.println("random instance-index:" + list.get(i) + " for cluster: " + i);
			medoids[i] = instances.get(list.get(i));
			instances.get(list.get(i)).assignedCluster = i;
		}
		// medoids[0] = instances.get(1);
		// medoids[1] = instances.get(4);

		// run
		boolean changed = true;
		int count = 0;
		while (changed && count < maxIterations) {
			changed = false;
			count++;
			int[] assignment = assign(medoids, instances, featuresImportance);
			changed = recalculateMedoids(assignment, medoids, instances, featuresImportance);

			if ((count % 10) == 0) {
				System.out.println("progress: " + (double) count / (double) maxIterations * 100);
			}
		}

		// queryFileName;queryIndex;primValue;numberOfStars;sumJoinableNodes;finalDifferenceTime;
		for (int c = 0; c < numberOfClusters; c++) {
			for (int i = 0; i < instances.size(); i++) {
				if (instances.get(i).assignedCluster == c) {
					System.out.println(instances.get(i).queryFileName + "; i:" + instances.get(i).queryIndex + "; mst:"
							+ instances.get(i).primValue + "; ns:" + instances.get(i).numberOfStars + ";j:"
							+ instances.get(i).sumJoinableNodes + "; PQ:" + instances.get(i).avgFirstPQItemSize + ";"
							+ instances.get(i).avgParialAnswersEstimate + "; t:" + instances.get(i).finalDifferenceTime
							+ "; c:" + instances.get(i).assignedCluster);
				}
			}
		}
		System.out.println("iterations done: " + count);

		System.out.println("training is finished properly!");

		return medoids;
	}

	private boolean recalculateMedoids(int[] assignment, Instance[] medoids, ArrayList<Instance> instances,
			double[] featuresImportance) {
		boolean changed = false;

		for (int i = 0; i < numberOfClusters; i++) {
			Instance thisClusterMedoid = medoids[i];
			for (int j = 0; j < instances.size(); j++) {
				if (assignment[j] == i) { // cluster of instance j is equal to
											// i.
					for (int k = 0; k < numberOfFeatures; k++) {
						// update the graph distance based on this cluster for
						// computing the centroid
						if (featureTypes[k] == FeatureType.GraphDistance) {
							instances.get(j).features[k] = getGraphDistance(thisClusterMedoid, instances.get(j));
						}
					}
				}
			}
		}

		for (int i = 0; i < numberOfClusters; i++) {
			boolean empty = true;
			for (int j = 0; j < assignment.length; j++) {
				if (assignment[j] == i) {
					instances.get(j).assignedCluster = assignment[j];
					empty = false;
				}
			}
			if (empty) { // new random, empty medoid
				medoids[i] = instances.get(randomGenerator.nextInt(instances.size()));
				changed = true;
			} else {
				Instance oldMedoid = medoids[i];
				Instance newMedoid = oldMedoid;
				double bestDistance = 0;
				for (int n = 0; n < instances.size(); n++) {
					if (instances.get(n).assignedCluster == i) {
						bestDistance += getTotalDistance(instances.get(n), oldMedoid, featuresImportance);
					}
				}

				for (int m = 0; m < instances.size(); m++) {
					if (instances.get(m).assignedCluster == i) {
						double tempDistance = 0;
						for (int n = 0; n < instances.size(); n++) {
							if (instances.get(n).assignedCluster == i && n != m) {
								tempDistance += getTotalDistance(instances.get(n), instances.get(m),
										featuresImportance);
							}
						}
						if (tempDistance < bestDistance) {
							bestDistance = tempDistance;
							newMedoid = instances.get(m);
						}
					}
				}

				medoids[i] = newMedoid;

				if (!medoids[i].equals(oldMedoid))
					changed = true;
			}
		}

		// for debug start
		for (int i = 0; i < numberOfClusters; i++) {
			if (medoids[i] == null) {
				System.err.println(i + " is null");
			}
		}
		// for debug end

		return changed;
	}

	public double[] getCentroid(ArrayList<Instance> instnaces, int clusterIndex, double[] featuresImportance) {
		if (instnaces.size() == 0)
			return null;

		double[] sumPosition = new double[numberOfFeatures];
		int cnt = 0;
		for (int i = 0; i < instnaces.size(); i++) {
			Instance in = instnaces.get(i);
			if (in.assignedCluster == clusterIndex) {
				sumPosition = vTool.sumVectors(sumPosition, in.features);
				cnt++;
			}
		}
		sumPosition = vTool.divideVectorByScalar(sumPosition, cnt);

		return sumPosition;

	}

	private void initialize(int numberOfSameExperiments, double[] featuresImportance) throws Exception {

		ArrayList<Instance> allInstances = new ArrayList<Instance>();

		FileInputStream fis = new FileInputStream(new File("yago.db_learning.csv"));

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		String[] header = br.readLine().split(",");
		while ((line = br.readLine()) != null) {

			String[] items = line.split(",");
			// 0 1 2 3 4 5 6 7
			// queryFileName;queryIndex;primValue;numberOfStars;sumJoinableNodes;avgFirstPQItemSize;avgParialAnswersEstimate;finalDifferenceTime;

			allInstances.add(new Instance(items[0].replace(".txt", ""), Integer.parseInt(items[1]),
					Integer.parseInt(items[2]), Integer.parseInt(items[3]), Integer.parseInt(items[4]),
					Integer.parseInt(items[5]), Integer.parseInt(items[6]), Integer.parseInt(items[7])));
		}

		// Experiment1: measuring the clusters by allInstances
		double bestInternalDistance = Double.MAX_VALUE;
		double bestExternalDistance = Double.MAX_VALUE;

		for (int exp = 0; exp < numberOfSameExperiments; exp++) {

			System.out.println("exp: " + exp);

			Instance[] medoids = buildClusters(allInstances, featuresImportance);
			HashMap<Integer, Double> internalDistanceMap = new HashMap<Integer, Double>();

			// internal distance based on the feature vectors
			for (int clusterIndex = 0; clusterIndex < numberOfClusters; clusterIndex++) {
				int cnt = 0;
				double sum = 0;
				for (int i = 0; i < allInstances.size(); i++) {
					if (allInstances.get(i).assignedCluster == clusterIndex
							&& !allInstances.get(i).equals(medoids[clusterIndex])) {
						sum += getTotalDistance(allInstances.get(i), medoids[clusterIndex], featuresImportance);
						cnt++;
					}
				}
				if (cnt == 0) {
					System.err.println(clusterIndex + " clusterIndex doesnt have any item.");
				}
				if (cnt != 0)
					internalDistanceMap.put(clusterIndex, sum / cnt);
				cnt++;
				System.out.println("members of the cluster: " + clusterIndex + " is " + cnt);
				System.out.println("medoid features: " + StringUtils.join(medoids[clusterIndex].features, ','));
				System.out.println("medoid running time: " + medoids[clusterIndex].finalDifferenceTime);
			}

			HashMap<Integer, Double> externalDistanceMap = new HashMap<Integer, Double>();
			// external distance based on the running time
			for (int clusterIndex = 0; clusterIndex < numberOfClusters; clusterIndex++) {
				int cnt = 0;
				double sum = 0;
				boolean noItemInTheCluster = true;
				for (int i = 0; i < allInstances.size(); i++) {
					if (allInstances.get(i).assignedCluster == clusterIndex
							&& !allInstances.get(i).equals(medoids[clusterIndex])) {
						sum += Math.abs(
								allInstances.get(i).finalDifferenceTime - medoids[clusterIndex].finalDifferenceTime);
						cnt++;
						noItemInTheCluster = false;
					}
					if (allInstances.get(i).equals(medoids[clusterIndex])) {
						noItemInTheCluster = false;
					}
				}
				if (noItemInTheCluster) {
					System.err.println("clusterIndex doesnt have any item: " + clusterIndex);
				}

				if (cnt != 0)
					externalDistanceMap.put(clusterIndex, sum / cnt);

			}

			System.out.println("exp1: internalDistanceMap - based on the feature vectors");
			double sumInternalDistance = 0;
			int cntInternal = 0;
			for (Integer clusterIndex : internalDistanceMap.keySet()) {
				sumInternalDistance += internalDistanceMap.get(clusterIndex);
				cntInternal++;
				System.out.println(clusterIndex + ";" + internalDistanceMap.get(clusterIndex));
			}

			if ((sumInternalDistance / cntInternal) < bestInternalDistance) {
				bestInternalDistance = (sumInternalDistance / cntInternal);
				System.out.println("bestInternalDistance so far is: " + bestInternalDistance);
			} else {
				System.out.println("InternalDistance so far is: " + (sumInternalDistance / cntInternal));
			}

			double sumExternalDistance = 0;
			int cntExternal = 0;
			System.out.println("exp1: externalDistanceMap - based on the running time");
			for (Integer clusterIndex : externalDistanceMap.keySet()) {
				sumExternalDistance += externalDistanceMap.get(clusterIndex);
				cntExternal++;

				System.out.println(clusterIndex + ";" + externalDistanceMap.get(clusterIndex));
			}

			if ((sumExternalDistance / cntExternal) < bestExternalDistance) {
				bestExternalDistance = (sumExternalDistance / cntExternal);
				System.out.println("bestExternalDistance so far is: " + bestExternalDistance);
			} else {
				System.out.println("ExternalDistance so far is: " + (sumExternalDistance / cntExternal));
			}

			System.out.println();
		}
		System.out.println();
		System.out.println();
		// Experiment2: measuring the clusters by testInstances
		// for (int exp = 0; exp < numberOfSameExperiments; exp++) {
		// double trainingRatio = 0.8;
		// for (int i = 0; i < numberOfSameExperiments; i++) {
		// ArrayList<Instance> trainingInstances = new ArrayList<Instance>();
		// ArrayList<Instance> testingInstances = new ArrayList<Instance>();
		// Collections.shuffle(allInstances);
		//
		// for (int j = 0; j < Math.floor(allInstances.size() * trainingRatio);
		// j++) {
		// trainingInstances.add(allInstances.get(j));
		// }
		// for (int j = (int) (Math.floor(allInstances.size() * trainingRatio) +
		// 1); j < allInstances
		// .size(); j++) {
		// testingInstances.add(allInstances.get(j));
		// }
		//
		// Instance[] medoids = buildClusters(trainingInstances,
		// featuresImportance);
		// // trainingInstances has the assignments also.
		//
		// // find the closest medoid for each test instance
		// for (int t = 0; t < testingInstances.size(); t++) {
		// double bestDistance = getTotalDistance(testingInstances.get(t),
		// medoids[0], featuresImportance);
		// testingInstances.get(t).assignedCluster = 0;
		//
		// for (int m = 1; m < medoids.length; m++) {
		// double tempDistance = getTotalDistance(testingInstances.get(t),
		// medoids[m], featuresImportance);
		// if (tempDistance < bestDistance) {
		// bestDistance = tempDistance;
		// testingInstances.get(t).assignedCluster = m;
		// }
		// }
		// }
		//
		// }
		// }

	}

	public static void main(String[] args) throws Exception {
		int numberOfFeatures = 0;
		int numberOfClusters = 0;
		int numberOfSameExperiments = 1;
		int maxIterations = 0;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-numberOfFeatures")) {
				numberOfFeatures = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-numberOfClusters")) {
				numberOfClusters = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-numberOfSameExperiments")) {
				numberOfSameExperiments = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-maxIterations")) {
				maxIterations = Integer.parseInt(args[++i]);
			}

		}

		// number of features?!
		FeatureType[] featureTypes = new FeatureType[numberOfFeatures];
		featureTypes[0] = FeatureType.GraphDistance;
		for (int i = 1; i < numberOfFeatures; i++) {
			featureTypes[i] = FeatureType.Double;
		}

		// featuresImportance
		double[] featuresImportance = new double[numberOfFeatures];
		// featuresImportance[0] = 1; // editDistance
		// featuresImportance[1] = 0; // primValue
		// featuresImportance[2] = 1; // numberOfStars
		// featuresImportance[3] = 1; // numberOfJoinableNodes
		 featuresImportance[4] = 0.5; // avgFirstPQItemSize
		 featuresImportance[5] = 0.5;// avgParialAnswersEstimate

//		for (int i = 0; i < numberOfFeatures; i++) {
//			featuresImportance[i] = ((double) 1) / ((double) numberOfFeatures);
//		}
		//featuresImportance[1] = 0; // primValue

		KMedoids kMedoids = new KMedoids(numberOfFeatures, numberOfClusters, featureTypes, maxIterations);
		kMedoids.initialize(numberOfSameExperiments, featuresImportance);
	}

	public Double getTotalDistance(Instance instance, Instance medoidInstance, double[] featuresImportance) {
		double totalDistance = 0;
		for (int fi = 0; fi < numberOfFeatures; fi++) {
			double tempValue = Double.MAX_VALUE;
			if (featuresImportance[fi] != 0) {
				if (featureTypes[fi] == FeatureType.Double)
					tempValue = getDistance(fi, instance.features[fi], medoidInstance.features[fi]);
				else if (featureTypes[fi] == FeatureType.GraphDistance)
					tempValue = getGraphDistance(instance, medoidInstance);

				totalDistance += featuresImportance[fi] * tempValue;
			}
		}

		if (totalDistance == Double.NaN) {
			System.err.println("total distance is " + totalDistance);
		}
		return totalDistance;
	}

	private double getGraphDistance(Instance instance, Instance medoidInstance) {
		if (instance.smallGraphType == medoidInstance.smallGraphType) {
			return 0;
		} else {
			int editDistance = editDistanceMap
					.get(instance.smallGraphType.toString() + "_" + medoidInstance.smallGraphType.toString());

			if (editDistance == 0) {
				System.err.println("two distinct graphs are similar?! " + instance.smallGraphType + "; "
						+ medoidInstance.smallGraphType);
			}

			return ((double) editDistance / (double) maxEditDistance);

		}
	}

	public Double getDistance(int attributeIndex, double instanceFeature, double centroidFeature) {
		// literal code
		double distance = Math.pow(instanceFeature - centroidFeature, 2);
		if (distance == 0) {
			return 0d;
		}
		return (distance / maxValueOfThisFeature[attributeIndex]);

	}

	private int[] assign(Instance[] medoids, ArrayList<Instance> instances, double[] featuresImportance) {
		int[] out = new int[instances.size()];
		for (int i = 0; i < instances.size(); i++) {
			double bestDistance = getTotalDistance(instances.get(i), medoids[0], featuresImportance);
			int bestIndex = 0;
			for (int j = 1; j < medoids.length; j++) {
				double tmpDistance = getTotalDistance(instances.get(i), medoids[j], featuresImportance);
				if (tmpDistance < bestDistance) {
					bestDistance = tmpDistance;
					bestIndex = j;
				}
			}
			out[i] = bestIndex;
			instances.get(i).assignedCluster = bestIndex;

		}
		return out;

	}

}

class Instance {
	// queryFileName;queryIndex;primValue;numberOfStars;sumJoinableNodes;avgFirstPQItemSize;avgParialAnswersEstimate;finalDifferenceTime;

	// instance id
	public int queryIndex;

	// 0
	public String queryFileName;
	KMedoids.SmallGraphType smallGraphType;

	// 1
	public int primValue;
	// 2
	public int numberOfStars;
	// 3
	public int sumJoinableNodes;
	// 4
	public int avgFirstPQItemSize;
	// 5
	public int avgParialAnswersEstimate;

	// computed
	double[] features;

	// output
	public int finalDifferenceTime;
	public int assignedCluster = -1;

	// 0 1 2 3 4 5 6 7
	// queryFileName;queryIndex;primValue;numberOfStars;sumJoinableNodes;avgFirstPQItemSize;avgParialAnswersEstimate;finalDifferenceTime;
	public Instance(String queryFileName, int queryIndex, int primValue, int numberOfStars, int sumJoinableNodes,
			int avgFirstPQItemSize, int avgParialAnswersEstimate, int finalDifferenceTime) {
		this.queryFileName = queryFileName;
		this.queryIndex = queryIndex;
		this.primValue = primValue;
		this.numberOfStars = numberOfStars;
		this.sumJoinableNodes = sumJoinableNodes;
		this.avgFirstPQItemSize = avgFirstPQItemSize;
		this.avgParialAnswersEstimate = avgParialAnswersEstimate;
		this.finalDifferenceTime = finalDifferenceTime;

		smallGraphType = SmallGraphType.valueOf(queryFileName);

		// computing the features:
		features = new double[6];
		features[0] = 0; // smallGraphType
		features[1] = primValue; // primValue
		features[2] = numberOfStars; // numberOfStars
		features[3] = sumJoinableNodes; // sumJoinableNodes
		features[4] = avgFirstPQItemSize; // avgFirstPQItemSize
		features[5] = avgParialAnswersEstimate; // avgParialAnswersEstimate

	}
}
