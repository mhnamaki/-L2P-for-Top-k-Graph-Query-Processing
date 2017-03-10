package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import weka.associations.tertius.IndividualInstance;
import weka.clusterers.SimpleKMeans;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SparseInstance;
import weka.experiment.DensityBasedClustererSplitEvaluator;

public class WekaKMeans {

	static int numberOfClusters = 2;

	public WekaKMeans(int numberOfClusters) {
		this.numberOfClusters = numberOfClusters;
	}

	public static void main(String[] args) throws Exception {
		SimpleKMeans kmeans = new SimpleKMeans();

		kmeans.setSeed(10);

		// important parameter to set: preserver order, number of cluster.
		kmeans.setPreserveInstancesOrder(true);
		kmeans.setNumClusters(numberOfClusters);

		FastVector atts = new FastVector();
		List<Instance> instances = new ArrayList<Instance>();
		for (int dim = 0; dim < 2; dim++) {
			Attribute current = new Attribute("Attribute" + dim, dim);
			if (dim == 0) {
				for (int obj = 0; obj < 10; obj++) {
					instances.add(new SparseInstance(2));
				}
			}

			for (int obj = 0; obj < 10; obj++) {
				if ((obj % 2) == 1) {
					instances.get(obj).setValue(0, 1);
					instances.get(obj).setValue(1, 1);
				} else {
					instances.get(obj).setValue(0, 0);
					instances.get(obj).setValue(1, 0);
				}
			}

			atts.addElement(current);
		}

		Instances newDataset = new Instances("Dataset", atts, instances.size());

		for (Instance inst : instances)
			newDataset.add(inst);

		kmeans.buildClusterer(newDataset);
		
		//kmeans

		// This array returns the cluster number (starting with 0) for each
		// instance
		// The array has as many elements as the number of instances
		int[] assignments = kmeans.getAssignments();

		int i = 0;
		for (int clusterNum : assignments) {
			System.out.printf("Instance %d -> Cluster %d \n", i, clusterNum);
			i++;
		}
	}

}
