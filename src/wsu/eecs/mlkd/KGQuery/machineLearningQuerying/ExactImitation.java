package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import scala.Array;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.REPTree;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;

public class ExactImitation {

	public int foldCount;
	public String dataFileClassifier;
	public String dataFileRegressor;
	public String dataFoldFile = "folds.txt";
	public String classifierName = "decisionTree";
	public String regressorName = "regressor";
	public Instances instanceClassifier;
	public Instances instanceRegressor;
	public Classifier classifier;
	public Classifier regressor;
	public Set<Integer> queryIndexSet;
	public Map<Integer, HashSet<Integer>> queryMapbyFold;
	public CommonFunctions commonFunctions = new CommonFunctions();

	// arguments -dataClassifier classification.arff -dataRegressor
	// regression.arff -splitRatio .6 -classifierName DecisionTree
	// -regressorName regressor
	public static void main(String[] args) {
		new ExactImitation(args);
	}

	public ExactImitation(String[] args) {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-dataClassifier")) {
				dataFileClassifier = args[++i];
			} else if (args[i].equals("-dataRegressor")) {
				dataFileRegressor = args[++i];
			} // else if (args[i].equals("-splitRatio")) {
				// splitRatio = Double.parseDouble(args[++i]);//}
			else if (args[i].equals("-classifier")) {
				classifierName = args[++i];
			} else if (args[i].equals("-regressor")) {
				regressorName = args[++i];
			} else if (args[i].equals("-foldCount")) {
				foldCount = Integer.parseInt(args[++i]);
			}
		}

		/* some examples for using the class */

		try {
			ExactImitation exactImitation = new ExactImitation("paSelectionFeaturesFor_3.txt",
					"paExpansionFeaturesFor_3.txt");
			// exactImitation.learnClassifier(exactImitation.instanceClassifier,
			// 1, 8);// will
			// return
			// a
			// classifier
			// exactImitation.learnRegressor(exactImitation.instanceRegressor,
			// 7, 10); // will
			// retunr
			// a
			// regressor
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		// evaluateModel("training-selection-split-by-query.arff",
		// "testing-selection-split-by-query.arff", classifier);
		// evaluateModel("training-expansion-split-by-query.arff",
		// "testing-expansion-split-by-query.arff", regressor);
	}

	private void PrepareALL() {
		InstantiateClassifier(classifierName);
		InstantiateRegressor(regressorName);
		this.instanceClassifier = getInstances(dataFileClassifier);
		// trainInstanceClassifier = getInstances(dataFileClassifier);
		// testInstanceClassifier = getInstances(dataFileClassifier);

		this.instanceRegressor = getInstances(dataFileRegressor);
		// trainInstanceRegressor = getInstances(dataFileRegressor);
		// testInstanceRegressor = getInstances(dataFileRegressor);

		splitbyQueryFoldIndex(instanceClassifier);
		// trainClassifierWithSplittedQuery(classifierInstances);
		// trainRegressorWithSplittedQuery(regressorInstances);

	}

	public ExactImitation() {
	}

	/*
	 * we assumned the fold count as 10 the fold text file is in the format
	 * (queryId, assignedFold)
	 */
	public ExactImitation(String dataFileClassifier, String dataFileRegressor) throws Exception {

		this.dataFileClassifier = ConvertClassifierFiletoArff("classification-converted.arff", dataFileClassifier,
				"selection-some108-header.txt");
		;
		this.dataFileRegressor = ConvertClassifierFiletoArff("regression-converted.arff", dataFileRegressor,
				"expansion-some49-header.txt");
		;
		this.queryMapbyFold = new HashMap<Integer, HashSet<Integer>>();
		this.foldCount = 10;

		PrepareALL();
		SaveModels();
	}

	private String ConvertClassifierFiletoArff(String arffFileName, String classifierFile, String headerFile) {
		try {
			BufferedReader brHeader = new BufferedReader(new FileReader(headerFile));
			BufferedReader brData = new BufferedReader(new FileReader(classifierFile));
			BufferedWriter bw = new BufferedWriter(new FileWriter(arffFileName));
			String line = "";
			while ((line = brHeader.readLine()) != null) {
				bw.write(line + "\n");
			}
			brHeader.close();
			brData.readLine();// for the column labels
			while ((line = brData.readLine()) != null) {
				bw.write(line + "\n");
			}
			brData.close();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return arffFileName;
	}

	private void SaveModels() throws Exception {
		weka.core.SerializationHelper.write("classifier.model", classifier);
		weka.core.SerializationHelper.write("regressor.model", regressor);

	}

	public void LoadClassifier() throws Exception {
		this.classifier = (Classifier) weka.core.SerializationHelper.read("classifier.model");
	}

	public void LoadRegressor() throws Exception {
		this.regressor = (REPTree) weka.core.SerializationHelper.read("regressor.model");
	}

	public Instances getInstances(String file) {
		if (!file.contains(".arff") || file.isEmpty()) {
			System.out.println("file name problem or file is empty");
			return null;
		}
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			Instances data = new Instances(reader);
			reader.close();
			// setting class attribute
			data.setClassIndex(data.numAttributes() - 1);
			return data;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private void trainClassifierWithSplittedQuery(Instances instances, int foldStart, int foldEnd, int numberOfTrees,
			int maxDepth, boolean isCostSensitive, double[] selectionNormalizationFeaturesVector) {

		Instances trainInstanceClassifier = getInstances(dataFileClassifier);
		trainInstanceClassifier.delete();
		HashSet<Integer> setOfQueryIndexByFold = new HashSet<>();

		for (int i = foldStart; i <= foldEnd; ++i) {
			HashSet<Integer> tempSet = queryMapbyFold.get(i);
			setOfQueryIndexByFold.addAll(tempSet);
		}

		for (int i = 0; i < instances.numInstances(); ++i) {
			Instance instance = (Instance) instances.instance(i).copy();
			int queryIndex = (int) instance.value(0);
			// instance.deleteAttributeAt(0);
			if (setOfQueryIndexByFold.contains(queryIndex)) {
				trainInstanceClassifier.add(instance);
				;
			}
		}
		try {
			// todo: remove query index before learning
			printToArff("training-selection-split-by-query.arff", trainInstanceClassifier);
			printQueryIndex("train-selection-queryIndex", foldStart, foldEnd);
			this.classifier = commonFunctions.learnRFClassifier(
					getInstancesWithOutQueryIndex(trainInstanceClassifier, selectionNormalizationFeaturesVector),
					numberOfTrees, maxDepth, isCostSensitive);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Classifier learnClassifier(Instances instances, int foldStart, int foldEnd, int numberOfTrees, int maxDepth,
			boolean isCostSensitive, double[] selectionNormalizationFeaturesVector) {

		if (foldStart <= 0) {
			System.out.println("fold Start from 1, setting startFold to 1");
			foldStart = 1;

		}
		if (foldEnd > 10) {
			System.out.println("now we have 10 folds, flodEnd set to 10");
			foldEnd = 10;
		}
		trainClassifierWithSplittedQuery(instances, foldStart, foldEnd, numberOfTrees, maxDepth, isCostSensitive,
				selectionNormalizationFeaturesVector);
		return this.classifier;
	}

	private void trainRegressorWithSplittedQuery(Instances instances, int foldStart, int foldEnd,
			double[] expansionNormalizationFeaturesVector) {
		Instances trainInstanceRegressor = getInstances(dataFileRegressor);
		trainInstanceRegressor.delete();
		HashSet<Integer> setOfQueryIndexByFold = new HashSet<>();

		for (int i = foldStart; i <= foldEnd; ++i) {
			HashSet<Integer> tempSet = queryMapbyFold.get(i);
			setOfQueryIndexByFold.addAll(tempSet);
		}

		for (int i = 0; i < instances.numInstances(); ++i) {
			Instance instance = (Instance) instances.instance(i).copy();
			int queryIndex = (int) instance.value(0);

			if (setOfQueryIndexByFold.contains(queryIndex)) {
				trainInstanceRegressor.add(instance);
			}
		}
		try {
			printToArff("training-expansion-split-by-query.arff", trainInstanceRegressor);
			printQueryIndex("train-expansion-queryIndex", foldStart, foldEnd);
			this.regressor = commonFunctions.learnRegressor(
					getInstancesWithOutQueryIndex(trainInstanceRegressor, expansionNormalizationFeaturesVector));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Classifier learnRegressor(Instances instances, int foldStart, int foldEnd,
			double[] expansionNormalizationFeaturesVector) {
		if (foldStart <= 0) {
			System.out.println("fold Start from 1, setting startFold to 1");
			foldStart = 1;

		}
		if (foldEnd > 10) {
			System.out.println("now we have 10 folds, flodEnd set to 10");
			foldEnd = 10;
		}
		trainRegressorWithSplittedQuery(instances, foldStart, foldEnd, expansionNormalizationFeaturesVector);
		return this.regressor;
	}

	public void splitbyQueryFoldIndex(Instances instances) {

		int noOfQueries = -1;
		List<Integer> listIndex = null;
		queryIndexSet = new HashSet<>();

		for (int i = 0; i < instances.numInstances(); ++i) {
			Instance instance = (Instance) instances.instance(i);
			int queryIndex = (int) instance.value(0);
			queryIndexSet.add(queryIndex);
		}
		noOfQueries = queryIndexSet.size();
		listIndex = new ArrayList<>(queryIndexSet);
		Collections.sort(listIndex);
		// long seed = System.nanoTime();
		// Collections.shuffle(listIndex, new Random(seed));
		// Collections.shuffle(listIndex, new Random(seed));

		// write to file and create the map of query index by fold
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(dataFoldFile));
			for (int i = 0; i < noOfQueries; ++i) {
				int fold = (i % foldCount) + 1;
				bw.write(fold + " " + listIndex.get(i) + "\n");
				HashSet<Integer> qIndexforFold = queryMapbyFold.get(fold);
				if (qIndexforFold == null) {
					qIndexforFold = new HashSet<>();
				}
				qIndexforFold.add(listIndex.get(i));
				queryMapbyFold.put(fold, qIndexforFold);
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// independent print by query index format it (foldIndex, queryIndex)
	public void printQueryIndex(String string, int foldStart, int foldEnd) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(string));
			for (int i = foldStart; i <= foldEnd; ++i) {
				HashSet<Integer> tempSet = queryMapbyFold.get(i);
				for (int index : tempSet) {
					bw.write(i + ";" + index + "\n");
				}
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void printToArff(String string, Instances instancesWithOutQueryIndex) {
		Instances dataSet = instancesWithOutQueryIndex;
		ArffSaver saver = new ArffSaver();
		saver.setInstances(dataSet);
		try {
			saver.setFile(new File(string));
			saver.writeBatch();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public Instances getInstancesWithOutQueryIndex(Instances instances, double[] normalizationFeaturesVector) {
		Instances trainWithOutQueryIndex = null;
		int[] indexList = new int[1];
		indexList[0] = 0;
		Remove remove = new Remove();
		remove.setAttributeIndicesArray(indexList);
		remove.setInvertSelection(new Boolean(false));
		try {
			remove.setInputFormat(instances);
			trainWithOutQueryIndex = Filter.useFilter(instances, remove);
			// trainWithOutQueryIndex.setClassIndex(trainWithOutQueryIndex.numAttributes()-1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// TODO: make sure that it doesn't change the output label
		if (normalizationFeaturesVector != null) {
			for (int index = 0; index < trainWithOutQueryIndex.numInstances(); index++) {
				for (int attIndex = 0; attIndex < trainWithOutQueryIndex.numAttributes() - 1; attIndex++) {
					double newValue = trainWithOutQueryIndex.instance(index).value(attIndex)
							/ normalizationFeaturesVector[attIndex];
					trainWithOutQueryIndex.instance(index).setValue(attIndex, newValue);
				}
			}
		}

		return trainWithOutQueryIndex;
	}

	private void InstantiateClassifier(String classifierName) {
		if (classifierName.toLowerCase().contains("decisiontree") || classifierName.toLowerCase().contains("decision")
				|| classifierName.toLowerCase().contains("dt")) {
			this.classifier = new J48();
		}
	}

	private void InstantiateRegressor(String regressorName) {
		if (regressorName.toLowerCase().contains("regressiontree") || regressorName.toLowerCase().contains("regression")
				|| regressorName.toLowerCase().contains("regtree") || regressorName.toLowerCase().contains("reg")
				|| regressorName.toLowerCase().contains("rt")) {
			this.regressor = new REPTree();
		}
	}

	public Classifier trainDecisionTreeClassifier(String arffFile) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(arffFile));
			Instances data = new Instances(reader);
			reader.close();

			// setting class attribute
			data.setClassIndex(data.numAttributes() - 1);

			Classifier decisionTree = new J48();
			decisionTree.buildClassifier(data);

			return decisionTree;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public Classifier getClassifier() {
		return this.classifier;
	}

	public Classifier getRegressor() {
		return this.regressor;
	}

	public void loadModels() throws Exception {
		this.classifier = (Classifier) weka.core.SerializationHelper.read("classifier.model");
		this.regressor = (REPTree) weka.core.SerializationHelper.read("regressor.model");

	}

	public Classifier getModel(String modelFilePath) throws Exception {
		return (Classifier) weka.core.SerializationHelper.read(modelFilePath);
	}

	public void evaluateModel(String trainFile, String testFile, Classifier classifier) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(trainFile));
			Instances dataTrain = new Instances(reader);
			dataTrain.setClassIndex(dataTrain.numAttributes() - 1);
			reader.close();

			reader = new BufferedReader(new FileReader(testFile));
			Instances dataTest = new Instances(reader);
			dataTest.setClassIndex(dataTest.numAttributes() - 1);
			reader.close();

			Evaluation eval = new Evaluation(dataTrain);
			eval.evaluateModel(classifier, dataTest);
			System.out.println(eval.toSummaryString("\nResults\n======\n", false));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void SaveModelSetName(String fileName, Classifier classifier) throws Exception {
		weka.core.SerializationHelper.write(fileName + ".model", classifier);
	}

}
