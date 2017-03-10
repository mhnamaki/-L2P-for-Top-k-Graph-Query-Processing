package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.neo4j.graphdb.GraphDatabaseService;

import biz.k11i.xgboost.util.FVec;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoostError;
import weka.classifiers.Classifier;
import weka.classifiers.CostMatrix;
import weka.classifiers.meta.MetaCost;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.REPTree;
import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;

public class CommonFunctions {
	public FastVector getWekaAttributesForCreatingANewClassificationInstance(int allCalcNodeSize, FastVector fvClassVal,
			int maxNumberOfStars, int maxNumberOfCalcNodes) {
		// we should keep all of this work somewhere and just add new values
		// there.
		// int numberOfStars = sqIndices.size();

		ArrayList<Attribute> attributes = new ArrayList<Attribute>();

		// base static features
		// attributes.add(new Attribute("queryIndex"));

		attributes.add(new Attribute("nodes"));
		attributes.add(new Attribute("edges"));
		attributes.add(new Attribute("stars"));
		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("nodesInStar" + i));
		}
		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("avgPivotDegreeInDataGraph" + i));
		}

		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("estimatedPA" + i));
		}
		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("firstPQItemSize" + i));
		}
		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("possiblePivots" + i));
		}
		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("joinableNodes" + i));
		}

		// selectionFeatures

		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("pqCurrent" + i));
		}

		for (int i = 1; i <= maxNumberOfCalcNodes; i++) {
			attributes.add(new Attribute("ubCurrent" + i));
		}

		attributes.add(new Attribute("lbCurrent"));

		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("pqDiffThisFromParent" + i));
		}
		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("pqDiffThisFromRoot" + i));
		}

		// for (int i = 1; i <= maxNumberOfStars; i++) {
		// attributes.add(new Attribute("generateNextBestMatchQueued" + i));
		// }

		for (int i = 1; i <= maxNumberOfCalcNodes; i++) {
			attributes.add(new Attribute("ubDifferenceFromCurrentLB" + i));
		}
		for (int i = 1; i <= maxNumberOfCalcNodes; i++) {
			attributes.add(new Attribute("ubDifferenceFromParentUB" + i));
		}
		for (int i = 1; i <= maxNumberOfCalcNodes; i++) {
			attributes.add(new Attribute("ubDifferenceFromRootUB" + i));
		}

		attributes.add(new Attribute("lbDifferenceFromRootLB"));
		attributes.add(new Attribute("lbDifferenceFromParentLB"));

		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("howManyTimesSelectedBefore" + i));
		}

		// for (int i = 1; i <= numberOfStars; i++) {
		// attributes.add(new
		// Attribute("contributionToCurrentAnswer" + i));
		// }

		// for (int i = 1; i <= numberOfStars; i++) {
		// attributes.add(new Attribute("sqCalcTreeDepth" + i));
		// }

		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("currentDepth" + i));
		}

		FastVector fvTrueFalse = new FastVector(2);
		fvTrueFalse.addElement("false");
		fvTrueFalse.addElement("true");
		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("isStarkIsEnough" + i, fvTrueFalse));
		}

		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("remainingPA" + i));
		}

		attributes.add(new Attribute("previousPASelected"));
		attributes.add(new Attribute("starQuerySelectedIndex", fvClassVal));

		FastVector fvWekaAttributes = new FastVector(attributes.size());
		for (int i = 0; i < attributes.size(); i++) {
			fvWekaAttributes.addElement(attributes.get(i));
		}

		// for (int i = 0; i < fvWekaAttributes.size(); i++) {
		// System.out.println(i + ", " +
		// fvWekaAttributes.elementAt(i).toString() + " : "
		// + ((Attribute) fvWekaAttributes.elementAt(i)).isNominal());
		// }

		return fvWekaAttributes;
	}

	public FastVector getWekaAttributesForCreatingANewRegressionInstance(FastVector fvClassVal, int maxNumberOfStars,
			int maxNumberOfCalcNodes) {
		// we should keep all of this work somewhere and just add new values
		// there.
		// int numberOfStars = sqIndices.size();
		// int numberOfStars = DummyProperties.maxNumberOfSQ;

		ArrayList<Attribute> attributes = new ArrayList<Attribute>();

		// base static features
		// attributes.add(new Attribute("queryIndex"));
		attributes.add(new Attribute("nodes"));
		attributes.add(new Attribute("edges"));
		attributes.add(new Attribute("stars"));
		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("nodesInStar" + i));
		}
		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("avgPivotDegreeInDataGraph" + i));
		}

		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("estimatedPA" + i));
		}
		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("firstPQItemSize" + i));
		}
		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("possiblePivots" + i));
		}

		for (int i = 1; i <= maxNumberOfStars; i++) {
			attributes.add(new Attribute("joinableNodes" + i));
		}

		attributes.add(new Attribute("currentPQ"));
		attributes.add(new Attribute("currentThisLB"));
		attributes.add(new Attribute("currentThisUB"));
		attributes.add(new Attribute("currentParentUB"));

		// attributes.add(new Attribute("pqDiffThisFromParent"));
		// attributes.add(new Attribute("pqDiffThisFromRoot"));
		// attributes.add(new Attribute("generateNextBestMatchQueued"));
		attributes.add(new Attribute("ubDifferenceFromCurrentLB"));
		attributes.add(new Attribute("ubDifferenceFromParentUB"));
		attributes.add(new Attribute("ubDifferenceFromRootUB"));

		attributes.add(new Attribute("lbDifferenceFromRootLB"));
		attributes.add(new Attribute("lbDifferenceFromParentLB"));

		// attributes.add(new Attribute("howManyTimesSelectedBefore"));

		attributes.add(new Attribute("currentDepth"));

		FastVector fvTrueFalse = new FastVector(2);
		fvTrueFalse.addElement("false");
		fvTrueFalse.addElement("true");
		attributes.add(new Attribute("isStarkIsEnough", fvTrueFalse));

		attributes.add(new Attribute("remainingPA"));
		attributes.add(new Attribute("searchLevel"));
		// attributes.add(new Attribute("diffMaxPossibleRankCurrentRank"));

		FastVector fvTrueFalse2 = new FastVector(2);
		fvTrueFalse2.addElement("false");
		fvTrueFalse2.addElement("true");
		attributes.add(new Attribute("isPreviouslySelected", fvTrueFalse2));

		// attributes.add(new Attribute("maxUB"));
		// attributes.add(new Attribute("currentRank"));

		FastVector fvPrevPaSelected = new FastVector(maxNumberOfStars + 1);
		for (int i = 0; i <= maxNumberOfStars; i++) {
			fvPrevPaSelected.addElement(String.valueOf(i));
		}
		attributes.add(new Attribute("previousPASelected", fvClassVal));

		FastVector fvPaSelected = new FastVector(maxNumberOfStars + 1);
		for (int i = 0; i <= maxNumberOfStars; i++) {
			fvPaSelected.addElement(String.valueOf(i));
		}
		// attributes.add(new Attribute("paSelected", fvClassVal));

		attributes.add(new Attribute("previousExpansionValue"));
		attributes.add(new Attribute("expandValue"));

		FastVector fvWekaAttributes = new FastVector(attributes.size());
		for (int i = 0; i < attributes.size(); i++) {
			fvWekaAttributes.addElement(attributes.get(i));
		}

		// for (int i = 0; i < fvWekaAttributes.size(); i++) {
		// System.out.println(i + ", " +
		// fvWekaAttributes.elementAt(i).toString() + " : "
		// + ((Attribute) fvWekaAttributes.elementAt(i)).isNominal());
		// }

		return fvWekaAttributes;
	}

	public Instances createClassificationTestingInstance(FastVector fvWekaAttributes, Features baseStaticFeatures,
			SelectionFeatures selectionFeatures, int maxNumberOfStars, double[] selectionNormalizationFeaturesVector) {
		Instances testingInstance = new Instances("SelectionRel", fvWekaAttributes, 1);

		// ? TODO:make sure it's correct
		testingInstance.setClassIndex(fvWekaAttributes.size() - 1);

		Instance iExample = new Instance(fvWekaAttributes.size());

		Object[] allFeatures = selectionFeatures.getSelectionFeaturesArray(
				baseStaticFeatures.getStaticFeaturesArray(maxNumberOfStars), maxNumberOfStars);

		for (int f = 0; f < (allFeatures.length - 1); f++) {
			Attribute att = (Attribute) fvWekaAttributes.elementAt(f);

			if (att.isNominal() || att.isString()) {
				// System.out.println("nominal or string: " + f + " : " +
				// allFeatures[f].toString() + "; " + att.name());
				iExample.setValue(att, allFeatures[f].toString());
			} else {
				// System.out.println("double: " + f + " : " +
				// allFeatures[f].toString() + "; " + att.name());
				iExample.setValue(att, Double.parseDouble(allFeatures[f].toString()));
			}

		}

		// add the instance
		// System.out.println("numAttributes: " + iExample.numAttributes());
		// System.out.println();

		if (selectionNormalizationFeaturesVector != null) {
			for (int attIndex = 0; attIndex < iExample.numAttributes() - 1; attIndex++) {
				double newValue = iExample.value(attIndex) / selectionNormalizationFeaturesVector[attIndex];
				iExample.setValue(attIndex, newValue);
			}
		}

		testingInstance.add(iExample);

		return testingInstance;
	}

	public Instances createRegressionTestingInstance(FastVector fvWekaRegressionAttributes, Features baseStaticFeatures,
			ExpansionFeatures expansionFeatures, int maxNumberOfStars, double[] expansionNormalizationFeaturesVector) {
		Instances testingInstance = new Instances("RegressionRel", fvWekaRegressionAttributes, 1);

		// ? TODO:make sure it's correct
		testingInstance.setClassIndex(fvWekaRegressionAttributes.size() - 1);

		Instance iExample = new Instance(fvWekaRegressionAttributes.size());

		Object[] allFeatures = expansionFeatures.getExpansionFeaturesArray(
				baseStaticFeatures.getStaticFeaturesArray(maxNumberOfStars), maxNumberOfStars);

		for (int f = 0; f < (allFeatures.length - 1); f++) {
			Attribute att = (Attribute) fvWekaRegressionAttributes.elementAt(f);

			if (att.isNominal() || att.isString()) {
				// System.out.println("regression: nominal or string: " + f + ":
				// " + allFeatures[f].toString());
				iExample.setValue(att, allFeatures[f].toString());
			} else {
				// System.out.println("regression: double: " + f + " : " +
				// allFeatures[f].toString());
				iExample.setValue(att, Double.parseDouble(allFeatures[f].toString()));
			}
		}

		if (expansionNormalizationFeaturesVector != null) {
			for (int attIndex = 0; attIndex < iExample.numAttributes() - 1; attIndex++) {
				double newValue = iExample.value(attIndex) / expansionNormalizationFeaturesVector[attIndex];
				iExample.setValue(attIndex, newValue);
			}
		}

		// add the instance
		// System.out.println("numAttributes: " + iExample.numAttributes());
		// System.out.println();
		testingInstance.add(iExample);

		return testingInstance;
	}

	public void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	public HashSet<Integer> readQueryIndexBasedOnFolds(String filePath, int startFrom, int endTo) throws Exception {
		HashSet<Integer> queries = new HashSet<Integer>();
		FileInputStream fis = new FileInputStream(filePath);

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		while ((line = br.readLine()) != null) {
			if (Integer.parseInt(line.split(" ")[0]) >= startFrom && Integer.parseInt(line.split(" ")[0]) <= endTo) {
				queries.add(Integer.parseInt(line.split(" ")[1]));
			}

		}
		br.close();

		return queries;
	}

	public Classifier learnRFClassifier(Instances examples, int numberOfTrees, int maxDepth, boolean isCostSensitive
	/* double[] selectionNormalizationFeaturesVector */) throws Exception {

		// if (selectionNormalizationFeaturesVector != null) {
		// for (int i = 0; i < examples.numInstances(); i++) {
		// for (int j = 0; j < examples.instance(i).numAttributes() - 1; j++) {
		// if (!examples.instance(i).attribute(j).isNominal()) {
		// double newValue = examples.instance(i).value(j) /
		// selectionNormalizationFeaturesVector[j];
		// if (newValue > 1) {
		// System.err.println("not normalized well! instance " + i + " and
		// attriute "
		// + examples.instance(i).attribute(j).name() + " with value " +
		// newValue);
		// }
		// examples.instance(i).setValue(j, newValue);
		// }
		// }
		// }
		// }

		examples.setClassIndex(examples.numAttributes() - 1);

		RandomForest rf = new RandomForest();
		rf.setNumTrees(numberOfTrees);
		rf.setMaxDepth(maxDepth);
		rf.setNumFeatures(examples.numAttributes() - 1);
		if (isCostSensitive) {
			CostMatrix myCostMatrix = new CostMatrix(new BufferedReader(new FileReader("costMatrixBoth.txt")));
			MetaCost metaCost = new MetaCost();
			metaCost.setClassifier(rf);
			metaCost.setCostMatrix(myCostMatrix);
			metaCost.buildClassifier(examples);
			return metaCost;
		} else {
			rf.buildClassifier(examples);
			return rf;
		}
	}

	private Classifier learnClassifier(Instances examples) throws Exception {
		examples.setClassIndex(examples.numAttributes() - 1);

		Classifier cls = new J48();
		cls.buildClassifier(examples);
		return cls;
	}

	// public int classifyInstance(Classifier classifier, Features
	// baseStaticFeatures, SelectionFeatures selectionFeatures,
	// FastVector fvWekaAttributes, HashMap<Integer, Integer> classValMap)
	// throws Exception {
	//
	// Instances testingInstance =
	// createClassificationTestingInstance(fvWekaAttributes, baseStaticFeatures,
	// selectionFeatures, DummyProperties.maxNumberOfSQ);
	//
	// return classValMap.get((int)
	// classifier.classifyInstance(testingInstance.firstInstance()));
	//
	// }

	public Classifier learnRegressor(
			Instances regressorsExamples/*
										 * , double[]
										 * expansionNormalizationFeaturesVector
										 */) throws Exception {

		// if (expansionNormalizationFeaturesVector != null) {
		// for (int i = 0; i < regressorsExamples.numInstances(); i++) {
		// for (int j = 0; j < regressorsExamples.instance(i).numAttributes() -
		// 1; j++) {
		// if (!regressorsExamples.instance(i).attribute(j).isNominal()) {
		// double newValue = regressorsExamples.instance(i).value(j)
		// / expansionNormalizationFeaturesVector[j];
		// if (newValue > 1) {
		// System.err.println("not normalized well! instance " + i + " and
		// attriute "
		// + regressorsExamples.instance(i).attribute(j).name() + " with value "
		// + newValue);
		// }
		// regressorsExamples.instance(i).setValue(j, newValue);
		// }
		// }
		// }
		// }

		// setting class attribute
		regressorsExamples.setClassIndex(regressorsExamples.numAttributes() - 1);

		Classifier cls = new REPTree();
		cls.buildClassifier(regressorsExamples);
		return cls;

		// RandomForest rf = new RandomForest();
		// rf.setNumTrees(10);
		// rf.setNumFeatures(15);
		// rf.buildClassifier(regressorsExamples);
		// return rf;

	}

	public static File[] fileInTheDirfinder(String dirName) {
		File dir = new File(dirName);

		File[] files = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String filename) {
				return filename.endsWith(".txt");
			}
		});

		if (files != null && files.length > 1)
			Arrays.sort(files);

		for (int i = 0; i < files.length; i++) {
			System.out.println("catched file " + i + "; " + files[i].getName());
		}
		return files;

	}

	public void setOracleSteps(String oracleSequenceFile, int queryIndex, ArrayList<Integer> oracleSelectionSteps,
			ArrayList<Integer> oracleExpansionSteps) throws Exception {
		ArrayList<Integer> selectionSteps = new ArrayList<Integer>();

		FileInputStream fis = new FileInputStream(oracleSequenceFile);

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		while ((line = br.readLine()) != null) {
			String[] splittedLine = line.split(";");
			if (splittedLine.length > 1) {
				if (Integer.parseInt(splittedLine[0]) == queryIndex) {
					String[] splittedSequence = splittedLine[1].replace("<", "").replace(">", "").split(",");
					if (splittedSequence.length > 1) {
						for (int i = 0; i < splittedSequence.length; i++) {
							if (splittedSequence[i].trim().length() > 0) {
								oracleSelectionSteps.add(Integer.parseInt(splittedSequence[i].split(":")[0].trim()));
								oracleExpansionSteps.add(Integer.parseInt(splittedSequence[i].split(":")[1].trim()));
							}

						}

					}

				}
			}
		}

		if (oracleSelectionSteps != null) {
			System.out.println("oracleSelectionSteps size: " + oracleSelectionSteps.size());
		}
		if (oracleExpansionSteps != null) {
			System.out.println("oracleExpansionSteps size: " + oracleExpansionSteps.size());
		}
		br.close();
	}

	public FVec createClassificationBoosterInstance(Features baseStaticFeatures, SelectionFeatures selectionFeatures,
			int maxNumberOfStars, float[] selectionNormalizationFeaturesVector) throws Exception {

		Object[] allFeatures = selectionFeatures.getSelectionFeaturesArray(
				baseStaticFeatures.getStaticFeaturesArray(maxNumberOfStars), maxNumberOfStars);

		// newing 108 items
		float[] testingInstance = new float[allFeatures.length - 1];

		for (int f = 0; f < 97; f++) {
			testingInstance[f] = Float.parseFloat(allFeatures[f].toString()) / selectionNormalizationFeaturesVector[f];
		}

		for (int f = 97; f < 102; f++) {
			if (allFeatures[f].toString().toLowerCase().equals("false")) {
				testingInstance[f] = 0;
			} else if (allFeatures[f].toString().toLowerCase().equals("true")) {
				testingInstance[f] = 1;
			}
		}

		for (int f = 102; f < testingInstance.length; f++) {
			testingInstance[f] = Float.parseFloat(allFeatures[f].toString()) / selectionNormalizationFeaturesVector[f];
		}

		// DMatrix testInstace = new DMatrix(testingInstance, 1,
		// testingInstance.length,
		// DummyProperties.DEFAULT_VALUE_FOR_MISSING);

		FVec testInstace = FVec.Transformer.fromArray(testingInstance,
				false /* treat zero element as N/A */);

		return testInstace;
	}

	public FVec createRegressionBoosterInstance(Features baseStaticFeatures, ExpansionFeatures expansionFeatures,
			int maxNumberOfStars, float[] expansionNormalizationFeaturesVector) throws Exception {

		Object[] allFeatures = expansionFeatures.getExpansionFeaturesArray(
				baseStaticFeatures.getStaticFeaturesArray(maxNumberOfStars), maxNumberOfStars);

		// newing 49 items
		float[] testingInstance = new float[allFeatures.length - 1];
		for (int f = 0; f < 43; f++) {
			testingInstance[f] = Float.parseFloat(allFeatures[f].toString()) / expansionNormalizationFeaturesVector[f];
		}

		if (allFeatures[43].toString().toLowerCase().equals("false")) {
			testingInstance[43] = 0;
		} else if (allFeatures[43].toString().toLowerCase().equals("true")) {
			testingInstance[43] = 1;
		}

		for (int f = 44; f < 46; f++) {
			testingInstance[f] = Float.parseFloat(allFeatures[f].toString()) / expansionNormalizationFeaturesVector[f];
		}

		if (allFeatures[46].toString().toLowerCase().equals("false")) {
			testingInstance[46] = 0;
		} else if (allFeatures[46].toString().toLowerCase().equals("true")) {
			testingInstance[46] = 1;
		}

		for (int f = 47; f < testingInstance.length; f++) {
			testingInstance[f] = Float.parseFloat(allFeatures[f].toString());
		}

		// TODO: removing this if esle if
		// because allfeatures has the output label also
		// for (int f = 0; f < (allFeatures.length - 1); f++) {
		// if (allFeatures[f].toString().toLowerCase().equals("false")) {
		// allFeatures[f] = "0";
		// } else if (allFeatures[f].toString().toLowerCase().equals("true")) {
		// allFeatures[f] = "1";
		// }
		// testingInstance[f] = Float.parseFloat(allFeatures[f].toString());
		//
		// }

		// DMatrix testInstace = new DMatrix(testingInstance, 1,
		// testingInstance.length,
		// DummyProperties.DEFAULT_VALUE_FOR_MISSING);

		FVec testInstace = FVec.Transformer.fromArray(testingInstance,
				false /* treat zero element as N/A */);
		return testInstace;
	}

}
