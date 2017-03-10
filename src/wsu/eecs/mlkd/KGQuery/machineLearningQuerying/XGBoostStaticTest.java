package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import ml.dmlc.xgboost4j.java.XGBoostError;
import weka.classifiers.Classifier;
import weka.core.Instances;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import biz.k11i.xgboost.Predictor;
import biz.k11i.xgboost.learner.ObjFunction;
import biz.k11i.xgboost.util.FVec;

public class XGBoostStaticTest {

	public static void main(String[] args) throws Exception {
		classificationStaticTest();

		// regressionStaticTest();

	}

	private static void regressionStaticTest() throws Exception {

		int numberOfFeatures = 49;

		String trainTestPath = "/Users/mnamaki/Documents/Education/PhD/Spring2016/Research/testi/newRegExmpls_0.txt";

		// load file from text file to learn
		DMatrix trainMat = new DMatrix(trainTestPath);

		HashMap<String, Object> paramsReg = new HashMap<String, Object>();
		paramsReg.put("eta", 1.0);
		paramsReg.put("booster", "gbtree");
		paramsReg.put("silent", 1);
		paramsReg.put("objective", "reg:linear");
		paramsReg.put("eval_metric", "rmse");
		paramsReg.put("max_depth", numberOfFeatures);

		HashMap<String, DMatrix> watchesReg = new HashMap<String, DMatrix>();
		watchesReg.put("regTrain", trainMat);

		int round = 3;

		Booster boosterRegressor = XGBoost.train(trainMat, paramsReg, round, watchesReg, null, null);

		int threshold = 10;
		printResultByStaticTestFileForRegression(boosterRegressor, trainTestPath, threshold);

		printResultByCreatingTestInstancesOnTheFlyForRegression(boosterRegressor, trainTestPath, numberOfFeatures,
				threshold);

	}

	private static void printResultByCreatingTestInstancesOnTheFlyForRegression(Booster boosterRegressor,
			String trainTestPath, int numberOfFeatures, int threshold) throws Exception {

		System.out.println("printResultByCreatingTestInstancesOnTheFly");

		// testing with create instances one by one from the same file.
		// for some of instances it generates different value than the filePath
		// approach!

		FileInputStream fis = new FileInputStream(trainTestPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		int trainErrCnt = 0;
		int testingInstancesCnt = 0;
		String line = null;
		while ((line = br.readLine()) != null) {

			// splitting lib-svm formatted file
			String[] splittedLine = line.split(" ");

			// initializing a data array to keep the features
			float[] testingInstance = new float[1 * numberOfFeatures];

			for (int f = 0; f < (splittedLine.length - 1); f++) {
				// the first item in splitted line is output label so we
				// start from one....
				testingInstance[f] = Float.parseFloat(splittedLine[f + 1].split(":")[1].toString());
			}

			// creating a DMatrix with one row and the numberOfFeatures
			// columns

			DMatrix testInstance = new DMatrix(testingInstance, 1, numberOfFeatures);

			// predict just one instance
			float[][] predicts = boosterRegressor.predict(testInstance);

			// the first splitted item in each row is output label
			float label = Float.parseFloat(splittedLine[0]);

			// assigning true label to Dmatrix
			float[] labels = new float[1];
			labels[0] = label;
			testInstance.setLabel(labels);

			if (((int) labels[0]) > ((int) predicts[0][0] + threshold)
					|| ((int) labels[0]) < ((int) predicts[0][0] - threshold)) {
				trainErrCnt++;
				System.out.println(label + ", " + predicts[0][0]);
			}

			testingInstancesCnt++;

		}
		System.out.println("trainErr: " + trainErrCnt);
		System.out.println("testCnt: " + testingInstancesCnt);
		br.close();

		System.out.println();

	}

	private static void printResultByStaticTestFileForRegression(Booster boosterRegressor, String trainTestPath,
			int threshold) throws Exception {
		System.out.println("printResultByStaticTestFile");

		// init DMatrix using filePath approach
		DMatrix testMat = new DMatrix(trainTestPath);

		// count the misclassifications
		int trainErrCnt = 0;

		// count the instances to be tested?
		int testInstancesCnt = 0;

		// getting labels from DMatrix directly (from lib-svm formatted
		// file)
		float[] labels = testMat.getLabel();

		// predict the whole of the test file
		float[][] predicts = boosterRegressor.predict(testMat);

		for (int i = 0; i < predicts.length; i++) {
			if (((int) labels[i]) > ((int) predicts[i][0] + threshold)
					|| ((int) labels[i]) < ((int) predicts[i][0] - threshold)) {
				trainErrCnt++;
				System.out.println(labels[i] + ", " + predicts[i][0]);
			}
			testInstancesCnt++;

		}

		System.out.println("trainErr: " + trainErrCnt);
		System.out.println("testCnt: " + testInstancesCnt);

		System.out.println();

	}

	private static void classificationStaticTest() throws Exception {
		int numberOfFeatures = 108;

		String trainTestPath = "/Users/mnamaki/Documents/Education/PhD/Spring2016/Research/testi/newClsExmpls_1.txt";
		/// writeInArffFile(trainTestPath);

		// load file from text file to learn
		DMatrix trainMat = new DMatrix(trainTestPath);

		System.out.println(trainTestPath);

		HashMap<String, Object> paramsCls = new HashMap<String, Object>();
		paramsCls.put("eta", 1.0);
		paramsCls.put("booster", "gbtree");
		paramsCls.put("silent", 1);
		paramsCls.put("objective", "multi:softmax");
		paramsCls.put("num_class", "6");
		paramsCls.put("eval_metric", "mlogloss");
		paramsCls.put("max_depth", numberOfFeatures);

		HashMap<String, DMatrix> watches = new HashMap<String, DMatrix>();
		watches.put("train", trainMat);

		// set round
		int round = 1;

		// train a boost model
		Booster booster = XGBoost.train(trainMat, paramsCls, round, watches, null, null);
		System.out.println("after training");
		String modelPath = "/Users/mnamaki/Documents/Education/PhD/Spring2016/Research/testi/model/model.model";
		booster.saveModel(new java.io.FileOutputStream(modelPath));
		System.out.println("after saving the model");
		Predictor predictor = new Predictor(new java.io.FileInputStream(modelPath));
		System.out.println("after loading the model");
		ObjFunction.useFastMathExp(true);
		int numberOfExperiments = 5;
		ArrayList<Double> times = new ArrayList<Double>();

		// for (int i = 0; i < numberOfExperiments; i++) {
		// double startTime = System.nanoTime();
		// printResultByStaticTestFile(booster, trainTestPath);
		// times.add((System.nanoTime() - startTime) / 1e6);
		// }
		//
		// System.out.println("static: " +
		// DummyFunctions.computeNonOutlierAverage(times, numberOfExperiments));
		// times.clear();

		// printResultByWritingInstanceThenReadFromThatFile(booster,
		// trainTestPath);

		// printResultByGettingInstanceSplitWriteThenReadFromThatFile(booster,
		// trainTestPath, numberOfFeatures);

		// printResultByCreatingCSCMatrixOnTheFly(booster, trainTestPath,
		// numberOfFeatures);

		for (int i = 0; i < numberOfExperiments; i++) {
			Double predictionTime = printResultByCreatingTestInstancesOnTheFly(booster, trainTestPath,
					numberOfFeatures);
			times.add(predictionTime);
		}
		System.out.println("dynamic XGBoost: " + DummyFunctions.computeNonOutlierAverage(times, numberOfExperiments));
		times.clear();

		for (int i = 0; i < numberOfExperiments; i++) {
			Double predictionTime = printResultByCreatingTestInstancesOnTheFlyNewPredictor(predictor, trainTestPath,
					numberOfFeatures);
			times.add(predictionTime);
		}
		System.out.println("dynamic XGBoost new prediction: "
				+ DummyFunctions.computeNonOutlierAverage(times, numberOfExperiments));
		times.clear();

		CommonFunctions cf = new CommonFunctions();

		Instances examples = getInstances(trainTestPath);

		Classifier classifier = cf.learnRFClassifier(examples, round, numberOfFeatures, false);

		System.out.println("after rf learning!");

		for (int i = 0; i < numberOfExperiments; i++) {
			Double predictionTime = printResultForRFByCreatingTestInstancesOnTheFly(classifier, examples);
			times.add(predictionTime);
		}
		System.out.println("dynamic RF: " + DummyFunctions.computeNonOutlierAverage(times, numberOfExperiments));
		times.clear();

	}

	private static Double printResultByCreatingTestInstancesOnTheFlyNewPredictor(Predictor predictor,
			String trainTestPath, int numberOfFeatures) throws Exception {

		Double predictionTime = 0.0d;

		System.out.println("printResultByCreatingTestInstancesOnTheFly NEW");

		// testing with create instances one by one from the same file.
		// for some of instances it generates different value than the filePath
		// approach!

		FileInputStream fis = new FileInputStream(trainTestPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		int trainErrCnt = 0;
		int testingInstancesCnt = 0;
		String line = null;
		while ((line = br.readLine()) != null) {

			// splitting lib-svm formatted file
			String[] splittedLine = line.split(" ");

			// initializing a data array to keep the features
			float[] testingInstance = new float[1 * numberOfFeatures];

			for (int f = 0; f < (splittedLine.length - 1); f++) {
				// the first item in splitted line is output label so we
				// start from one....
				testingInstance[f] = Float.parseFloat(splittedLine[f + 1].split(":")[1].toString());
			}

			// creating a DMatrix with one row and the numberOfFeatures
			// columns
			double startTime = System.nanoTime();
			// DMatrix testInstance = new DMatrix(testingInstance, 1,
			// numberOfFeatures, 9999F);

			FVec fVecDense = FVec.Transformer.fromArray(testingInstance,
					false /* treat zero element as N/A */);

			// predict just one instance
			double[] predicts = predictor.predict(fVecDense);
			;
			predictionTime += (System.nanoTime() - startTime) / 1e6;

			// the first splitted item in each row is output label
			float label = Float.parseFloat(splittedLine[0]);

			// assigning true label to Dmatrix
			// float[] labels = new float[1];
			// labels[0] = label;
			// testInstance.setLabel(labels);

			if (((int) label) != ((int) predicts[0])) {
				trainErrCnt++;
				// System.out.println(label + ", " + predicts[0][0]);
			}

			testingInstancesCnt++;

		}
		System.out.println("trainErr: " + trainErrCnt);
		System.out.println("testCnt: " + testingInstancesCnt);
		br.close();

		// System.out.println();

		return predictionTime;
	}

	private static Double printResultForRFByCreatingTestInstancesOnTheFly(Classifier classifier, Instances examples)
			throws Exception {

		Double predictionTime = 0.0d;
		for (int i = 0; i < examples.numInstances(); i++) {
			double startTime = System.nanoTime();

			double classVal = classifier.classifyInstance(examples.instance(i));

			predictionTime += (System.nanoTime() - startTime) / 1e6;

			if (classVal > 1) {
				int j = 0;
				startTime = j;
			}

		}

		return predictionTime;
	}

	private static Instances getInstances(String trainTestPath) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(trainTestPath + "_weka2.arff"));
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

	private static void writeInArffFile(String trainTestPath) throws Exception {
		FileInputStream fis = new FileInputStream(trainTestPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String line = null;

		FileOutputStream fos = new FileOutputStream(trainTestPath + "_weka3.arff");
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		while ((line = br.readLine()) != null) {
			String[] splittedLine = line.split(" ");

			for (int f = 0; f < (splittedLine.length - 1); f++) {
				bw.write(splittedLine[f + 1].split(":")[1].toString());

				bw.write(", ");

			}
			Double classValue = Double.parseDouble(splittedLine[0].trim());
			bw.write(String.valueOf(classValue.intValue()));
			bw.newLine();
		}

		bw.close();
		br.close();

	}

	private static void printResultByCreatingCSCMatrixOnTheFly(Booster booster, String trainTestPath,
			int numberOfFeatures) throws Exception {

		System.out.println("printResultByCreatingCSCMatrixOnTheFly");

		long[] colHeaders = new long[numberOfFeatures];
		int[] rowIndex = new int[numberOfFeatures];

		for (int i = 0; i < colHeaders.length; i++) {
			colHeaders[i] = i;
			rowIndex[i] = 0;
		}

		FileInputStream fis = new FileInputStream(trainTestPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		int trainErrCnt = 0;
		int testingInstancesCnt = 0;
		String line = null;
		while ((line = br.readLine()) != null) {

			// splitting lib-svm formatted file
			String[] splittedLine = line.split(" ");

			// initializing a data array to keep the features
			float[] testingInstance = new float[1 * numberOfFeatures];

			for (int f = 0; f < (splittedLine.length - 1); f++) {
				// the first item in splitted line is output label so we
				// start from one....
				testingInstance[f] = Float.parseFloat(splittedLine[f + 1].split(":")[1].toString());
			}

			// creating a DMatrix with one row and the numberOfFeatures
			// columns
			DMatrix testInstance = new DMatrix(colHeaders, rowIndex, testingInstance, DMatrix.SparseType.CSC);

			// predict just one instance
			float[][] predicts = booster.predict(testInstance);

			// the first splitted item in each row is output label
			float label = Float.parseFloat(splittedLine[0]);

			// assigning true label to Dmatrix
			float[] labels = new float[1];
			labels[0] = label;
			testInstance.setLabel(labels);

			if (((int) label) != ((int) predicts[0][0])) {
				trainErrCnt++;
			}

			testingInstancesCnt++;
			System.out.println(label + ", " + predicts[0][0]);

		}

		System.out.println("trainErr: " + trainErrCnt);
		System.out.println("testCnt: " + testingInstancesCnt);
		br.close();

		System.out.println();

	}

	private static void printResultByGettingInstanceSplitWriteThenReadFromThatFile(Booster booster,
			String trainTestPath, int numberOfFeatures) throws Exception {

		System.out.println("printResultByGettingInstanceSplitWriteThenReadFromThatFile");

		FileInputStream fis = new FileInputStream(trainTestPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		int trainErrCnt = 0;
		int testingInstancesCnt = 0;
		String line = null;
		while ((line = br.readLine()) != null) {

			// splitting lib-svm formatted file
			String[] splittedLine = line.split(" ");

			// initializing a data array to keep the features
			float[] testingInstance = new float[1 * numberOfFeatures];

			for (int f = 0; f < (splittedLine.length - 1); f++) {
				// the first item in splitted line is output label so we
				// start from one....
				testingInstance[f] = Float.parseFloat(splittedLine[f + 1].split(":")[1].toString());
			}

			// the first splitted item in each row is output label
			float label = Float.parseFloat(splittedLine[0]);

			FileOutputStream fos = new FileOutputStream("oneInstanceOut.txt");
			BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(fos));

			brOut.write(label + " ");
			for (int i = 0; i < testingInstance.length; i++) {
				brOut.write(i + ":" + testingInstance[i] + " ");
			}
			brOut.close();

			// creating a DMatrix with one row and the numberOfFeatures
			// columns
			DMatrix testInstance = new DMatrix("oneInstanceOut.txt");

			// predict just one instance
			float[][] predicts = booster.predict(testInstance);

			if (((int) label) != ((int) predicts[0][0])) {
				trainErrCnt++;
			}

			testingInstancesCnt++;
			System.out.println(label + ", " + predicts[0][0]);

		}
		System.out.println("trainErr: " + trainErrCnt);
		System.out.println("testCnt: " + testingInstancesCnt);
		br.close();

		System.out.println();

	}

	private static Double printResultByCreatingTestInstancesOnTheFly(Booster booster, String trainTestPath,
			int numberOfFeatures) throws Exception {

		Double predictionTime = 0.0d;

		System.out.println("printResultByCreatingTestInstancesOnTheFly");

		// testing with create instances one by one from the same file.
		// for some of instances it generates different value than the filePath
		// approach!

		FileInputStream fis = new FileInputStream(trainTestPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		int trainErrCnt = 0;
		int testingInstancesCnt = 0;
		String line = null;
		while ((line = br.readLine()) != null) {

			// splitting lib-svm formatted file
			String[] splittedLine = line.split(" ");

			// initializing a data array to keep the features
			float[] testingInstance = new float[1 * numberOfFeatures];

			for (int f = 0; f < (splittedLine.length - 1); f++) {
				// the first item in splitted line is output label so we
				// start from one....
				testingInstance[f] = Float.parseFloat(splittedLine[f + 1].split(":")[1].toString());
			}

			// creating a DMatrix with one row and the numberOfFeatures
			// columns
			double startTime = System.nanoTime();
			DMatrix testInstance = new DMatrix(testingInstance, 1, numberOfFeatures, 9999F);

			// predict just one instance
			float[][] predicts = booster.predict(testInstance);
			predictionTime += (System.nanoTime() - startTime) / 1e6;

			// the first splitted item in each row is output label
			float label = Float.parseFloat(splittedLine[0]);

			// assigning true label to Dmatrix
			float[] labels = new float[1];
			labels[0] = label;
			testInstance.setLabel(labels);

			if (((int) label) != ((int) predicts[0][0])) {
				trainErrCnt++;
				// System.out.println(label + ", " + predicts[0][0]);
			}

			testingInstancesCnt++;

		}
		System.out.println("trainErr: " + trainErrCnt);
		System.out.println("testCnt: " + testingInstancesCnt);
		br.close();

		// System.out.println();

		return predictionTime;

	}

	private static void printResultByCreatingTestInstancesOnTheFlyForNewVersion(Booster booster, String trainTestPath,
			int numberOfFeatures) throws Exception {
		System.out.println("printResultByCreatingTestInstancesOnTheFlyForNewVersion");

		// testing with create instances one by one from the same file.
		// for some of instances it generates different value than the filePath
		// approach!

		FileInputStream fis = new FileInputStream(trainTestPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		int trainErrCnt = 0;
		int testingInstancesCnt = 0;
		String line = null;
		while ((line = br.readLine()) != null) {

			// splitting lib-svm formatted file
			String[] splittedLine = line.split(" ");

			// initializing a data array to keep the features
			float[] testingInstance = new float[1 * numberOfFeatures];

			for (int f = 0; f < (splittedLine.length - 1); f++) {
				// the first item in splitted line is output label so we
				// start from one....
				testingInstance[f] = Float.parseFloat(splittedLine[f + 1].split(":")[1].toString());
			}

			// creating a DMatrix with one row and the numberOfFeatures
			// columns

			DMatrix testInstance = new DMatrix(testingInstance, 1, numberOfFeatures);

			// predict just one instance
			float[][] predicts = booster.predict(testInstance);

			// the first splitted item in each row is output label
			float label = Float.parseFloat(splittedLine[0]);

			// assigning true label to Dmatrix
			float[] labels = new float[1];
			labels[0] = label;
			testInstance.setLabel(labels);

			if (((int) label) != ((int) predicts[0][0])) {
				trainErrCnt++;
				System.out.println(label + ", " + predicts[0][0]);
			}

			testingInstancesCnt++;

		}
		System.out.println("trainErr: " + trainErrCnt);
		System.out.println("testCnt: " + testingInstancesCnt);
		br.close();

		System.out.println();

	}

	private static void printResultByWritingInstanceThenReadFromThatFile(Booster booster, String trainTestPath)
			throws Exception {

		System.out.println("printResultByWritingInstanceThenReadFromThatFile");

		FileInputStream fis = new FileInputStream(trainTestPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		int trainErrCnt = 0;
		int testingInstancesCnt = 0;
		String line = null;
		while ((line = br.readLine()) != null) {

			FileOutputStream fos = new FileOutputStream("oneInstanceOut.txt");
			BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(fos));

			brOut.write(line);
			brOut.close();

			// creating a DMatrix with one row and the numberOfFeatures
			// columns
			DMatrix testInstance = new DMatrix("oneInstanceOut.txt");

			// predict just one instance
			float[][] predicts = booster.predict(testInstance);

			float[] labels = testInstance.getLabel();

			if (((int) labels[0]) != ((int) predicts[0][0])) {
				trainErrCnt++;
			}

			testingInstancesCnt++;
			System.out.println(labels[0] + ", " + predicts[0][0]);

		}
		System.out.println("trainErr: " + trainErrCnt);
		System.out.println("testCnt: " + testingInstancesCnt);
		br.close();

		System.out.println();

	}

	private static void printResultByStaticTestFile(Booster booster, String trainTestPath) throws Exception {

		// System.out.println("printResultByStaticTestFile");

		// init DMatrix using filePath approach
		DMatrix testMat = new DMatrix(trainTestPath);

		// count the misclassifications
		int trainErrCnt = 0;

		// count the instances to be tested?
		int testInstancesCnt = 0;

		// getting labels from DMatrix directly (from lib-svm formatted
		// file)
		float[] labels = testMat.getLabel();

		// predict the whole of the test file
		float[][] predicts = booster.predict(testMat);

		for (int i = 0; i < predicts.length; i++) {
			if (((int) labels[i]) != ((int) predicts[i][0])) {
				trainErrCnt++;
				// System.out.println(labels[i] + ", " + predicts[i][0]);
			}
			testInstancesCnt++;

		}

		// System.out.println("trainErr: " + trainErrCnt);
		// System.out.println("testCnt: " + testInstancesCnt);

		// System.out.println();
	}

}

//////

// { // int numberOfLines = 6;
// FileInputStream fis = new FileInputStream(trainTestPath);
//
// // Construct BufferedReader from InputStreamReader
// BufferedReader br = new BufferedReader(new InputStreamReader(fis));
//
// int trainErr = 0;
// int testCnt = 0;
// String line = null;
// float[] testingInstance = new float[numberOfLines *
// numberOfFeatures];
// float[] labels = new float[numberOfLines];
// int lineCnt = 0;
// int testingInstanceCnt = 0;
// while ((line = br.readLine()) != null) {
// String[] splittedLine = line.split(" ");
// // System.out.println(splittedLine.length);
//
// for (int f = 0; f < (splittedLine.length - 1); f++) {
// testingInstance[testingInstanceCnt++] = Float
// .parseFloat(splittedLine[f + 1].split(":")[1].toString());
// }
// labels[lineCnt++] = Float.parseFloat(splittedLine[0]);
//
// }
//
// DMatrix testInstace = new DMatrix(testingInstance, numberOfLines,
// numberOfFeatures);
// testInstace.setLabel(labels);
//
// // predict
// float[][] predicts = booster2.predict(testInstace);
//
// for (int i = 0; i < predicts.length; i++) {
// if (labels[i] != predicts[i][0]) {
// trainErr++;
// }
// testCnt++;
// System.out.println(labels[i] + ", " + predicts[i][0]);
// }
// System.out.println("trainErr: " + trainErr);
// System.out.println("testCnt: " + testCnt);
// br.close();
//
// System.out.println("finish!");
// }
