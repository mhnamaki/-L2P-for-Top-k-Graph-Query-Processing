package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;

import java.io.InputStreamReader;

import java.util.HashMap;
import java.util.HashSet;

import org.neo4j.graphdb.GraphDatabaseService;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import wsu.eecs.mlkd.KGQuery.TopKQuery.CacheServer;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.NeighborIndexing;

import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;

import weka.classifiers.Classifier;

public class MLQueryRunnerOverValidation {

	private String MODELGRAPH_DB_PATH = "";
	private String PATTERNGRAPH_DB_PATH = "";

	private String queryFileName = "";
	private String queryFileDirectory = "";

	private String GName = ""; // Yago, DBPedia, ...

	private String queryDBInNeo4j = "query.db";
	private String GDirectory = "";
	private int numberOfSameExperiment = 1;
	private int k = 0;
	private GraphDatabaseService knowledgeGraph;
	private Levenshtein levenshtein;
	private NeighborIndexing neighborIndexingInstance;
	private CacheServer cacheServer;
	private int startingQueryIndex;
	private int endingQueryIndex = 1000000;
	private CommonFunctions commonFunctions = new CommonFunctions();
	private int validationFoldStartFrom;
	private int validationFoldEndTo;
	private String queriesFoldPath;
	private int maxNumberOfIteration;
	private int startingModelIndex = 0;

	final int SELECTION_FEATURES_SIZE = 108;
	final int EXPANSION_FEATURES_SIZE = 49;

	double[] selectionNormalizationFeaturesVector;
	double[] expansionNormalizationFeaturesVector;

	public MLQueryRunnerOverValidation(String queryFileDirectory, String GDirectory, String GName,
			int startingQueryIndex, int endingQueryIndex, String PATTERNGRAPH_DB_PATH, int k, Levenshtein levenshtein) {
		this.queryFileDirectory = queryFileDirectory;
		this.GDirectory = GDirectory;
		this.GName = GName;
		this.startingQueryIndex = startingQueryIndex;
		this.endingQueryIndex = endingQueryIndex;
		this.PATTERNGRAPH_DB_PATH = PATTERNGRAPH_DB_PATH;
		this.k = k;
		this.levenshtein = levenshtein;
	}

	public MLQueryRunnerOverValidation() {

	}

	public static void main(String[] args) throws Exception {
		MLQueryRunnerOverValidation mlqrOverValidation = new MLQueryRunnerOverValidation();
		mlqrOverValidation.initialize(args);
	}

	public void initialize(String[] args) throws Exception {
		int numberOfPrefixChars = 0;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-queryFileName")) {
				queryFileName = args[++i];
				queryFileName = queryFileName.replace(".txt", "");
			} else if (args[i].equals("-queryFileDirectory")) {
				queryFileDirectory = args[++i];
				if (!queryFileDirectory.endsWith("/") && !queryFileDirectory.equals("")) {
					queryFileDirectory += "/";
				}
			} else if (args[i].equals("-GName")) {
				GName = args[++i];

			} else if (args[i].equals("-GDirectory")) {
				GDirectory = args[++i];

			} else if (args[i].equals("-similarityThreshold")) {
				DummyProperties.similarityThreshold = Float.parseFloat(args[++i]);
			} else if (args[i].equals("-numberOfSameExperiment")) {
				numberOfSameExperiment = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-k")) {
				k = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-numberOfPrefixChars")) {
				numberOfPrefixChars = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-startingQueryIndex")) {
				startingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-endingQueryIndex")) {
				endingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-validationFoldStartFrom")) {
				validationFoldStartFrom = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-validationFoldEndTo")) {
				validationFoldEndTo = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-queriesFoldPath")) {
				queriesFoldPath = args[++i];
			} else if (args[i].equals("-maxNumberOfIteration")) {
				maxNumberOfIteration = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-startingModelIndex")) {
				startingModelIndex = Integer.parseInt(args[++i]);
			}
		}

		

		cacheServer = new CacheServer();

		if (numberOfPrefixChars > 0) {
			DummyProperties.numberOfPrefixChars = numberOfPrefixChars;
		}
		if (!GDirectory.endsWith("/")) {
			GDirectory += "/";
		}
		MODELGRAPH_DB_PATH = GDirectory + GName;
		PATTERNGRAPH_DB_PATH = queryFileDirectory + queryDBInNeo4j;

		String totalParams = "";
		for (String arg : args) {
			totalParams += arg + ", ";
		}
		DummyFunctions.printIfItIsInDebuggedMode(totalParams);

		knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(MODELGRAPH_DB_PATH)
				.setConfig(GraphDatabaseSettings.pagecache_memory, "6g").newGraphDatabase();
		DummyFunctions.printIfItIsInDebuggedMode("after initialization of GraphDatabaseServices");

		commonFunctions.registerShutdownHook(knowledgeGraph);

		long start_time, end_time;
		double difference;

		HashMap<String, HashSet<Long>> nodeLabelsIndex = PreProcessingLabels.getPrefixLabelsIndex(knowledgeGraph,
				Dummy.DummyProperties.numberOfPrefixChars);

		neighborIndexingInstance = new NeighborIndexing();
		start_time = System.nanoTime();
		neighborIndexingInstance.knowledgeGraphNeighborIndexer(knowledgeGraph);
		end_time = System.nanoTime();
		difference = (end_time - start_time) / 1e6;
		System.out.println("knowledgeGraphNeighborIndexer finished in " + difference + "miliseconds!");

		levenshtein = new Levenshtein(nodeLabelsIndex, Dummy.DummyProperties.numberOfPrefixChars);

		HashSet<Integer> validationQueriesSet = commonFunctions.readQueryIndexBasedOnFolds(queriesFoldPath,
				validationFoldStartFrom, validationFoldEndTo);

		MachineLearningQueryRunner mlQueryRunner = new MachineLearningQueryRunner(knowledgeGraph, queryFileDirectory,
				GDirectory, GName, startingQueryIndex, endingQueryIndex, PATTERNGRAPH_DB_PATH, k, levenshtein,
				cacheServer, neighborIndexingInstance);

		ExactImitation ei = new ExactImitation();
		for (Integer testQueryIndex : validationQueriesSet) {
			System.out.println("queries for test: " + testQueryIndex);
		}

		fillNormVectorsIfTheyAreExist();

		if (selectionNormalizationFeaturesVector != null) {
			System.out.println("selectionNormalizationFeaturesVector!=null");
		}
		if (expansionNormalizationFeaturesVector != null) {
			System.out.println("expansionNormalizationFeaturesVector!=null");
		}

		// for (int j = 0; j < maxNumberOfIteration; j++) {
		for (int i = startingModelIndex; i <= maxNumberOfIteration; i++) {

			// if (i == 0 && j == 0) {
			// continue;
			// }
			System.out.println("test with DAggerClassifier_" + i + ".model and DAggerRegressor_" + i + ".model");

			mlQueryRunner.findSpeedUpAndQualityOutOfAClassifierRegressorForASet(validationQueriesSet,
					ei.getModel("DAggerClassifier_" + i + ".model"),
					(Classifier) ei.getModel("DAggerRegressor_" + i + ".model"), numberOfSameExperiment,
					GName + "_machineLearningQueryRunningTime_" + i + "_" + i + "_test.txt",
					selectionNormalizationFeaturesVector, expansionNormalizationFeaturesVector);
			// }
		}

		knowledgeGraph.shutdown();

		System.out.println("program is finished properly!");

	}

	private void fillNormVectorsIfTheyAreExist() throws Exception {
		File f = new File("normalizationVectors.txt");
		if (!f.exists()) {
			System.out.println("normalizationVectors.txt doesn't exist");
			return;
		}

		selectionNormalizationFeaturesVector = new double[SELECTION_FEATURES_SIZE];
		expansionNormalizationFeaturesVector = new double[EXPANSION_FEATURES_SIZE];

		FileInputStream fis = new FileInputStream(f.getAbsolutePath());
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String selectionNormalizationLine = null;
		String expansionNormalizationLine = null;

		selectionNormalizationLine = br.readLine();
		String[] splitedSelNorm = selectionNormalizationLine.split(",");
		for (int i = 0; i < SELECTION_FEATURES_SIZE; i++) {
			selectionNormalizationFeaturesVector[i] = Double.parseDouble(splitedSelNorm[i]);
		}

		expansionNormalizationLine = br.readLine();
		String[] splitedRegNorm = expansionNormalizationLine.split(",");
		for (int i = 0; i < EXPANSION_FEATURES_SIZE; i++) {
			expansionNormalizationFeaturesVector[i] = Double.parseDouble(splitedRegNorm[i]);
		}

		br.close();
	}

}
