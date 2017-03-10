package wsu.eecs.mlkd.KGQuery.test;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.TopKQuery.*;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

// the main purpose for writing this duplicated class is generating queries for anytime star framework.
// for anytime starframework we don't need to find queries to have a lot of completed answers.
// because we accept the semi-completed answers.

public class QueryGeneratorFromTemplatesAllLbls {

	private static String MODELGRAPH_DB_PATH = "";
	private static String PATTERNGRAPH_DB_PATH = "";

	public static String templateFilePath = "queriesTemplatePruned.txt";
	public static String graphFileDirectory = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.3.1/data/yago.db";
	public static int k = 2;
	public static String queryDBInNeo4j = "query.db";
	public static int numberOfPrefixChars = 12;
	private static BufferedWriter outputFile = null;
	private static String outputFileName = "generatedQueries";
	// public static String GDirectory = "";
	public static CacheServer cacheServer;
	private static int startingQueryIndex = 0;
	private static int endingQueryIndex = 20;
	private static int maxNumberOfLabelsCombination = 0;

	public static void main(String[] args) throws Exception {
		DummyProperties.similarityThreshold = 0.3F;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-templateFilePath")) {
				templateFilePath = args[++i];
			} else if (args[i].equals("-graphFileDirectory")) {
				graphFileDirectory = args[++i];
			} else if (args[i].equals("-similarityThreshold")) {
				DummyProperties.similarityThreshold = Float.parseFloat(args[++i]);
			} else if (args[i].equals("-k")) {
				k = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-outputFileName")) {
				outputFileName = args[++i];
			} else if (args[i].equals("-startingQueryIndex")) {
				startingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-endingQueryIndex")) {
				endingQueryIndex = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-maxNumberOfLabelsCombination")) {
				maxNumberOfLabelsCombination = Integer.parseInt(args[++i]);
			}
		}

		if (!graphFileDirectory.endsWith("/")) {
			graphFileDirectory += "/";
		}
		MODELGRAPH_DB_PATH = graphFileDirectory;
		PATTERNGRAPH_DB_PATH = queryDBInNeo4j;

		QueryGenerator queryGenerator = new QueryGenerator(graphFileDirectory);

		// output the results and answers
		// File fout = new File(GName + queryFileName +
		// "_answerResults_starFramework.txt");
		// FileOutputStream fos = new FileOutputStream(fout);
		//
		// BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		// output the
		// File foutTime = new File(GName + queryFileName +
		// "_timeResults_starFramework.txt");
		// FileOutputStream fosTime = new FileOutputStream(foutTime);
		//
		// BufferedWriter bwTime = new BufferedWriter(new
		// OutputStreamWriter(fosTime));

		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(MODELGRAPH_DB_PATH);
		DummyFunctions.printIfItIsInDebuggedMode("after initialization of GraphDatabaseServices");
		registerShutdownHook(knowledgeGraph);

		long start_time, end_time;
		double difference;

		NeighborIndexing neighborIndexingInstance = new NeighborIndexing();
		start_time = System.nanoTime();
		neighborIndexingInstance.knowledgeGraphNeighborIndexer(knowledgeGraph);
		end_time = System.nanoTime();
		difference = (end_time - start_time) / 1e6;
		System.out.println("knowledgeGraphNeighborIndexer finished in " + difference + "miliseconds!");

		float alpha = 0.5F;

		List<QueryFromFile> queriesFromFile = queryGenerator.getQueryFromFile(templateFilePath);
		int queryIndex = 0;
		for (QueryFromFile queryFromFile : queriesFromFile) {
			System.gc();
			DummyFunctions.printIfItIsInDebuggedMode("start ConstrucQueryGraph");

			queryIndex++;

			if (queryIndex < startingQueryIndex || queryIndex > endingQueryIndex) {
				continue;
			}

			System.out.println("queryIndex: " + queryFromFile.queryIndex);

			GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH, queryFromFile);
			DummyFunctions.printIfItIsInDebuggedMode("end ConstrucQueryGraph");

			GraphDatabaseService queryGraph = smallGraph;
			// registerShutdownHook(queryGraph);

			start_time = System.nanoTime();
			Dummy.DummyProperties.semanticChecking = false;
			neighborIndexingInstance.queryNeighborIndexer(queryGraph);
			end_time = System.nanoTime();
			difference = (end_time - start_time) / 1e6;
			Dummy.DummyFunctions
					.printIfItIsInDebuggedMode("queryNeighborIndexer finished in " + difference + "miliseconds!");

			HashMap<String, HashSet<Long>> nodeLabelsIndex = PreProcessingLabels.getPrefixLabelsIndex(knowledgeGraph,
					numberOfPrefixChars);
			Levenshtein levenshtein = new Levenshtein(nodeLabelsIndex, numberOfPrefixChars);
			try (Transaction tx1 = queryGraph.beginTx()) {
				try (Transaction tx2 = knowledgeGraph.beginTx()) {

					// CacheServer.clear();

					StarFramework starFramework2 = new StarFramework(queryGraph, knowledgeGraph, k, alpha, null);
					starFramework2.decomposeQuery(queryGraph, knowledgeGraph, neighborIndexingInstance, cacheServer);

					start_time = System.nanoTime();

					ArrayList<GraphResult> finalResults = starFramework2.starRun(queryGraph, knowledgeGraph,
							neighborIndexingInstance, cacheServer);

					end_time = System.nanoTime();
					difference = (end_time - start_time) / 1e6;
					System.out.println("StarFramework finished in " + difference + "miliseconds!");

					int finalResultSizeTemp = finalResults.size();
					System.out.println("finalResults: " + finalResults.size());
					if (finalResultSizeTemp == 0) {
						System.err.println("No Answer Found for query index! " + queryIndex);
						break;
					} else {

						// iterating all answers
						for (int m = 0; m < finalResults.size(); m++) {

							HashMap<Long, ArrayList<String>> qNodesLabelsMap = new HashMap<Long, ArrayList<String>>();
							for (Node qNode : finalResults.get(m).assembledResult.keySet()) {
								ArrayList<String> qNodeLabels = new ArrayList<String>();
								for (Label lbl : finalResults.get(m).assembledResult.get(qNode).node.getLabels()) {
									qNodeLabels.add(lbl.toString());
								}
								qNodesLabelsMap.put(qNode.getId(), qNodeLabels);
							}

							int solutions = 1;
							for (Long qNode : qNodesLabelsMap.keySet()) {
								solutions *= qNodesLabelsMap.get(qNode).size();
							}
							System.out.println("number of all solutions: " + solutions);

							int cnt = 0;
							for (int i = 0; i < solutions; i++) {

								if ((cnt % 5000000) == 0) {
									int rem = cnt / 5000000;
									if (outputFile != null) {
										outputFile.flush();
										outputFile.close();
									}
									outputFile = new BufferedWriter(
											new FileWriter(outputFileName + "_" + queryIndex + "_" + rem + ".txt"));
								}
								// if (maxNumberOfLabelsCombination > 0 && cnt >
								// maxNumberOfLabelsCombination) {
								// break;
								// }
								String query = Integer.toString(finalResults.get(m).assembledResult.size()) + "\n";

								int j = 1;

								HashSet<String> duplicatedLabelChecking = new HashSet<String>();
								int tempJ = j;
								for (Long qNode : qNodesLabelsMap.keySet()) {
									duplicatedLabelChecking.add(qNodesLabelsMap.get(qNode)
											.get((i / tempJ) % qNodesLabelsMap.get(qNode).size()));
									tempJ *= qNodesLabelsMap.get(qNode).size();
								}
								if (duplicatedLabelChecking.size() == qNodesLabelsMap.size()) {
									cnt++;
									for (Long qNode : qNodesLabelsMap.keySet()) {
										query += qNode + ";" + qNodesLabelsMap.get(qNode)
												.get((i / j) % qNodesLabelsMap.get(qNode).size()) + " \n";

										j *= qNodesLabelsMap.get(qNode).size();
									}

									for (Long queryNodeId : neighborIndexingInstance.queryNodeIdSet) {
										query += queryNodeId + ";";
										for (Long neighborId : neighborIndexingInstance.queryOutNeighborIndicesMap
												.get(queryNodeId)) {
											query += neighborId + ";";
										}
										query += "\n";
									}
									outputFile.write(query);

								}

							}

						}
						finalResults.clear();

						System.out.println();
						System.out.println();
					}

					tx2.success();
					// DummyFunctions.printIfItIsInDebuggedMode("before
					// tx2 success");

				} catch (Exception exc) {
					System.out.println("queryGraph Transaction failed");
					exc.printStackTrace();
				}
				// DummyFunctions.printIfItIsInDebuggedMode("before
				// tx1
				// success");
				tx1.success();
				// DummyFunctions.printIfItIsInDebuggedMode("after
				// tx1
				// success");
			} catch (Exception exc) {
				System.out.println("modelGraph Transaction failed");
				exc.printStackTrace();
			}
			outputFile.close();
			outputFile = null;
			queryGraph.shutdown();
		}

		// }
		// bw.close();
		// bwTime.close();
		knowledgeGraph.shutdown();

		System.out.println("program is finished properly!");

	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
}
