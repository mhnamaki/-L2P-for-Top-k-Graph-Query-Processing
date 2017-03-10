package wsu.eecs.mlkd.KGQuery.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.TopKQuery.GraphTA;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;


public class SimilarityTester {
	private static String MODELGRAPH_DB_PATH = "";
	private static String PATTERNGRAPH_DB_PATH = "";

	public static String queryFileName = "";
	public static String queryFileDirectory = "";
	public static int kFrom = 0;
	public static int kTo = 0;
	public static int querySizeFrom = 0;
	public static int querySizeTo = 0;
	public static String GName = ""; // Yago, DBPedia, ...

	public static String queryDBInNeo4j = "query.db";
	public static String GDirectory = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/newData";

	public static void main(String[] args) throws Exception {

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-queryFileName")) {
				queryFileName = args[++i];
				queryFileName = queryFileName.replace(".txt", "");
			} else if (args[i].equals("-queryFileDirectory")) {
				queryFileDirectory = args[++i];
				if (!queryFileDirectory.endsWith("/") && !queryFileDirectory.equals("")) {
					queryFileDirectory += "/";
				}
			} else if (args[i].equals("-kFrom")) {
				kFrom = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-kTo")) {
				kTo = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-querySizeFrom")) {
				querySizeFrom = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-querySizeTo")) {
				querySizeTo = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-GName")) {
				GName = args[++i];

			}
		}

		if (!GDirectory.endsWith("/")) {
			GDirectory += "/";
		}
		MODELGRAPH_DB_PATH = GDirectory + GName;
		PATTERNGRAPH_DB_PATH = queryFileDirectory + queryDBInNeo4j;

		if (queryFileName.equals("") || GName.equals("") || kFrom == 0 || kTo == 0 || querySizeFrom == 0
				|| querySizeTo == 0) {
			throw new Exception(
					"You should provide all the parameters -queryFileName  -queryFileDirectory -kFrom -kTo -querySizeFrom -querySizeTo -GName");
		}

		QueryGenerator queryGenerator = new QueryGenerator(GDirectory + GName);

		// output the results and answers
		File fout = new File(GName + queryFileName + "_answerResults.txt");
		FileOutputStream fos = new FileOutputStream(fout);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		// output the
		File foutTime = new File(GName + queryFileName + "_timeResults.txt");
		FileOutputStream fosTime = new FileOutputStream(foutTime);

		BufferedWriter bwTime = new BufferedWriter(new OutputStreamWriter(fosTime));

		for (int querySize = querySizeFrom; querySize <= querySizeTo; querySize++) {
			List<QueryFromFile> queriesFromFile = queryGenerator
					.getQueryFromFile(queryFileDirectory + queryFileName + querySize + ".txt");
			for (QueryFromFile queryFromFile : queriesFromFile) {
				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH,
						queryFromFile);

				for (int k = kFrom; k <= kTo; k++) {

					GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory()
							.newEmbeddedDatabase(MODELGRAPH_DB_PATH);

					registerShutdownHook(knowledgeGraph);

					GraphDatabaseService queryGraph = smallGraph;
					registerShutdownHook(queryGraph);

					try (Transaction tx1 = queryGraph.beginTx()) {
						try (Transaction tx2 = knowledgeGraph.beginTx()) {

							//GraphTA graphTa = new GraphTA(queryGraph, knowledgeGraph, k);

							// GraphItemSimilarity similarity = new
							// GraphItemSimilarity();

//							System.out.println(GraphItemSimilarity.Levenshtein.HowMuchTwoNodesAreSimilar(queryGraph.getNodeById(0),
//									knowledgeGraph.getNodeById(3838702)));

							tx2.success();
						} catch (Exception exc) {
							System.out.println("queryGraph Transaction failed");
							exc.printStackTrace();
						}

						tx1.success();
					} catch (Exception exc) {
						System.out.println("modelGraph Transaction failed");
						exc.printStackTrace();
					}
					queryGraph.shutdown();
					knowledgeGraph.shutdown();
					bw.close();
					bwTime.close();
				}
			}
		}

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
