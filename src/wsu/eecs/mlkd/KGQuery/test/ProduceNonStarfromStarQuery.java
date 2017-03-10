package wsu.eecs.mlkd.KGQuery.test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.TopKQuery.CacheServer;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy;
import wsu.eecs.mlkd.KGQuery.TopKQuery.GraphResult;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;
import wsu.eecs.mlkd.KGQuery.TopKQuery.StarFramework;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;

/*
 * the main purpose of this Class is to convert or find a non star query from a star
 * query. We got the star queries as Frequent Pattern from each of the DataSets.
 * 
 *  We want to find edges between all pivot nodes for our desired pattern.
 *  Here we want to find in such Pattern (5,5), (5,6) //nodes and edges ratio
 */

//parameters are 
//-frequent pattern file
//no of nodes, no of edges
//threshold, graph Name to save the file in format GName_Node_Edge.txt i.e dbPedia_5_5.txt 

public class ProduceNonStarfromStarQuery {
	private static String PatternFileName;
	private static String PatternFileDirectory;
	private static int NoOfEdges;
	private static int NoOfNodes;
	private static String GraphName;
	private static int Threshold;
	private static String GDirectory;
	private static String MODELGRAPH_DB_PATH = "";
	private static String PATTERNGRAPH_DB_PATH = "";
	public static String queryDBInNeo4j = "query.db";
	public static int numberOfPrefixChars = 4;
	private static float alpha = 0.5F;
	private static int querySizeTo;
	private static int querySizefrom;

	public static void main(String[] args) throws Exception {

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-patternFileName")) {
				PatternFileName = args[++i];
				PatternFileName = PatternFileName.replace(".txt", "");
			} else if (args[i].equals("-patternFileDirectory")) {
				PatternFileDirectory = args[++i];
				if (!PatternFileDirectory.endsWith("/") && !PatternFileDirectory.equals("")) {
					PatternFileDirectory += "/";
				}
			} else if (args[i].equals("-edges")) {
				NoOfEdges = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-nodes")) {
				NoOfNodes = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-threshold")) {
				Threshold = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-GName")) {
				GraphName = args[++i];
			} else if (args[i].equals("-GDirectory")) {
				GDirectory = args[++i];
			} else if (args[i].equals("-qFrom")) {
				querySizefrom = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-qTo")) {
				querySizeTo = Integer.parseInt(args[++i]);
			}
		}
		if (!GDirectory.endsWith("/")) {
			GDirectory += "/";
		}
		MODELGRAPH_DB_PATH = GDirectory + GraphName;
		PATTERNGRAPH_DB_PATH = PatternFileDirectory + queryDBInNeo4j;

		if (PatternFileName.equals("") || GraphName.equals("") || NoOfEdges == 0 || NoOfNodes == 0 || Threshold == 0) {
			throw new Exception(
					"We need the parameters, -patternFileName -patternFileDirectory -edges -nodes -threshold -GName -GDirectory");
		}
		long pivotNode = 0;
		ArrayList<Long> nonPivotNode = new ArrayList<>();
		ProduceNonStarfromStarQuery findNonStarfromStarQuery = new ProduceNonStarfromStarQuery();

		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(MODELGRAPH_DB_PATH);
		registerShutdownHook(knowledgeGraph);
		HashMap<String, HashSet<Long>> nodeLabelsIndex = PreProcessingLabels.getPrefixLabelsIndex(knowledgeGraph,
				numberOfPrefixChars);
		Levenshtein levenshtein = new Levenshtein(nodeLabelsIndex, numberOfPrefixChars);
		for (int querySize = querySizefrom; querySize <= querySizeTo; ++querySize) {
			QueryGenerator queryGenerator = new QueryGenerator(GDirectory + GraphName);
			List<QueryFromFile> queriesFromFile = queryGenerator
					.getQueryFromFile(PatternFileDirectory + PatternFileName + querySize + ".txt");
			for (QueryFromFile queryFromFile : queriesFromFile) {
				DummyFunctions.printIfItIsInDebuggedMode("start ConstructQueryGraph");
				FileUtils.deleteRecursively(new File(PATTERNGRAPH_DB_PATH));
				GraphDatabaseService smallGraph = queryGenerator.ConstrucQueryGraph(PATTERNGRAPH_DB_PATH,
						queryFromFile);
				DummyFunctions.printIfItIsInDebuggedMode("end ConstructQueryGraph");

				GraphDatabaseService queryGraph = smallGraph;
				registerShutdownHook(queryGraph);
				try (Transaction tx = smallGraph.beginTx()) {
					pivotNode = findNonStarfromStarQuery.FindPivotNode(queryGraph, nonPivotNode);
					int nodes = queryFromFile.numberOfNodes;
					int edges = queryFromFile.numberOfEdges;
					int k = nodes - edges + 1;
					Long[] input = nonPivotNode.toArray(new Long[nonPivotNode.size()]);
					Long[] branch = new Long[k];// {0,0};//new char[k];
					HashSet<ArrayList<Long>> arrSet = new HashSet();
					arrSet = combine(input, k, 0, branch, 0, arrSet);// got the
																		// possible
																		// matches.
					for (ArrayList<Long> list : arrSet) {
						findNonStarfromStarQuery.FeasibelEdge(queryGraph, list, knowledgeGraph, nodeLabelsIndex,
								levenshtein);
					}
					tx.success();
				} catch (Exception e) {
					// TODO: handle exception
					knowledgeGraph.shutdown();
				}
				smallGraph.shutdown();
				smallGraph.isAvailable(5000);
			}

		}
	}

	private void FeasibelEdge(GraphDatabaseService queryGraph, ArrayList<Long> relation,
			GraphDatabaseService knowledgeGraph, HashMap<String, HashSet<Long>> nodeLabelsIndex,
			Levenshtein levenshtein) {
		// split relation list into pairs 1,2,3 to 1,2 and 2,3 like this
		int listSize = relation.size();
		Relationship rel = null;
		for (int i = 0; i < listSize - 1; ++i) {
			for (int j = 0; j < 2; ++j) {
				Node sourceNode = queryGraph.getNodeById(relation.get(i));
				Node destinationNode = queryGraph.getNodeById(relation.get(i + 1));
				if (j == 0) {
					rel = sourceNode.createRelationshipTo(destinationNode, RelTypes.RELATED);
					System.out.println(rel.getStartNode().getId() + " and " + rel.getEndNode().getId() + " connected");
				} else {
					rel = destinationNode.createRelationshipTo(sourceNode, RelTypes.RELATED);
					System.out.println(rel.getStartNode().getId() + " and " + rel.getEndNode().getId() + "connected");
				}
				// now we have to search for the threshold
				try (Transaction tx1 = queryGraph.beginTx()) {
					try (Transaction tx2 = knowledgeGraph.beginTx()) {
						CacheServer.clear();
						Dummy.DummyProperties.similarityThreshold = 0.5F;
						Dummy.DummyProperties.debuggMode = false;
						StarFramework starFramework2 = new StarFramework(queryGraph, knowledgeGraph, Threshold, alpha,
								levenshtein);

						ArrayList<GraphResult> finalResults = starFramework2.starRun(queryGraph, knowledgeGraph, neighborIndexingInstance);
						System.out.println("StarFramework finished with result count: " + finalResults.size());

						int finalResultSizeTemp = finalResults.size();

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
				rel.delete();
				System.out.println("removed the relation" + rel.getStartNode().getId() + " and "
						+ rel.getEndNode().getId() + "disconnected connected");
			}
		}
	}

	private long FindPivotNode(GraphDatabaseService queryGraph, List<Long> nonPivotNode) {
		ResourceIterable<Node> allNodes = GlobalGraphOperations.at(queryGraph).getAllNodes();
		long pivotNode = 0;
		for (Node node : allNodes) {
			if (node.getDegree() > 1) {
				pivotNode = node.getId();
			} else {
				nonPivotNode.add(node.getId());
			}
		}

		return pivotNode;
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	private static enum RelTypes implements RelationshipType {
		RELATED
	}

	static HashSet combine(Long[] arr, int k, int startId, Long[] branch, int numElem, HashSet arrSet) {
		if (numElem == k) {
			// System.out.println("k: "+k+(Arrays.toString(branch)));
			ArrayList<Long> mySet = new ArrayList<Long>();
			for (int i = 0; i < branch.length; i++) {
				mySet.add(branch[i]);
			}
			arrSet.add(mySet);
			return arrSet;
		}

		for (int i = startId; i < arr.length; ++i) {
			branch[numElem++] = arr[i];
			combine(arr, k, ++startId, branch, numElem, arrSet);
			--numElem;
		}
		return arrSet;
	}
}
