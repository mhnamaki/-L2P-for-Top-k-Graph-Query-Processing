package wsu.eecs.mlkd.KGQuery.test;

import java.io.*;
import java.util.*;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.TopKQuery.NeighborIndexing;

/**
 * Created by mislam1 on 11/9/15.
 */
// it will generate queries for a given database and number
public class QueryGenerator {
	private static Boolean DEBUG = false;
	private GraphDatabaseService graphDb;
	private static final String QUERY_PATH = "/Users/mislam1/Documents/courses/BigData/Project/TopKStarQuery/";
	private static final String QUERY_FILE = "/Users/mislam1/Documents/courses/BigData/Project/TopKStarQuery/StoredQuery.txt";
	private static int labelSizeLimit = 0;
	public int nodeCount;
	public int relationCount;
	public int indexBuff = 0;
	public Random random;
	int queryCount = 100;
	int minQuerySize = 1;
	int maxQuerySize = 1;
	String queryFileName = "";
	String dbFilePath = "";
	LabelStat labelStat = null;
	Map<String, Integer> labelMap = null;
	int labelSize = 3;

	private static enum RelTypes implements RelationshipType {
		RELATED
	}

	public static void main(String[] args) throws IOException {

		QueryGenerator queryGenerator = new QueryGenerator("");

		for (int i = 0; i < args.length; ++i) {
			if (args[i].compareTo("-qcount") == 0)
				queryGenerator.queryCount = Integer.parseInt(args[++i]);
			else if (args[i].compareTo("-minQ") == 0)
				queryGenerator.minQuerySize = Integer.parseInt(args[++i]);
			else if (args[i].compareTo("-maxQ") == 0)
				queryGenerator.maxQuerySize = Integer.parseInt(args[++i]);
			else if (args[i].compareTo("-qFile") == 0)
				queryGenerator.queryFileName = args[++i];
			else if (args[i].compareTo("-minLabel") == 0)
				labelSizeLimit = Integer.parseInt(args[++i]);
			else if (args[i].compareTo("-dbFile") == 0) {
				queryGenerator.dbFilePath = args[++i];

				queryGenerator.graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(queryGenerator.dbFilePath);
				registerShutdownHook(queryGenerator.graphDb);
				queryGenerator.labelStat = new LabelStat(queryGenerator.graphDb);
				queryGenerator.labelStat.ConstructLabelMap();
				queryGenerator.labelMap = queryGenerator.labelStat.getLabelMap();
				try (Transaction tx = queryGenerator.graphDb.beginTx()) {
					ResourceIterable<Node> allNodes = GlobalGraphOperations.at(queryGenerator.graphDb).getAllNodes();
					Iterable<Relationship> relationships = GlobalGraphOperations.at(queryGenerator.graphDb)
							.getAllRelationships();

					for (Node node : allNodes) {
						queryGenerator.nodeCount++;
						// System.out.println(node.getId());
					}

					System.out.println("No of Nodes: " + queryGenerator.nodeCount);
					for (Relationship rel : relationships) {
						queryGenerator.relationCount++;
					}
					System.out.println("No of RelationShips: " + queryGenerator.relationCount);
					tx.success();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}

		System.out.println("starting");
		try (Transaction tx = queryGenerator.graphDb.beginTx()) {
			// Database operations go here
			// queryGenerator.GetStat();

			// queryGenerator.Traverse(true, 3, queryGenerator.queryCount);
			for (int i = queryGenerator.minQuerySize; i <= queryGenerator.maxQuerySize; ++i) {
				queryGenerator.Traverse(false, i, queryGenerator.queryCount);
			}

			// queryGenerator.ReConstructGraph();
			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
		}
		// System.out.println("result:");

		queryGenerator.graphDb.shutdown();
	}

	public QueryGenerator(String path) {
		// graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(path);
		this.nodeCount = 0;
		this.relationCount = 0;
	}

	public boolean Traverse(boolean tree, int nodes, int qCount) throws IOException {
		//
		List<HashSet<Node>> generatedQuery = new ArrayList<>();
		HashSet<Node> currentSet = new HashSet<>();

		random = new Random(System.currentTimeMillis());
		Node nodeSelected = graphDb.getNodeById(GetRandomId(nodeCount));
		int depth = nodes;
		ShowProperties(nodeSelected);
		if (tree) {
			System.out.println("Tree Query Generation");
			HashSet<Node> currentTree = new HashSet<>();
			currentTree.add(nodeSelected);
			ConstructTree(currentTree, nodeSelected, nodes);

			System.out.println("Selected Node: " + nodeSelected.getId());
			if (currentTree.size() >= nodes) {
				// PrintSet(currentTree);
				SaveQuery(currentTree);
				qCount--;
			}
			currentTree.clear();
			if (qCount > 0)
				Traverse(true, nodes, qCount);
		} else {
			// System.out.println("========================");
			// System.out.println("Graph Query Generation");
			// System.out.println("========================");
			while (generatedQuery.size() <= qCount) {
				// System.out.println("size of generate: " +
				// generatedQuery.size());
				ConstructGraph(currentSet, nodeSelected, depth, nodes);
				if (generatedQuery.size() == 0) {
					HashSet<Node> graphNode = new HashSet<>(currentSet);
					generatedQuery.add(graphNode);
				} else if (CheckForDuplicates(generatedQuery, currentSet) == false && currentSet.size() == nodes) {
					// --qCount;
					HashSet<Node> graphNode = new HashSet<>(currentSet);
					generatedQuery.add(graphNode);
					// PrintSet(currentSet);
					System.out.println("graphQuery count is :" + generatedQuery.size());
					// System.out.println("========================");
					SaveQuery(graphNode);
				}
				currentSet.clear();
				nodeSelected = graphDb.getNodeById(GetRandomId(-1));
			}
		}
		return true;
	}

	public List<String> GetProperties(Node nodeSelected) {
		// TODO Auto-generated method stub
		List<String> propertiesList = new ArrayList<>();
		Iterable<String> nodeProperties = nodeSelected.getPropertyKeys();
		Iterator<String> propertyIterator = nodeProperties.iterator();
		while (propertyIterator.hasNext()) {
			String nextKey = propertyIterator.next();
			// System.out.println(nextKey + ": " +
			// nodeSelected.getProperty(nextKey));
			String nextProp = nodeSelected.getProperty(nextKey).toString().replace("\n", "");
			nextProp = nextProp.replace(";", "");
			propertiesList.add(nextProp);
		}
		return propertiesList;
	}

	public void ShowProperties(Node nodeSelected) {
		Iterable<String> nodeProperties = nodeSelected.getPropertyKeys();
		Iterator<String> propertyIterator = nodeProperties.iterator();
		while (propertyIterator.hasNext()) {
			String nextKey = propertyIterator.next();
			System.out.println(nextKey + ": " + nodeSelected.getProperty(nextKey));
		}
	}

	// returns true if already found in the generated query list
	private boolean CheckForDuplicates(List<HashSet<Node>> generatedQuery, HashSet<Node> currentSet) {
		// TODO Auto-generated method stub

		for (HashSet<Node> querySet : generatedQuery) {
			// if its already generated then skip it
			if (querySet.containsAll(currentSet) && currentSet.containsAll(querySet)) {
				return true;
			}
		}
		return false;

	}

	public void PrintSet(HashSet<Node> set) {
		System.out.println("Printing Set: ");
		for (Node node : set) {
			System.out.print(node.getId() + " ");
		}
	}

	public boolean LabelisFeasible(String label) {
		if (!label.isEmpty() && label.length() >= labelSizeLimit)
			return true;
		return false;
	}

	public void SaveQuery(HashSet<Node> set, String queryFileName) throws IOException {
		this.queryFileName = queryFileName;
		SaveQuery(set);
	}

	public void SaveQueryInfo(int queryIndex, HashSet<Long> set, String queryFileName, String nodeEstimation,
			String joinableNodes, String queryInfo, Double time, NeighborIndexing neighborIndexingInstance,
			int numberOfSQs, int offset) throws IOException {
		this.queryFileName = queryFileName;

		BufferedWriter output = new BufferedWriter(
				new FileWriter("BasedOnNumberOfSQs" + numberOfSQs + "_" + offset + ".txt", true));

		// output.write("queryFileName: " + queryFileName + "\n");
		if (set.size() > 0) {
			output.write(set.size() + "\n");
		}

		output.write("queryIndex:" + queryIndex + "\n");

		if (nodeEstimation != null) {
			output.write("nodeEstimation: " + nodeEstimation + "\n");
		}
		if (joinableNodes != null) {
			output.write("joinableNodes: " + joinableNodes + "\n");
		}
		if (time != null) {
			output.write("time: " + time.intValue() + "\n");
		}
		if (queryInfo != null) {
			output.write("queryInfo: " + queryInfo + "\n");
		}

		// write the nodes labels
		for (Long nodeId : set) {
			output.write(nodeId + ";" + neighborIndexingInstance.queryNodeLabelMap.get(nodeId) + ";\n");
		}

		for (Long sourceNode : set) {
			output.write(sourceNode + ";");
			for (Long destNode : neighborIndexingInstance.queryOutNeighborIndicesMap.get(sourceNode)) {
				output.write(destNode + ";");
			}
			output.write("\n");

		}
		output.flush();
		output.close();
		output = null;
	}

	public void SaveQuery(HashSet<Node> set) throws IOException {
		boolean first = false;
		List<String> propertyList = null;
		HashSet<Long> neighborSet = new HashSet<>();
		BufferedWriter output = new BufferedWriter(new FileWriter(queryFileName + "_" + set.size() + ".txt", true));
		// traverse the graphDB using the node list from the tree/graph
		Node[] nodeArray = set.toArray(new Node[set.size()]);

		output.write("queryFileName: " + queryFileName + "\n");
		if (set.size() > 0) {
			output.write(set.size() + "\n");
		}

		// write the nodes labels
		for (Node node : nodeArray) {
			// output.write(String.valueOf(node.getPropertyKeys()));
			Iterable<Label> nodeLabel = node.getLabels();
			Iterator<Label> labelIterator = nodeLabel.iterator();
			// if (!labelIterator.hasNext())
			// output.write("No_Label_" + node.getId() + ";");
			output.write(node.getId() + ";");
			boolean hasLabel = false;
			int count = -1;
			String labelCandidate = null;
			while (labelIterator.hasNext()) {
				Label label = labelIterator.next();

				if (labelMap == null) {
					hasLabel = true;
					labelCandidate = label.toString();
				} else {
					// filter if its size >=4 and not an URL
					// then filter based on the frequency
					// (we are considering not too general not too rare texts as
					// labels)
					if (LabelisFeasible(label.name())) {
						hasLabel = true;
						if (labelMap.containsKey(label.name())) {
							int frequency = labelMap.get(label.name());
							if (frequency > count /*
													 * && frequency >= 2 &&
													 * frequency <= 1000
													 */) {
								count = frequency;
								labelCandidate = label.name();
							}
						}

					}
					// TODO: select the label as the query label

				}
			}
			if (labelCandidate != null && !labelCandidate.isEmpty()) {
				output.write(labelCandidate + ";\n");
			}
			if (!hasLabel) {
				propertyList = GetProperties(node);
				for (String prop : propertyList) {
					if (LabelisFeasible(prop) && labelMap.containsKey(prop)) {
						int frequency = labelMap.get(prop);
						if (frequency > count /*
												 * && frequency >= 10 &&
												 * frequency <= 100
												 */) {
							count = frequency;
							labelCandidate = prop;
						}
					}
				}
				// TODO: select the property as the label for that node
				if (labelCandidate != null && !labelCandidate.isEmpty()) {
					output.write(labelCandidate + ";\n");
				}
			}

		}

		for (Node node : nodeArray) {
			Iterable<Relationship> relations = node.getRelationships();
			Iterator<Relationship> relationIterator = relations.iterator();
			Iterable<Label> nodeLabel = node.getLabels();
			Iterator<Label> labelIterator = nodeLabel.iterator();

			output.write(node.getId() + ";");
			// first = false;
			neighborSet.add(new Long(node.getId()));
			while (relationIterator.hasNext()) {
				Relationship rel = relationIterator.next();
				if (rel.getStartNode().getId() == node.getId()) {
					if (set.contains(rel.getOtherNode(node)) && !neighborSet.contains(rel.getOtherNode(node).getId())) {
						neighborSet.add(new Long(rel.getOtherNode(node).getId()));
						nodeLabel = rel.getOtherNode(node).getLabels();
						labelIterator = nodeLabel.iterator();
						output.write(rel.getOtherNode(node).getId() + ";");
					}
				}
			}
			output.write("\n");
			output.write("ENDQUERY\n\n");
			output.flush();
			neighborSet.clear();
		}
		output.close();
		output = null;
	}

	public void ConstructGraph(HashSet<Node> currentSet, Node nodeSelected, int depth, int nodes) {

		Queue<Node> frontier = new LinkedList<>();
		frontier.add(nodeSelected);
		boolean graph = false;
		// System.out.println("starting from: " + nodeSelected.getId());
		while (!frontier.isEmpty() && depth > 0) {
			Node front = frontier.peek();
			currentSet.add(frontier.peek());
			frontier.remove();
			if (front.getDegree() >= 1) {
				Iterable<Relationship> relations = front.getRelationships();
				Iterator<Relationship> relationIterator = relations.iterator();
				while (relationIterator.hasNext()) {
					// System.out.println("iterating over relations");
					Relationship rel = relationIterator.next();
					Node otherNode = rel.getOtherNode(front);
					if (frontier.contains(otherNode) && !currentSet.contains(otherNode) && currentSet.size() <= nodes) {
						currentSet.add(rel.getOtherNode(front));
						currentSet.add(otherNode);
						Iterable<Relationship> curRelation = otherNode.getRelationships();
						Iterator<Relationship> curRelItr = relations.iterator();
						int count = 0;
						while (curRelItr.hasNext()) {
							Relationship relInner = curRelItr.next();
							if (relInner.getEndNode().getId() == otherNode.getId()
									|| relInner.getStartNode().getId() == otherNode.getId()) {
								count++;
							}
						}
						if (count == 1) {
							graph = true;
							// System.out.println("found graph!! may be");
						} else {
							// System.out.println("multiple relationship");
						}
					} else if (currentSet.size() == nodes && graph) {

						return;
					} else if (currentSet.size() == nodes) {
						currentSet.clear();
						frontier.clear();
						return;
					} else if (!frontier.contains(otherNode) && !currentSet.contains(otherNode)) {
						frontier.add(rel.getOtherNode(front));
					}
				}
				depth--;
			} else {
				// System.out.println("not enough edges found " +
				// nodeSelected.getId());
			}
		}

	}

	//
	public void ConstructTree(HashSet<Node> currentTree, Node nodeSelected, int nodes) {

		if (currentTree.size() == nodes) {
			return;
		}
		// tree query
		int degree = nodeSelected.getDegree();
		// select random adjacent node that is not in the tree
		if (degree > 1) {
			Iterable<Relationship> relations = nodeSelected.getRelationships();
			Iterator<Relationship> relationIterator = relations.iterator();
			while (relationIterator.hasNext()) {
				Relationship rel = relationIterator.next();
				if (!currentTree.contains(rel.getOtherNode(nodeSelected))) {
					currentTree.add(rel.getOtherNode(nodeSelected));
					if (currentTree.size() == nodes)
						break;
					// PrintSet(currentTree);
				}
			}
			ConstructTree(currentTree, graphDb.getNodeById(GetRandomId(nodeSelected.getDegree())), nodes);

		} else {
			System.out.println("degree is < 1");
		}
	}

	public void ReConstructTree() {

	}

	public GraphDatabaseService ConstrucQueryGraph(String graphPath, QueryFromFile queryFromFile) {
		GraphDatabaseService smallGraph = null;
		Node graphNode = null;

		try {
			FileUtils.deleteRecursively(new File(graphPath));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		smallGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(graphPath)
				.setConfig(GraphDatabaseSettings.pagecache_memory, "240k").newGraphDatabase();
		// registerShutdownHook(smallGraph);
		Map<String, Node> nodesCreated = new HashMap<String, Node>();
		try (Transaction tx = smallGraph.beginTx()) {
			for (QueryNode node : queryFromFile.nodes) {
				graphNode = smallGraph.createNode();
				// graphNode.addLabel(DynamicLabel.label(node.node));
				nodesCreated.put(node.node, graphNode);
				if (node.labels.size() > 0)
					graphNode.addLabel(DynamicLabel.label(node.labels.get(0)));
				// for (String label : node.labels) {
				// graphNode.addLabel(DynamicLabel.label(label));
				// }
			}
			for (QueryNode node : queryFromFile.nodes) {
				List<String> relationships = queryFromFile.relationShips.get(node.node);
				graphNode = nodesCreated.get(node.node);
				for (String relation : relationships) {
					Node otherNode = nodesCreated.get(relation);
					if (otherNode != null) {
						boolean existsRelation = ExistsRelation(graphNode, otherNode);
						if (!existsRelation) {
							graphNode.createRelationshipTo(otherNode, RelTypes.RELATED);
						}
					}
				}
			}
			tx.success();
			tx.close();
		}
		return smallGraph;
	}

	public void ReConstructGraph() {
		String fileLocation = "StoredQuery.txt";
		String currentLine = "";
		GraphDatabaseService smallGraph = null;
		HashMap<String, Node> qGraphNodes = new HashMap<>();
		try {
			BufferedReader bufferedQueryReader = new BufferedReader(new FileReader(fileLocation));
			FileUtils.deleteRecursively(new File(QUERY_PATH + "query1.db"));
			smallGraph = new GraphDatabaseFactory().newEmbeddedDatabase(QUERY_PATH + "query1.db");

			registerShutdownHook(smallGraph);
			while ((currentLine = bufferedQueryReader.readLine()) != null) {
				int noOfNodes = Integer.parseInt(currentLine);
				Node sourceNode = null;
				try (Transaction tx = smallGraph.beginTx()) {
					Node firstNode;
					Relationship relationship;
					for (int i = 0; i < noOfNodes; ++i) {
						currentLine = bufferedQueryReader.readLine();
						String[] splittedLine = currentLine.split(";");
						firstNode = smallGraph.createNode();
						// if (splittedLine.length > 0)
						// qGraphNodes.put(splittedLine[0], firstNode);
						for (int label = 1; label < splittedLine.length; ++label) {
							firstNode.addLabel(DynamicLabel.label(splittedLine[label]));
							qGraphNodes.put(splittedLine[0], firstNode);
						}
					}
					for (int i = 0; i < noOfNodes; ++i) {
						currentLine = bufferedQueryReader.readLine();
						String[] splittedLine = currentLine.split(";");
						if (splittedLine.length > 0)
							sourceNode = qGraphNodes.get(splittedLine[0]);
						for (int label = 1; label < splittedLine.length; ++label) {

							Node destinationNode = qGraphNodes.get(splittedLine[label]);
							boolean existRelation = ExistsRelation(sourceNode, destinationNode);
							if (!existRelation) {
								System.out.println(
										"creating relations: " + sourceNode + " destionation: " + destinationNode);
								sourceNode.createRelationshipTo(destinationNode, RelTypes.RELATED);
							}
						}
					}
					tx.success();
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			smallGraph.shutdown();
		}
		return;
	}

	// if relation exists then return true else false
	public boolean ExistsRelation(Node source, Node destination) {
		Iterable<Relationship> relations = destination.getRelationships();
		Iterator<Relationship> relationIterator = relations.iterator();
		while (relationIterator.hasNext()) {
			Relationship rel = relationIterator.next();
			if (rel.getOtherNode(destination).getId() == source.getId()) {
				return true;
			}
		}
		return false;
	}

	// to get general stat about the DB
	public void GetStat() {
		ResourceIterable<Node> allNodes = GlobalGraphOperations.at(this.graphDb).getAllNodes();
		Iterable<Relationship> relationships = GlobalGraphOperations.at(this.graphDb).getAllRelationships();

		for (Node node : allNodes) {
			nodeCount++;
			// System.out.println(node.getId());
		}

		System.out.println("No of Nodes: " + nodeCount);
		for (Relationship rel : relationships) {
			relationCount++;
		}
		System.out.println("No of RelationShips: " + relationCount);
	}

	public void ProcessResult() {

	}

	public void shutdown() {
		graphDb.shutdown();
	}

	public long GetRandomId(int modParam) {
		Node tempNode = null;
		long nodeId = -1;
		while (tempNode == null) {
			if (modParam == -1 || modParam < 1)
				nodeId = Math.abs(random.nextInt() % nodeCount) + indexBuff;
			else if (modParam >= 1)
				nodeId = Math.abs(random.nextInt() % modParam) + indexBuff;

			try {
				tempNode = graphDb.getNodeById(nodeId);
			} catch (NotFoundException ecv) {
				ecv.printStackTrace();
			}
		}
		return nodeId;
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	public Integer tryParse(String str) {
		Integer retVal;
		try {
			retVal = Integer.parseInt(str);
		} catch (NumberFormatException nfe) {
			retVal = null;
		}
		return retVal;
	}

	// read queried from file
	public List<QueryFromFile> getQueryFromFile(String fileName) {
		String currentLine = "";
		List<QueryFromFile> queriesFromFile = new ArrayList<QueryFromFile>();
		try {
			BufferedReader bufferedQueryReader = new BufferedReader(new FileReader(fileName));
			while ((currentLine = bufferedQueryReader.readLine()) != null) {

				Integer noOfNodes = Integer.parseInt(currentLine.trim());

				// read queryIndex;
				int queryIndex = 0;

				boolean hadQueryIndex = false;
				currentLine = bufferedQueryReader.readLine();
				String[] qStr = currentLine.split(":");
				if (qStr.length > 1) {
					queryIndex = Integer.parseInt(qStr[1].trim());
					hadQueryIndex = true;
				}

				QueryFromFile queryFromFile = new QueryFromFile(noOfNodes, queryIndex);

				for (int i = 0; i < noOfNodes; ++i) {
					List<String> listOfLabels = new ArrayList<>();
					if (i != 0 || hadQueryIndex)
						currentLine = bufferedQueryReader.readLine();
					String[] splittedLine = currentLine.split(";");
					for (int label = 1; label < splittedLine.length; ++label) {
						listOfLabels.add(new String(splittedLine[label]));
					}
					QueryNode node = new QueryNode(splittedLine[0], listOfLabels);
					queryFromFile.nodes.add(node);
				}
				for (int i = 0; i < noOfNodes; ++i) {
					currentLine = bufferedQueryReader.readLine();
					// DummyFunctions.printIfItIsInDebuggedMode(currentLine);
					String[] splittedLine = currentLine.split(";");
					if (splittedLine.length > 0) {
						String startNodeId = splittedLine[0];
						List<String> relatedNodesId = new ArrayList<>();
						for (int labelIndex = 1; labelIndex < splittedLine.length; ++labelIndex) {
							relatedNodesId.add(new String(splittedLine[labelIndex]));
						}
						queryFromFile.relationShips.put(String.valueOf(startNodeId), relatedNodesId);
					}
				}
				queryFromFile.numberOfNodes = queryFromFile.nodes.size();
				queriesFromFile.add(queryFromFile);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return queriesFromFile;
	}

}
