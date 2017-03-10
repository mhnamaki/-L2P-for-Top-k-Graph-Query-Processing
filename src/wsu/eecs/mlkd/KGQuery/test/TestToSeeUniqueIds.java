package wsu.eecs.mlkd.KGQuery.test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;

public class TestToSeeUniqueIds {

	public static String MODELGRAPH_DB_PATH = "/Users/mnamaki/Documents/workspace/wsu.eecs.mlkd.KGQuery/dblp.db";
	
	public static void main(String[] args) {
		//if(args.length<=0){}
		//MODELGRAPH_DB_PATH = args[0];
		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(MODELGRAPH_DB_PATH);
		registerShutdownHook(knowledgeGraph);
		try (Transaction tx2 = knowledgeGraph.beginTx()) {
			long maxOriginalId = 0;
			ResourceIterable<Node> allNodes = GlobalGraphOperations.at(knowledgeGraph).getAllNodes();

			for (Node nodeItem : allNodes) {
				System.out.println(nodeItem.getProperty(DummyProperties.uniqueManualIdString));
			}
			
			tx2.success();
			//tx2.close();
			
			System.out.println("finished properly");
		}
		knowledgeGraph.shutdown();
		System.out.println("shut down properly");
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
