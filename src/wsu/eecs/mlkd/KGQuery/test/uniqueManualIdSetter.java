package wsu.eecs.mlkd.KGQuery.test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyProperties;

public class uniqueManualIdSetter {

	public static String MODELGRAPH_DB_PATH = "/Users/mnamaki/Documents/workspace/wsu.eecs.mlkd.KGQuery/dblp.db";

	public static void main(String[] args) {
		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory()

				.newEmbeddedDatabaseBuilder(MODELGRAPH_DB_PATH)
				.setConfig("allow_store_upgrade", "true")
				.newGraphDatabase();

		registerShutdownHook(knowledgeGraph);
		try (Transaction tx2 = knowledgeGraph.beginTx()) {

			ResourceIterable<Node> allNodes = GlobalGraphOperations.at(knowledgeGraph).getAllNodes();

			int uniqueManualId = 0;

			for (Node nodeItem : allNodes) {
				uniqueManualId++;
				nodeItem.setProperty(DummyProperties.uniqueManualIdString, uniqueManualId);
				System.out.println(uniqueManualId);
			}
			tx2.success();
			System.out.println("properly finished");
		} catch (Exception exc) {
			exc.printStackTrace();
		} finally {
			knowledgeGraph.shutdown();
			System.out.println("shut down properly");
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