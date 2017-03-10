package wsu.eecs.mlkd.KGQuery.TopKQuery;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class StarFrameworkExample {
	private static final String MODELGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/knowledge3Nodes.db";
	private static final String PATTERNGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/pattern3Nodes.db";

	public static void main(String[] args) {
//
//		GraphDatabaseService queryGraph = new GraphDatabaseFactory().newEmbeddedDatabase(PATTERNGRAPH_DB_PATH);
//		registerShutdownHook(queryGraph);
//		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(PATTERNGRAPH_DB_PATH);
//		registerShutdownHook(knowledgeGraph);
//
//		try (Transaction tx1 = queryGraph.beginTx()) {
//			try (Transaction tx2 = knowledgeGraph.beginTx()) {
//				StarFramework sf = new StarFramework(queryGraph, knowledgeGraph, 3);
//				sf.starRun();
//				tx1.success();
//			} catch (Exception exc) {
//				exc.printStackTrace();
//			} finally {
//				queryGraph.shutdown();
//			}
//		} catch (Exception exc) {
//
//		}
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
