package wsu.eecs.mlkd.KGQuery.example;

import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import wsu.eecs.mlkd.KGQuery.algo.ShortestPath;


public class ShortestPathExample {
	
	private static final String DB_PATH = "/Users/qsong/Downloads/graph.db";
	private GraphDatabaseService graphDb;
	private static final String NAME_KEY = "name";
	
	public void setUp() throws IOException
	{
	//FileUtils.deleteRecursively( new File( DB_PATH ) );
	graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
	registerShutdownHook(graphDb);
	}
	
	public void shutdown()
	{
	graphDb.shutdown();
	}
	
	
	public static void main(String[] args) throws IOException {
		ShortestPathExample shortestPathExample = new ShortestPathExample();
		shortestPathExample.setUp();

		ShortestPath shortestPath = new ShortestPath();
		try ( Transaction tx = shortestPathExample.graphDb.beginTx() )
		{
			Node node1 = shortestPathExample.graphDb.getNodeById(19);
			Node node2 = shortestPathExample.graphDb.getNodeById(1);
			shortestPath.calculateShortestPath(shortestPathExample.graphDb, node1, node2, NAME_KEY);
		}
		shortestPathExample.shutdown();
	}
	
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running application).
	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run()
	        {
	            graphDb.shutdown();
	        }
	    } );
	}
	
	
}
