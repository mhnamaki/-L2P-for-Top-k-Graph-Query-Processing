package wsu.eecs.mlkd.KGQuery.example;

import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import wsu.eecs.mlkd.KGQuery.algo.PageRank;

public class PageRankExample {
	
//	private static final String DB_PATH = "/Users/qsong/Downloads/graph.db_sample";
	private static final String DB_PATH = "/Users/qsong/Downloads/graph.db";
	private GraphDatabaseService graphDb;
	
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
		PageRankExample pageRankExample = new PageRankExample();
		pageRankExample.setUp();
		PageRank pagerank = new PageRank();
		long timeStart = 0;
		long timeEnd = 0;
		timeStart = System.currentTimeMillis();
		try ( Transaction tx = pageRankExample.graphDb.beginTx() )
		{
			pagerank.calculatePageRank(pageRankExample.graphDb);
		}
		timeEnd = System.currentTimeMillis();
		 if (timeEnd > timeStart) {
	        	System.err.println("Time   : " + ((timeEnd - timeStart) / 1000.0) + " seconds");
	        }
		pageRankExample.shutdown();
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
