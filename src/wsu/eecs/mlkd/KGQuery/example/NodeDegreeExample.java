package wsu.eecs.mlkd.KGQuery.example;

import java.io.IOException;


import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;


public class NodeDegreeExample {
	
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
		NodeDegreeExample nodeDegreeExample = new NodeDegreeExample();
		nodeDegreeExample.setUp();
		int i=0;
		try ( Transaction tx = nodeDegreeExample.graphDb.beginTx() )
		{
			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(nodeDegreeExample.graphDb);
			ResourceIterable<Node> neoNodes  = globalOperation.getAllNodes();
			for ( Node item : neoNodes )
			 {
			    // System.out.println(item.getId()+" "+nodeDegree.nodeDegreeCalculate(nodeDegreeExample.graphDb, item));
				System.out.println(item.getId()+" "+item.getDegree());
				i++;
				if(i>100)
					break;
			 }
		}
		nodeDegreeExample.shutdown();
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
