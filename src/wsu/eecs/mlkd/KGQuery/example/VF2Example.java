package wsu.eecs.mlkd.KGQuery.example;


import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy.DummyFunctions;
import wsu.eecs.mlkd.KGQuery.algo.VF2.*;

public class VF2Example {
	
//	private static final String MODELGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/VF2modelgraph.db";
//	private static final String PATTERNGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/VF2patterngraph.db";
//	
//	private static final String MODELGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/knowledge3Nodes.db";
//	private static final String PATTERNGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/pattern3Nodes.db";
	
	private static final String MODELGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/starFrameWorkTestG1Prime.db";
    private static final String PATTERNGRAPH_DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/starFrameWorkTestQ1.db";
	
	
	public static void main(String[] args) {
		
//		if(args.length < 2)
//			System.out.println("Input modelgraph path and pattern graph path:");
//		GraphDatabaseService g1 = new GraphDatabaseFactory().newEmbeddedDatabase( MODELGRAPH_DB_PATH );
//		registerShutdownHook(g1);
//		GraphDatabaseService g2 = new GraphDatabaseFactory().newEmbeddedDatabase( PATTERNGRAPH_DB_PATH );
//		registerShutdownHook(g2);
//
//		try ( Transaction tx1 = g1.beginTx() ){
//			try (Transaction tx2 = g2.beginTx() ){
//				//DummyFunctions.createUniqueIdForEachNode(g1, g2);
//				VF2Matcher matcher = new VF2Matcher();
//				matcher.match(g1, g2); // starts pattern search
//				tx2.success();
//			}
//			tx1.success();
//		}
//		finally {
//			g1.shutdown();
//			g2.shutdown();	
//		}
		
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
