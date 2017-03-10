package wsu.eecs.mlkd.KGQuery.example;

import java.io.IOException;
import java.util.HashSet;

import org.neo4j.graphdb.*;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.impl.util.DoubleComparator;
import org.neo4j.graphalgo.impl.centrality.NetworkDiameter;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;



public class GraphDiameterExample {
	
	private static final String DB_PATH = "/Users/qsong/Downloads/graph.db";
	private GraphDatabaseService graphDb;
	
	public static enum MyRelTypes implements RelationshipType
	     {
		RelationshipTypeToken
	     }
	
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
	
	protected static SingleSourceShortestPath<Double> getSingleSourceShortestPath()
    {
        return new SingleSourceShortestPathDijkstra<Double>( 0.0, null,
            new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                            Direction direction )
                {
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(),
            Direction.BOTH, MyRelTypes.RelationshipTypeToken);
    }
	
	public static void main(String[] args) throws IOException{
		GraphDiameterExample graphDiameterExample = new GraphDiameterExample();
		graphDiameterExample.setUp();
		try ( Transaction tx = graphDiameterExample.graphDb.beginTx() )
		{
			HashSet<Node> set = new HashSet<Node>();
			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphDiameterExample.graphDb);
			ResourceIterable<Node> neoNodes  = globalOperation.getAllNodes();
			for(Node item : neoNodes)
			{
					set.add(item);
			}
			NetworkDiameter<Double> graphDiameter = new NetworkDiameter<Double>(getSingleSourceShortestPath(), 0.0, set,
		            new DoubleComparator());
			System.out.println(graphDiameter.getCentrality(null));
		}

		graphDiameterExample.shutdown();
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
