package wsu.eecs.mlkd.KGQuery.algo;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.traversal.Paths;

public class ShortestPath
{
    

    public void calculateShortestPath(GraphDatabaseService graphDb, Node node1, Node node2, String nodePropertyKey )
    {
        
            // START SNIPPET: shortestPathUsage
            PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                    PathExpanders.forDirection(Direction.BOTH ), 4 );
            Path foundPath = finder.findSinglePath( node1, node2 );
            System.out.println( "Path from Neo to Agent Smith: "
                                + Paths.simplePathToString( foundPath, nodePropertyKey ) );
            // END SNIPPET: shortestPathUsage
        }
    
}
