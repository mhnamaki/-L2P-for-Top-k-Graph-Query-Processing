package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.jgrapht.alg.DirectedNeighborIndex;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.tooling.GlobalGraphOperations;
import org.apache.commons.math3.util.Combinations;

public class BestMatch implements Comparable<BestMatch> {
	ArrayList<QueryKnowledgeNodeWithItsCursorAndValue> queryKnowledgeNeighborNodes;
	float totalValue;

	public BestMatch(ArrayList<QueryKnowledgeNodeWithItsCursorAndValue> queryKnowledgeNeighborNodes, float totalValue) {
		this.queryKnowledgeNeighborNodes = queryKnowledgeNeighborNodes;
		this.totalValue = totalValue;
	}

	@Override
	public int compareTo(BestMatch other) {
		return Float.compare(other.totalValue, this.totalValue);
	}
}
class QueryKnowledgeNodeWithItsCursorAndValue{
	Node queryNode;
	Node knowledgeNode;
	int cursor;
	float nodeValue;
	public QueryKnowledgeNodeWithItsCursorAndValue(Node queryNode, Node knowledgeNode,float nodeValue,int cursor){
		this.queryNode = queryNode;
		this.knowledgeNode = knowledgeNode;
		this.cursor = cursor;
		this.nodeValue = nodeValue;
	}
}
