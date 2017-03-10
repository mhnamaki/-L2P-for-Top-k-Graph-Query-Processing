package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class ForTestingSerialization {

	public static void main(String[] args) {
		Test test = new Test();
		Test test2 = (Test) UnoptimizedDeepCopy.copy(test);
		test2.Print();

	}

}

class Test implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2886691798610839297L;
	public int k;
	public Node pivotNode;
	public float alpha;
	public final int maxK = 100;
	public org.jgrapht.DirectedGraph<Node, Relationship> graphQuery;
	public Set<Node> allStarGraphQueryNodes;
	public TreeNode<CalculationNode> leftNode;
	public TreeNode<CalculationNode> rightNode;
	StarGraphQuery stargraphquery;
	public final int numberOfPartialMatches = 1;
	public Levenshtein levenshtein;
	public boolean noAnswerForAtLeastOneOfStarQueris = false;
	public boolean noAnswerForAtLeastOneOfTreeNodes = false;
	public TreeNode<CalculationNode> rootTreeNode;
	CalculationTree calculationTree = new CalculationTree();
	ArrayList<StarGraphQuery> starQueries  = new ArrayList<StarGraphQuery>();
	HashMap<StarGraphQuery, HashMap<NodeWithValue, HashMap<Node, ArrayListWithCursor>>> possibleMatchesForThisKnowledgePivotInThisStarQuery = new HashMap<StarGraphQuery, HashMap<NodeWithValue, HashMap<Node, ArrayListWithCursor>>>(); 
	HashMap<StarGraphQuery, HashMap<NodeWithValue, PriorityQueue<StarGraphResult>>> currentLatticeResultsOfStarkForGenerateNextBestMatchOfTheSQuery = new HashMap<StarGraphQuery, HashMap<NodeWithValue, PriorityQueue<StarGraphResult>>>();

	HashMap<StarGraphQuery, HashSet<String>> latticeVisitedResultsOfStarkForGenerateNextBestMatch = new HashMap<StarGraphQuery, HashSet<String>>();

	// PriorityQueue<GraphResult> finalResults;
	ArrayList<GraphResult> finalResultsArrayList = new ArrayList<GraphResult>(); 

	HashMap<StarGraphQuery, PriorityQueue<StarGraphResult>> starkForLeafPQResults = new HashMap<StarGraphQuery, PriorityQueue<StarGraphResult>>(); 
	public int maxJoinLevel = 0;
	double finalResultLowerbound = Double.MIN_VALUE;

	public void Print() {
		System.out.println("test");
		possibleMatchesForThisKnowledgePivotInThisStarQuery.clear();
	}
}
