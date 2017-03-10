package wsu.eecs.mlkd.KGQuery.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryFromFile {
	public Integer queryIndex;
	public Integer numberOfNodes;
	public Integer numberOfEdges;
	public List<QueryNode> nodes;
	public Map<String, List<String>> relationShips;

	public QueryFromFile(Integer numberOfNodes, Integer queryIndex) {
		this.numberOfNodes = numberOfNodes;
		this.nodes = new ArrayList<>();
		this.relationShips = new HashMap<String, List<String>>();
		this.queryIndex = queryIndex;
	}
}

class QueryNode {
	String node;
	List<String> labels;

	public QueryNode(String node, List<String> labels) {
		this.node = node;
		this.labels = labels;
	}
}
