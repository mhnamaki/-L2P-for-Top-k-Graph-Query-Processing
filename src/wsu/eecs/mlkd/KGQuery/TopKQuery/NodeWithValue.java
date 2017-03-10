package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.Comparator;

import org.neo4j.graphdb.Node;

public class NodeWithValue implements Comparable<NodeWithValue> {
	public Node node;
	public float simValue;

	public NodeWithValue(Node node, float simValue) {
		this.node = node;
		this.simValue = simValue;
	}

	@Override
	public int compareTo(NodeWithValue other) {
		if (other.simValue != this.simValue) {
			return Float.compare(other.simValue, this.simValue);
		} else {
			return Long.compare(other.node.getId(), this.node.getId());
		}
	}
}

class NodeWithValueComparator implements Comparator<NodeWithValue> {
	public int compare(NodeWithValue a, NodeWithValue b) {
		if (b.simValue != a.simValue) {
			return Float.compare(b.simValue, a.simValue);
		} else {
			return Long.compare(b.node.getId(), a.node.getId());
		}
	}
}
// class NodeWithValueAndItsCursorInPQ extends NodeWithValue {
// public int cursor; // this result came from which cursorIndex on
// // b1,b2,b3,b4, ... in a stark call.
// // for lattice result we have to change the cursors based on the best last
// // result in generateNextBestMatch.
//
// public NodeWithValueAndItsCursorInPQ(Node node, float simValue) {
// super(node, simValue);
//
// }
//
// }