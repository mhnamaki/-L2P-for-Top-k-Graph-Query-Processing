package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.neo4j.cypher.internal.compiler.v2_2.perty.print.condense;
import org.neo4j.graphdb.Relationship;

import wsu.eecs.mlkd.KGQuery.TopKQuery.NeighborIndexing;

public class PrimMST {
	Set<Long> nodes;
	Set<WeightedRelationship> weightedRelationships;
	int startingNodeId = 0;
	int[] pred;

	public PrimMST(Set<Long> nodes, Set<WeightedRelationship> weightedRelationships) {
		this.nodes = nodes;
		this.weightedRelationships = weightedRelationships;

		int maxDegree = 0;
		for (Long nodeId : nodes) {
			int tempDegree = NeighborIndexing.queryInDegreeMap.get(nodeId)
					+ NeighborIndexing.queryOutDegreeMap.get(nodeId);
			if (tempDegree > maxDegree) {
				startingNodeId = nodeId.intValue();
				maxDegree = tempDegree;
			}
		}

		pred = getMinimumSpanningTreeEdgeSet(startingNodeId);

	}

	private int[] getMinimumSpanningTreeEdgeSet(int startingNodeId) {

		final double[] dist = new double[nodes.size()]; // shortest known
														// distance to
		// MST
		final int[] pred = new int[nodes.size()]; // preceeding node in tree

		final boolean[] visited = new boolean[nodes.size()]; // all false
																// initially

		for (int i = 0; i < dist.length; i++) {
			dist[i] = Integer.MAX_VALUE;
		}
		dist[startingNodeId] = 0;

		for (int i = 0; i < dist.length; i++) {
			final int next = minVertex(dist, visited);
			visited[next] = true;

			// The edge from pred[next] to next is in the MST (if next!=s)

			Long[] in = new Long[NeighborIndexing.queryInNeighborIndicesMap.get(Long.valueOf(next)).size()];
			Long[] out = new Long[NeighborIndexing.queryOutNeighborIndicesMap.get(Long.valueOf(next)).size()];

			List<Long> listIn = new ArrayList<Long>();
			for (Long nodeId : NeighborIndexing.queryInNeighborIndicesMap.get(Long.valueOf(next)))
				listIn.add(nodeId);

			List<Long> listOut = new ArrayList<Long>();
			for (Long nodeId : NeighborIndexing.queryOutNeighborIndicesMap.get(Long.valueOf(next)))
				listOut.add(nodeId);

			listIn.toArray(in);
			listOut.toArray(out);

			final Integer[] n = new Integer[in.length + out.length];
			for (int t = 0; t < in.length; t++) {
				n[t] = in[t].intValue();
			}
			for (int t = 0; t < out.length; t++) {
				n[t + in.length] = out[t].intValue();
			}

			for (int j = 0; j < n.length; j++) {
				final int v = n[j];
				final double d = getWeights(next, v); // G.getWeight(next, v);
				if (dist[v] > d) {
					dist[v] = d;
					pred[v] = next;
				}
			}
		}
		return pred; // (ignore pred[s]==0!)

	}

	private double getWeights(int next, int v) {
		for (WeightedRelationship wr : weightedRelationships) {
			if ((wr.srcId == next && wr.destId == v) || (wr.destId == next && wr.srcId == v)) {
				return wr.weight;
			}
		}
		return Integer.MAX_VALUE;
	}

	private static int minVertex(double[] dist, boolean[] v) {
		double x = Integer.MAX_VALUE;
		int y = -1; // graph not connected, or no unvisited vertices
		for (int i = 0; i < dist.length; i++) {
			if (!v[i] && dist[i] < x) {
				y = i;
				x = dist[i];
			}
		}
		return y;
	}

	public Double getMinimumSpanningTreeTotalWeight() {
		double totalSum = 0;

		for (int i = 0; i < pred.length; i++) {
			if (i == startingNodeId)
				continue;
			totalSum += getWeights(pred[i], i);
		}

		return totalSum;
	}

}
