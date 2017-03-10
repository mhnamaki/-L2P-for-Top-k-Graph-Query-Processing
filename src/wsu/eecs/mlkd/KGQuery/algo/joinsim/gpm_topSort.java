package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Vector;

import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_edge;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_graph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_node;

//This class performs topological sorting on 
//a general graph -- with well-defined rankings.
//ranking method from: 
//"From Bisimulation to Simulation", R.Gentilini, C.Piazza & A.Policriti

public class gpm_topSort {

	public gpm_graph G; //graph to be sorted.
	public gpm_graph SccG;
	public List<DirectedSubgraph<gpm_node,gpm_edge>> sccsubGraphs;
	public Vector<gpm_node> sortednodes;
	public Vector<gpm_node> sortedsccnodes;

	public gpm_topSort(){
	}

	public gpm_topSort(gpm_graph g){
		G = g;
		topSort();
	}
	
	
	//	============================================================================//
	//	This function computes the ranking for computing bisimulation. See "A fast bisimulation algorithm"
	//	============================================================================//
	public void compRank(){
		//SccG = G.sccDAG();
		
	}
	

	//	============================================================================//
	//	This function defines comparator for gpm_nodes
	//	============================================================================//
	public class gpm_nodeComparator implements Comparator<gpm_node>{
		@Override
		public int compare(gpm_node a, gpm_node b){
			if (a.weight < b.weight){
				return -1;
			}
			if (a.weight > b.weight){
				return 1;
			}
			return 0;
		}
	}

	//	============================================================================//
	//	This function defines comparator for gpm_nodes
	//	============================================================================//
	public class gpm_edgeComparator implements Comparator<gpm_edge>{
		@Override
		public int compare(gpm_edge a, gpm_edge b){
			gpm_node tna = G.getEdgeTarget(a);
			gpm_node tnb = G.getEdgeTarget(b);
			Comparator<gpm_node> comparator = new gpm_nodeComparator();
			if(comparator.compare(tna,tnb)==-1)
				return -1;
			else if(comparator.compare(tna,tnb)==1)
				return 1;
			else{
				if ((Integer)a.e_bound < (Integer)b.e_bound){
					return -1;
				}
				if ((Integer)a.e_bound > (Integer)b.e_bound){
					return 1;
				}
			}
			return 0;
		}
	}

	//main process for topological sorting
	//returns a vector of sccnodes, reversed top-order.
	@SuppressWarnings({ "unchecked" })
	public void topSort(){
		SccG = G.sccDAG();

		TopologicalOrderIterator topiter = 
			new TopologicalOrderIterator(SccG);

		int lev = SccG.vertexSet().size();
		SccG.ClearNodeWeight();
		//the following utilize the scc iteration.
		//may be improved by Kosaraju and Sharir's SCC algorithm.
		while(topiter.hasNext()){
			lev--;
			gpm_node n = (gpm_node) topiter.next();
			n.weight = lev;
			HashSet<gpm_node> escc = G.expSCC(n);
			for(gpm_node s:escc)
				s.weight = lev;
		}
		if(sortednodes==null){
			sortednodes = new Vector<gpm_node>();
		}
		
		if(sortedsccnodes==null){
			sortedsccnodes = new Vector<gpm_node>();
		}
		
		sortedsccnodes.addAll(SccG.vertexSet());

		for(gpm_node n:SccG.vertexSet()){
			HashSet<gpm_node> escc = G.expSCC(n);
			sortednodes.addAll(escc);
		}
		Comparator<gpm_node> comparator = new gpm_nodeComparator();
		Collections.sort(sortednodes, comparator);
		Collections.sort(sortedsccnodes,comparator);

		sccsubGraphs = G.sccIns.stronglyConnectedSubgraphs();
	}

	//return sorted edge vector.
	public Vector<gpm_edge> sortEdge(){
		Vector<gpm_edge> evec = new Vector<gpm_edge>();
		evec.addAll(G.edgeSet());
		Comparator<gpm_edge> comparator = new gpm_edgeComparator();
		Collections.sort(evec, comparator);
		return evec;
	}


	//compute rankings
	public void compRanking(){


	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

