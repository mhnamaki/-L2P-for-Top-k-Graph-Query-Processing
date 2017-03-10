package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;







//import org._3pq.jgrapht.DefaultDirectedGraph;
//import org._3pq.jgrapht.alg.StrongConnectivityInspector;
import org.jgrapht.DirectedGraph;
//import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.alg.StrongConnectivityInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.Subgraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
//import org.jgrapht.alg.FloydWarshallShortestPaths;

//import Jama.*;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.MyFileOperation;

public class gpm_graph extends DefaultDirectedWeightedGraph<gpm_node, gpm_edge>{

	public String gfilename = "";

	public int gtype = 0; // pattern: 0; datagraph: 1

	public int patternId = 0; //pattern Id, 0 for datagraph, >= 1 for pattern
	
	public HashMap<String, String> schema; // schema on vertex. format: attr:type
	//	public Vector<Comparator> complist; // comparator of schema -- for index.

	public Vector<String> relschema;//schema on edges: ie, colors

	public int graphId;
	
	//distance matrix.
	//	public gpm_distMatrix M = null;
	public gpm_distFWMatrix FS = null;

	//distance index buffer.
	//increasing at runtime.
	//may introduce a new issue: buffer management and replacement algorithm.
	//based on a cost model.
	public gpm_distBuffer dBuff = null;
	public int distbuffsize = 0;
	
	public double interestingness = 0;
	public StrongConnectivityInspector<gpm_node, gpm_edge> sccIns; 

	public long distQtime = 0;
	public long distQtime2 = 0;
	public long distQtimeDM = 0;
	public long distQtime3 = 0;

	public int libattsize = 3;// data nodes contains 3 types of nodes
	public int[] rangeL = new int[3];
	public int[] rangeU = new int[3];

	//stores the hash result for minHash
	public int[] hashResult;
	
	//Stores current match set for nodes
	public TreeMap<String, Vector<String>> simMatchSet; 
	
	private static final long serialVersionUID = 1L;

	public gpm_graph() {
		super(gpm_edge.class);
	}




	//	============================================================================//
	//	This function returns attribute type
	//	============================================================================//
	public String attrtype(String attname){
		return this.schema.get(attname);
	}

	//	============================================================================//
	//	This function casts gpm_graph into directedgraph class.  
	//	============================================================================//
	@SuppressWarnings("unchecked")
	public DirectedGraph Cast2DG(){ 
		DirectedGraph<gpm_node,gpm_edge> DG = 
			new DefaultDirectedGraph<gpm_node, gpm_edge>(gpm_edge.class);
			for(gpm_node n: this.vertexSet())
				DG.addVertex(n);
			for(gpm_edge e:this.edgeSet()){
				DG.addEdge(this.GetVertex(e.from_node),this.GetVertex((e.to_node)),e);
			}
			return DG;
	}

	//	============================================================================//
	//	This function clears the additional information of nodes 
	//	============================================================================//
	public void ClearNodeInfo(){
		for(gpm_node n: this.vertexSet())
			n.addinfo = "";
	}

	//	============================================================================//
	//	This function clears the weights of nodes 
	//	============================================================================//
	public void ClearNodeWeight(){
		for(gpm_node n: this.vertexSet())
			n.weight = 0.0;
	}

	//	============================================================================//
	//	This function inserts a user-defined node into graph 
	//	============================================================================//
	public boolean InsertNode(gpm_node n){
		//n.tag = ""+(this.vertexSet().size()+1);
		return(this.addVertex(n));
	}

	//	============================================================================//
	//	This function removes a user-defined edge from graph 
	//	============================================================================//
	public boolean removeEdge(gpm_node a, gpm_node b, int color){
		gpm_edge e = this.getEdge(a, b);
		if(e==null||e.e_color!=color)
			return false;
		else
			return this.removeEdge(e);
	}

	//	============================================================================//
	//	This function removes a random edge from graph 
	//	============================================================================//
	public gpm_edge removeRandomEdge(gpm_node mnode){
		//		gpm_node a = this.getAnode();
		//		gpm_node b = this.getAnode();
		//		while(this.getEdge(a, b)==null){
		//			 a = this.getAnode();
		//			 b = this.getAnode();
		//		}
		double rate = 1.0;
		gpm_edge e = null;
		if(Math.random()>rate)
			e = this.getAnedge();
		else{
			Vector<gpm_edge> cedges = this.GetChildEdge(mnode, 0);
			if(cedges.size()>0){
				e = cedges.elementAt((int)(Math.random()*cedges.size()));
			}
			else{
				cedges.clear();
				cedges = this.GetParentEdge(mnode, false, 0);
				if(cedges.size()>0){
					e = cedges.elementAt((int)(Math.random()*cedges.size()));
				}
				else{
					e = this.getAnedge();
				}
			}

		}
		this.removeEdge(e);
		return e;
	}


	//	============================================================================//
	//	This function inserts a set of user-defined nodes into an edge of the graph 
	//	i.e: extend an edge into a path.
	//	============================================================================//
	public boolean extendEdge(Vector<gpm_node> nlist, gpm_node fn, gpm_node tn){
		if(this.getEdge(fn, tn)==null)
			return false;
		gpm_edge e = (gpm_edge)this.removeEdge(fn, tn);
		for(gpm_node n: nlist)
			this.InsertNode(n);
		this.InsertEdge(fn, nlist.elementAt(0), 1, e.e_color);
		this.InsertEdge(nlist.lastElement(),tn,1, e.e_color);
		for(int i=0;i<nlist.size()-1;i++){
			this.InsertEdge(nlist.elementAt(i), nlist.elementAt(i+1), 1, e.e_color);
		}
		return true;
	}

	//	============================================================================//
	//	This function copy a node num times. 
	//	============================================================================//
	public void copyNode(gpm_node a, int num, int maxenum){
		int nodenum = this.vertexSet().size();
		Vector<gpm_node> pset = this.GetParents(a, false,-1);
		Vector<gpm_node> cset = this.GetChildren(a,-1);
		for(int i=0;i<num;i++){
			gpm_node ai = new gpm_node();
			ai.exactcopyPN(a);
			ai.tag = ""+(int)(nodenum+i);
			this.InsertNode(ai);
			int ecolor = 0;
			int eaddcount = 0;
			//			for(gpm_node pn:pset){
			//				if(this.GetEdge(pn.tag, a.tag)==null){
			//					System.out.println("here.");
			//					break;
			//				}
			//				ecolor = this.GetEdge(pn.tag, a.tag).e_color;
			//				this.InsertEdge(pn, ai, this.GetEdge(pn.tag, a.tag).e_bound, ecolor);
			//				eaddcount++;
			//				if(eaddcount>=maxenum)
			//					break;
			//			}
			eaddcount = 0;
			for(gpm_node cn:cset){
				if(this.GetEdge(a.tag, cn.tag)==null){
					System.out.println("here.");
					break;
				}
				ecolor = this.GetEdge(a.tag, cn.tag).e_color;
				this.InsertEdge(ai, cn, this.GetEdge(a.tag, cn.tag).e_bound, ecolor);
				eaddcount++;
				//				if(eaddcount>=maxenum)
				//					break;
			}
		}
	}


	//	============================================================================//
	//	This function copy a pattern node num times. 
	//	============================================================================//
	public void copyPNode(gpm_node a, int num){
		int nodenum = this.vertexSet().size();
		Vector<gpm_node> pset = this.GetParents(a, false,-1);
		Vector<gpm_node> cset = this.GetChildren(a,-1);
		for(int i=0;i<num;i++){
			gpm_node ai = new gpm_node(a);
			//ai.copyPN(a);
			ai.tag = ""+(int)(nodenum+i);
			this.InsertNode(ai);
			int ecolor = 0;
			//			for(gpm_node pn:pset){
			//				if(this.GetEdge(pn.tag, a.tag)==null){
			//					System.out.println("here.");
			//				}
			//				ecolor = this.GetEdge(pn.tag, a.tag).e_color;
			//				this.InsertEdge(pn, ai, this.GetEdge(pn.tag, a.tag).e_bound, ecolor);
			//			}
			for(gpm_node cn:cset){
				if(this.GetEdge(a.tag, cn.tag)==null){
					System.out.println("here.");
				}
				ecolor = this.GetEdge(a.tag, cn.tag).e_color;
				this.InsertEdge(ai, cn,this.GetEdge(a.tag, cn.tag).e_bound, ecolor);
			}
		}
	}

	//	============================================================================//
	//	This function inserts a user-defined edge into graph 
	//	============================================================================//
	public boolean InsertEdge(gpm_node a, gpm_node b, int ebound, int color){
		gpm_edge e = new gpm_edge(a.tag,b.tag, ebound, color);
		return(this.addEdge(a, b, e));
	}

	//	============================================================================//
	//	This function inserts an edge according to node tag 
	//	============================================================================//
	//	public boolean InsertEdge(String a, String b, double ebound){
	//		gpm_edge e = new gpm_edge(a, b, ebound);
	//		gpm_node na = this.GetVertex(a);
	//		gpm_node nb = this.GetVertex(b);
	//		return(this.addEdge(na, nb, e));
	//	}

	//	============================================================================//
	//	This function inserts a user-defined edge into graph  
	//	============================================================================//
	public boolean InsertEdge(gpm_edge e){
		if(e!=null){
			gpm_node a = this.GetVertex(e.from_node);
			gpm_node b = this.GetVertex(e.to_node);
			return(this.addEdge(a, b, e));
		}
		return false;
	}

	//	============================================================================//
	//	This function inserts an random edge into graph  
	//	============================================================================//
	public gpm_edge InsertRandomEdge(gpm_node cn){

		double rate = 0.5;
		gpm_edge e = null;
		if(Math.random()<rate){
			gpm_node b = this.getAnode();
			while(this.getEdge(cn, b)!=null)
				b = this.getAnode();
			e = new gpm_edge(cn.tag, b.tag, 1,0);
		}
		else{
			gpm_node a = this.getAnode();
			gpm_node b = this.getAnode();
			while(this.getEdge(a, b)!=null){
				a = this.getAnode();
				b = this.getAnode();
			}
			e = new gpm_edge(a.tag,b.tag,1,0);
		}
		this.InsertEdge(e);
		return e;
	}

	//	============================================================================//
	//	This function gets vertex wrt node tag
	//	============================================================================//
	public gpm_node GetVertex(String ntag){
		for(gpm_node n: this.vertexSet()){
			if(n.tag.equals(ntag))
				return n;
		}
		return null;
	}

	//	============================================================================//
	//	This function removes all self-loops
	//	============================================================================//
	public void clearSLoops(){
		Vector<gpm_edge> eset = new Vector<gpm_edge>();
		eset.addAll(this.edgeSet());
		for(int i=0;i<eset.size();i++){
			if(eset.elementAt(i).from_node.equals(eset.elementAt(i).to_node)){
				this.removeEdge(eset.elementAt(i));
			}
		}
	}

	//	============================================================================//
	//	This function returns the degree info of the graph
	//	0: min deg
	//	1: aver deg
	//	2: max deg
	//	============================================================================//
	public int averDeg(int option){
		int mindeg = 1000000;
		int maxdeg = -1;
		int averdeg = 0;
		for(gpm_node n:this.vertexSet()){
			int deg = this.inDegreeOf(n)+ this.outDegreeOf(n);
			if(deg>maxdeg) maxdeg = deg;
			if(deg<mindeg) mindeg = deg;
			averdeg+=deg;
		}
		averdeg=averdeg/this.vertexSet().size();
		if(option==0)
			return mindeg;
		if(option==1)
			return averdeg;
		if(option==2)
			return maxdeg;
		return 0;
	}



	//	============================================================================//
	//	This function gets a random node
	//	============================================================================//
	public gpm_node getAnode(){
		Vector<gpm_node> vlist = new Vector<gpm_node>();
		vlist.addAll(this.vertexSet());
		return vlist.elementAt((int)(Math.random()*vlist.size()));
	}

	//	============================================================================//
	//	This function gets a random edge
	//	============================================================================//
	public gpm_edge getAnedge(){
		Vector<gpm_edge> elist = new Vector<gpm_edge>();
		elist.addAll(this.edgeSet());
		return elist.elementAt((int)(Math.random()*elist.size()));
	}

	//	============================================================================//
	//	This function gets edge wrt source-sink node tags
	//	============================================================================//
	public gpm_edge GetEdge(String fid, String tid){
		gpm_node fn  = this.GetVertex(fid);
		gpm_node tn = this.GetVertex(tid);
		return (gpm_edge)this.getEdge(fn, tn);
	}

	//	============================================================================//
	//	This function gets incoming edges (sorted) of a node
	//	============================================================================//
	public Vector<gpm_edge> GetParentEdge(gpm_node n, boolean sort, int color){
		Vector<gpm_node> pnvec = this.GetParents(n, sort, color);
		Vector<gpm_edge> envec = new Vector<gpm_edge>();
		for(gpm_node pn: pnvec){
			gpm_edge e = this.getEdge(pn, n);
			envec.add(e);
		}
		return envec;
	}

	//	============================================================================//
	//	This function gets outcoming edges (sorted) of a node
	//	============================================================================//
	public Vector<gpm_edge> GetChildEdge(gpm_node n, int color){
		Vector<gpm_node> cnvec = this.GetChildren(n, color);
		Vector<gpm_edge> envec = new Vector<gpm_edge>();
		for(gpm_node cn: cnvec){
			gpm_edge e = this.getEdge(cn, n);
			if(e!=null)
				envec.add(e);
		}
		return envec;
	}

	//	============================================================================//
	//	This function gets outcoming edges (sorted) of a node set
	//	============================================================================//
	public HashSet<gpm_edge> GetChildEdgeSet(HashSet<gpm_node> nset){
		HashSet<gpm_edge> eset = new HashSet<gpm_edge>();
		for(gpm_node n:nset)
			eset.addAll(this.outgoingEdgesOf(n));
		return eset;
	}


	//	============================================================================//
	//	This function gets parent set of a node 
	//	if sort, the parents will be sorted according to edge weights
	//  if color = -1, all colored edges will be added.
	//	============================================================================//
	public Vector<gpm_node> GetParents(gpm_node n, boolean sort, int color){
		Vector<gpm_node> pset = new Vector<gpm_node>();
		Vector<Integer> bound = new Vector<Integer>();
		for(gpm_edge e: this.incomingEdgesOf(n)){
			if(((gpm_edge)e).e_color==color){
				pset.add((gpm_node)this.getEdgeSource(e));
			}
			else if(color==-1)
				pset.add((gpm_node)this.getEdgeSource(e));
			if(sort){
				bound.add(e.e_bound);
				for(int i = bound.size()-1; i>0;i--){
					if(bound.elementAt(i).compareTo(bound.elementAt(i-1))>0){
						Integer tmp = bound.elementAt(i-1);
						bound.set(i-1, bound.elementAt(i));
						bound.set(i, tmp);
						gpm_node tmpn = pset.elementAt(i-1);
						pset.set(i-1, pset.elementAt(i));
						pset.set(i, tmpn);
					}
				}
			}
		}
		return pset;
	}


	//	============================================================================//
	//	This function gets parent set of a node 
	//	if sort, the parents will be sorted according to edge weights
	//	============================================================================//
	public HashSet<gpm_node> GetParentsSet(HashSet<gpm_node> nset,  int color){
		HashSet<gpm_node> psets = new HashSet<gpm_node>();
		//		HashSet<gpm_node> center = new HashSet<gpm_node>();
		//		center.addAll(nset);
		for(gpm_node n: nset){
			psets.addAll(GetParents(n,false,color));
		}
		return psets;
	}

	//	============================================================================//
	//	This function gets child set of a node 
	//	============================================================================//
	public Vector<gpm_node> GetChildren(gpm_node n, int color){
		Vector<gpm_node> cset = new Vector<gpm_node>();
		for(gpm_edge e:this.outgoingEdgesOf(n)){
			if(color!=-1 && ((gpm_edge)e).e_color==color){
				cset.add((gpm_node)this.getEdgeTarget(e));
			}
			else if(color==-1)
				cset.add((gpm_node)this.getEdgeTarget(e));
		}
		return cset;
	}

	//	============================================================================//
	//	This function gets child set of a node set
	//	============================================================================//
	public HashSet<gpm_node> GetChildrenSet(HashSet<gpm_node> nset, int color){
		HashSet<gpm_node> cset = new HashSet<gpm_node>();
		for(gpm_node n: nset){
			cset.addAll(GetChildren(n,color));
		}
		return cset;
		//		for(gpm_edge e:this.outgoingEdgesOf(n)){
		//			if(color!=-1 && ((gpm_edge)e).e_color==color){
		//				cset.add((gpm_node)this.getEdgeTarget(e));
		//			}
		//			else if(color==-1)
		//				cset.add((gpm_node)this.getEdgeTarget(e));
		//		}
		//		return cset;
	}


	//	============================================================================//
	//	This function gets all the reachable nodes from a set of node
	//	============================================================================//
	public HashSet<gpm_node> GetReachableNSet(HashSet<gpm_node> nset){
		HashSet<gpm_node> rset = new HashSet<gpm_node>();
		//rset.addAll(nset);
		HashSet<gpm_node> newneighbors = new HashSet<gpm_node>();
		newneighbors.addAll(nset);
		HashSet<gpm_node> tmpPset = new HashSet<gpm_node>();
		tmpPset.addAll(nset);

		while(newneighbors.size()>0){
			rset.addAll(newneighbors);
			newneighbors.clear();
			newneighbors.addAll(this.GetChildrenSet(tmpPset, -1));
			newneighbors.removeAll(tmpPset);
			tmpPset.clear();
			tmpPset.addAll(newneighbors);
		}
		return rset;
	}


	//	============================================================================//
	//	This function reverses a graph
	//	============================================================================//
	public void Reverse(){
		for(gpm_edge e: this.edgeSet()){
			gpm_node a = this.getEdgeSource(e);
			gpm_node b = this.getEdgeTarget(e);
			this.addEdge(b, a, e);
			this.removeEdge(a, b);
		}
	}





	//	============================================================================//
	//	This function returns node induced subgraph given a set of nodes
	//	============================================================================//
	public gpm_graph nsubGraph(HashSet<gpm_node> nset){
		gpm_graph nsub = new gpm_graph();
		for(gpm_node n: nset){
			nsub.InsertNode(n);
		}
		for(gpm_node a: nset){
			for(gpm_node b:nset){
				if(this.getEdge(a, b)!=null){
					nsub.InsertEdge(this.getEdge(a, b));
				}
			}
		}
		nsub.gfilename = this.gfilename;
		return nsub;
	}

	//	============================================================================//
	//	This function returns edge induced subgraph given a set of edges
	//	============================================================================//
	public gpm_graph esubGraph(HashSet<gpm_edge> eset){
		gpm_graph esub = new gpm_graph();
		for(gpm_edge e: eset){
			esub.addVertex(this.getEdgeSource(e));
			esub.addVertex(this.getEdgeTarget(e));
			esub.InsertEdge(e);
		}
		return esub;
	}


	//	============================================================================//
	//	This function returns a subgraph induced by all the reachable nodes from a node set
	//	See "Fast bisimulation algorithm"
	//	============================================================================//
	public gpm_graph rsubGraph(HashSet<gpm_node> nset){
		gpm_graph esub = new gpm_graph();
		HashSet<gpm_node> rnset = this.GetReachableNSet(nset);
		HashSet<gpm_edge> reset = this.GetChildEdgeSet(rnset);
		for(gpm_node n: rnset)
			esub.InsertNode(n);
		for(gpm_edge e: reset)
			esub.InsertEdge(e);
		return esub;
	}








	//	============================================================================//
	//	This function returns the DAG with each SCC a node.
	//  This function deal with small pattern graphs. 
	//	Each scc node has add info as "0_1_2_.._k" with 0-k the node id.
	//	This function also computes bisimulation ranks. See "the fast bisimulation algorithm"
	//	============================================================================//
	@SuppressWarnings("unchecked")
	public HashMap<Integer, HashSet<gpm_node>> sccDAGwithrank(){//Vector<HashSet<gpm_node>>

		Vector<HashSet<gpm_node>> C = new Vector<HashSet<gpm_node>>();
		HashMap<Integer, HashSet<gpm_node>> rankmap = new HashMap<Integer, HashSet<gpm_node>>();

		DirectedGraph dg = this.Cast2DG();
		gpm_graph sccDAG = new gpm_graph();
		if(sccIns==null)
			sccIns = new StrongConnectivityInspector(dg);
		List<Set<gpm_node>> sccsets = sccIns.stronglyConnectedSets();


		HashSet<gpm_node> WF = new HashSet<gpm_node>();
		HashSet<gpm_node> NWF = new HashSet<gpm_node>();


		Vector<gpm_node> sccnodes = new Vector<gpm_node>();
		HashSet<gpm_edge> sccedges = new HashSet<gpm_edge>();
		//int sccid = 0;

		for(int i=0;i<sccsets.size();i++){

			Set<gpm_node> s = sccsets.get(i);

			for(gpm_node n: s){
				n.addinfo = ""+i;
			}

			gpm_node scc = new gpm_node();
			//System.out.println("SCC:" + (i));
			//scc.tag = ""+sccid;
			for(gpm_node n: s){
				scc.addinfo = scc.addinfo + n.tag + "_";
			}
			scc.weight = s.size();
			scc.addinfo = scc.addinfo.substring(0,scc.addinfo.lastIndexOf("_"));
			scc.tag = ""+i;
			//sccnodes.add(scc);
			sccDAG.addVertex(scc);

			//			if(s.size()>1)
			//				sccedges.add(new gpm_edge(sccnodes.elementAt(i).tag, sccnodes.elementAt(i).tag,1,0));

			if(sccsets.get(i).size()>1)
				//NWF.addAll(sccsets.get(i));
				NWF.add(scc);
		}


		for(gpm_edge e: this.edgeSet()){
			String src = this.getEdgeSource(e).addinfo;
			String trg = this.getEdgeTarget(e).addinfo;
			if(!src.equals(trg)){
				gpm_edge he = new gpm_edge(src,trg,1,1);
				sccDAG.InsertEdge(he);
			}
		}

		//int i=0;
		//computing ranks
		//		for(int i=0;i<sccsets.size();i++){
		//			Set<gpm_node> s = sccsets.get(i);
		//			//sccid++;
		//
		//			for(int j=0;j<i && j!=i;j++){
		//				Set<gpm_node> s2 = sccsets.get(j);
		//				for(gpm_node na: s){
		//					for(gpm_node nb: s2){
		//						if(this.getEdge(na, nb)!=null){
		//							gpm_edge e = new gpm_edge(sccnodes.elementAt(i).tag,sccnodes.elementAt(j).tag,1,0);
		//							sccedges.add(e);
		//							break;
		//						}
		//						else if(this.getEdge(nb, na)!=null){
		//							gpm_edge e = new gpm_edge(sccnodes.elementAt(j).tag,sccnodes.elementAt(i).tag,1,0);
		//							sccedges.add(e);
		//							break;
		//						}
		//					}
		//					break;
		//				}
		//			}
		//		}




		//System.out.println("Original graph nodes: " + this.vertexSet().size() + " edges: "+this.edgeSet().size());

		//System.out.println("SCC nodes and edges done.");
		//System.out.println("nodes: " + sccDAG.vertexSet().size() + " edges: " + sccDAG.edgeSet().size());

		//		String sccname = gfilename.substring(0, gfilename.indexOf("."));
		//		sccname = sccname + "_sccDAG.grp";
		//		sccDAG.ConstructGraphFromVec(0, sccname, this.schema, sccnodes, sccedges, this.relschema);


		//propagate nonWF area
		//int NWFsize = 0;
		HashSet<gpm_node> newnf = sccDAG.GetParentsSet(NWF, -1);
		HashSet<gpm_node> tmp = new HashSet<gpm_node>();
		tmp.addAll(newnf);

		while(newnf.size()> 0){
			NWF.addAll(newnf);
			newnf.clear();
			newnf.addAll(sccDAG.GetParentsSet(tmp, -1));
			newnf.removeAll(NWF);
			tmp.clear();
			tmp.addAll(newnf);
		}

		WF.addAll(sccDAG.vertexSet());
		WF.removeAll(NWF);


		//the following is for whether a ranking based on sccgraph is required
		List<gpm_node> nlist = new ArrayList<gpm_node>();
		//nlist.addAll(this.vertexSet());

		//			for(gpm_node n: this.vertexSet()){
		//				if(this.outDegreeOf(n)==0){
		//					n.rank = 0;
		//					//nlist.remove(n);
		//				}
		//				else if(sccDAG.outDegreeOf(sccnodes.elementAt(Integer.parseInt(n.addinfo)))==0){
		//					n.rank = Integer.MIN_VALUE;
		//					//nlist.remove(n);
		//				}
		//			}

		TopologicalOrderIterator topiter = 
			new TopologicalOrderIterator(sccDAG);

		while(topiter.hasNext()){
			gpm_node n = (gpm_node) topiter.next();
			//nlist.addAll(this.expSCC(n));
			nlist.add(n);
		}

		//bottom up
		Collections.reverse(nlist);

		HashSet<gpm_node> col = new HashSet<gpm_node>();

		Vector<gpm_node> Cset = new Vector<gpm_node>();

		HashSet<gpm_node> tmprset = new HashSet<gpm_node>(); 

		for(gpm_node n: nlist){
			col.clear();
			col.addAll(this.expSCCwithaddinfo(n));
			if(sccDAG.outDegreeOf(n)==0 && n.weight==1){
				n.rank = 0;
				for(gpm_node v:col) v.rank = 0;
			}
			else if(sccDAG.outDegreeOf(n)==0 && n.weight>1){
				n.rank = Integer.MIN_VALUE;
				for(gpm_node v:col) v.rank = Integer.MIN_VALUE;
			}
			else{
				Cset.clear();
				Cset = sccDAG.GetChildren(n, -1);
				n.rank = Integer.MIN_VALUE;
				for(gpm_node cn: Cset){
					if(WF.contains(cn) && (cn.rank+1)> n.rank)
						n.rank = cn.rank+1;
					else if(NWF.contains(cn) && cn.rank>n.rank && cn.rank>=0)
						n.rank = cn.rank;
					else if(NWF.contains(cn) && cn.rank>=n.rank && cn.rank<0)
						n.rank = Integer.MIN_VALUE;
				}
				for(gpm_node v:col) v.rank = n.rank;
			}

			tmprset.clear();
			if(rankmap.get(n.rank)!=null){
				tmprset.addAll(rankmap.get(n.rank));
				tmprset.addAll(col);
				rankmap.put(n.rank, new HashSet<gpm_node>(tmprset));
			}
			else rankmap.put(n.rank, new HashSet<gpm_node>(col));
		}


		//		for(Integer I: rankmap.keySet()){
		//			if(I==Integer.MIN_VALUE){
		//				C.add(rankmap.get(I));
		//			}
		//			else{
		//				if(C.elementAt(I)==null)
		//				C.setElementAt(rankmap.get(I), I);
		//			}
		//		}
		return rankmap;//C;
	}



	@SuppressWarnings("unchecked")
	public gpm_graph sccDAG(){
		DirectedGraph dg = this.Cast2DG(); 
		gpm_graph sccDAG = new gpm_graph();
		if(sccIns==null)
			sccIns = new StrongConnectivityInspector(dg);
		List<Set<gpm_node>> sccsets = sccIns.stronglyConnectedSets();


		HashSet<gpm_node> WF = new HashSet<gpm_node>();
		HashSet<gpm_node> NWF = new HashSet<gpm_node>();


		Vector<gpm_node> sccnodes = new Vector<gpm_node>();
		HashSet<gpm_edge> sccedges = new HashSet<gpm_edge>();
		//int sccid = 0;

		for(int i=0;i<sccsets.size();i++){

			Set<gpm_node> s = sccsets.get(i);

			for(gpm_node n: s){
				n.addinfo = ""+i;
			}

			gpm_node scc = new gpm_node();
			//System.out.println("SCC:" + (i));
			//scc.tag = ""+sccid;
			for(gpm_node n: s){
				scc.addinfo = scc.addinfo + n.tag + "_";
			}
			scc.tag = scc.addinfo.substring(0,scc.addinfo.lastIndexOf("_"));
			sccnodes.add(scc);

			//sccedges.add(new gpm_edge(sccnodes.elementAt(i).tag, sccnodes.elementAt(i).tag,1,0));

			if(sccsets.get(i).size()>1)
				NWF.addAll(sccsets.get(i));
		}

		//propagate nonWF area
		//int NWFsize = 0;
		HashSet<gpm_node> newnf = this.GetParentsSet(NWF, -1);
		HashSet<gpm_node> tmp = new HashSet<gpm_node>();
		tmp.addAll(newnf);

		while(newnf.size()> 0){
			NWF.addAll(newnf);
			newnf.clear();
			newnf.addAll(this.GetParentsSet(tmp, -1));
			newnf.removeAll(NWF);
			tmp.clear();
			tmp.addAll(newnf);
		}

		WF.addAll(this.vertexSet());
		WF.removeAll(NWF);

		//int i=0;
		//computing ranks
		for(int i=0;i<sccsets.size();i++){
			Set<gpm_node> s = sccsets.get(i);
			//sccid++;

			for(int j=0;j<i && j!=i;j++){
				Set<gpm_node> s2 = sccsets.get(j);
				for(gpm_node na: s){
					for(gpm_node nb: s2){
						if(this.getEdge(na, nb)!=null){
							gpm_edge e = new gpm_edge(sccnodes.elementAt(i).tag,sccnodes.elementAt(j).tag,1,0);
							sccedges.add(e);
							break;
						}
						else if(this.getEdge(nb, na)!=null){
							gpm_edge e = new gpm_edge(sccnodes.elementAt(j).tag,sccnodes.elementAt(i).tag,1,0);
							sccedges.add(e);
							break;
						}
					}
					break;
				}
			}
		}


		//System.out.println("Original graph nodes:" + this.vertexSet().size() + "edges: "+this.edgeSet().size());

		//System.out.println("SCC nodes and edges done.");
		//System.out.println("nodes:" + sccnodes.size() + "edges:" + sccedges.size());

		String sccname = gfilename.substring(0, gfilename.indexOf("."));
		sccname = sccname + "_sccDAG.grp";
		sccDAG.ConstructGraphFromVec(0, sccname, this.schema, sccnodes, sccedges, this.relschema);

		return sccDAG;
	}

	//	============================================================================//
	//	This function expands an scc node into a node set
	//	============================================================================//
	public HashSet<gpm_node> expSCC(gpm_node sccnode){
		String s = sccnode.tag;
		Vector<String> nids = createVectorFromString(s,"_");
		HashSet<gpm_node> nset = new HashSet<gpm_node>();
		for(String id: nids){
			nset.add(this.GetVertex(id));
		}
		return nset;
	}


	//	============================================================================//
	//	This function expands an scc node into a node set
	//	============================================================================//
	public HashSet<gpm_node> expSCCwithaddinfo(gpm_node sccnode){
		String s = sccnode.addinfo;
		Vector<String> nids = createVectorFromString(s,"_");
		HashSet<gpm_node> nset = new HashSet<gpm_node>();
		for(String id: nids){
			nset.add(this.GetVertex(id));
		}
		return nset;
	}


	//	============================================================================//
	//	This function returns the forward k-hop of a set of nodes
	//	============================================================================//
	public HashSet<gpm_node> forwardKhop(Vector<gpm_node> center, int k, int color){
		HashSet<gpm_node> kset = new HashSet<gpm_node>();
		HashSet<gpm_node> neib = new HashSet<gpm_node>();
		kset.addAll(center);
		for(int i=0;i<k;i++){
			neib = GetChildrenSet(kset, color);
			if(neib.equals(kset))
				return kset;
			else kset.addAll(neib);
		}
		return kset;
	}

	//	============================================================================//
	//	This function returns the backward k-hop of a set of nodes
	//	============================================================================//
	public HashSet<gpm_node> backwardKhop(Vector<gpm_node> center, int k, int color){
		HashSet<gpm_node> kset = new HashSet<gpm_node>();
		HashSet<gpm_node> neib = new HashSet<gpm_node>();
		kset.addAll(center);
		for(int i=0;i<k;i++){
			neib = GetParentsSet(kset, color);
			if(neib.equals(kset))
				return kset;
			else kset.addAll(neib);
		}
		return kset;
	}

	//	============================================================================//
	//	This function checks the distance of two vertex.
	//	mode 1: from distance index;
	// 	mode 2: from matrix;
	//	============================================================================//
	public boolean checkDistance(boolean opt, gpm_node a, gpm_node b, int bound, int mode, int color, boolean inc){
		if(a.tag.equals(b.tag)){
			if(this.getEdge(a, b)==null)
				return false;
			else
				return true;
		}
		if(mode==1){

			if(this.dBuff!=null)
				this.dBuff = null;
			if(this.FS==null)
				this.compAPSP();
			long start = System.nanoTime();
			short dist = //FS.shortestDistance(a, b, color);
				FS.d[color][FS.revnodeidmap.get(a)][FS.revnodeidmap.get(b)];
			distQtimeDM += System.nanoTime() - start;

			if(dist<=bound)
				return true;
			return false;
			//return checkDistanceFromMatrix(a,b,bound,color);
		}
		else{
			long start = System.currentTimeMillis();
			if(this.FS!=null)
				this.FS = null;
			boolean isr =  checkDistanceFromDS(mode, opt, a,b,bound,color,inc);
			//distQtime +=System.currentTimeMillis() - start;
			return isr;
		}
	}

	//	============================================================================//
	//	This function gets the distance of two vertex.
	//	mode 1: from distance index;
	// 	mode 2: from matrix;
	//	============================================================================//
	public short getDist(gpm_node a, gpm_node b, int mode, int color){

		if(a.tag.equals(b.tag)){
			return 0;
		}

		if(this.FS==null)
			this.compAPSP();
		if(mode==1){
			return FS.shortestDistance(a, b, color);
		}
		else{
			if(dBuff.getDist(a.tag, b.tag, color)==-1){
				//System.out.println("recompute");
				boolean reachable = checkDistanceFromDS(mode, false, a, b, this.vertexSet().size(), color, false);
				if(!reachable)
					return Short.MAX_VALUE;
				else
					return ((Integer)dBuff.getDist(a.tag, b.tag, color)).shortValue();
			}
			else
				return ((Integer)dBuff.getDist(a.tag, b.tag, color)).shortValue();
		}
	}

	//	============================================================================//
	//	This function checks the distance of two vertex from Matrix;
	//	This function has multiple interfaces.
	//	============================================================================//
	public boolean checkDistanceFromMatrix(gpm_node a, gpm_node b, int bound, int color){
		this.FS = null;
		this.compAPSP();
		long start = System.nanoTime();
		//distQtimeDM +
		short dist = //FS.shortestDistance(a, b, color);
			FS.d[color][FS.revnodeidmap.get(a)][FS.revnodeidmap.get(b)]; 
		distQtimeDM += System.nanoTime() - start;
		if(dist<=bound)
			return true;
		return false;
	}

	//	============================================================================//
	//	This function computes all-pair shortest paths of a graph
	//	============================================================================//
	public void compAPSP(){
		FS = new gpm_distFWMatrix(this, true);
	}

	//	============================================================================//
	//	This function checks the distance of two vertex with ad-hoc BFS search
	//	utilizing bounded DijkstraShortestPath
	// 	when start and end vertices are specified -- becomes BFS search.
	//	============================================================================// // avpair color bound
	public boolean AnseringRQ(gpm_node a, gpm_node b, int[][] bcmap){

		//		long bibfs = 0;
		//		long bfs = 0;


		if(this.dBuff==null){
			this.dBuff = new gpm_distBuffer(gfilename);
		}

		if(a.tag.equals(b.tag)){
			if(this.getEdge(a, b)!=null){
				return true;
			}
			else
				return false;
		}

		int[] pos = new int[bcmap.length];
		int sum = bcmap[0][1];
		pos[0] = 0;
		for(int i=1;i<bcmap.length;i++){
			pos[i] = pos[i-1] + bcmap[i-1][1]; 
			sum += bcmap[i][1];
		}

		int posi = 0;
		int[] bcvec = new int[sum];
		for(int i=0;i<bcmap.length;i++){
			for(int j=0;j<bcmap[i][1];j++){
				bcvec[posi] = bcmap[i][0];
				posi++;
			}
		}


		//boolean isr = false;

		boolean Change1 = true;
		boolean Change2 = true;
		HashSet<gpm_node> neighbors1 = new HashSet<gpm_node>();
		HashSet<gpm_node> neighbors2 = new HashSet<gpm_node>();
		HashSet<gpm_node> visited1 = new HashSet<gpm_node>();
		HashSet<gpm_node> visited2 = new HashSet<gpm_node>();
		HashSet<gpm_node> newnodes1 = new HashSet<gpm_node>();
		HashSet<gpm_node> newnodes2 = new HashSet<gpm_node>();
		neighbors1.add(a);
		neighbors2.add(b);
		visited1.add(a);
		visited2.add(b);
		newnodes1.add(a);
		newnodes2.add(b);

		long start = System.currentTimeMillis();

		int step1 = 0;
		int step2 = bcvec.length-1;
		int currentcolor1 = 0;
		int currentcolor2 = 0;
		//int currentcoloridx1 = 0;
		//int currentcoloridx2 = ;
		int pos1 = 0;
		int pos2 = bcmap.length-1;

		boolean res = true;
		//		boolean s1change = true;
		//		boolean s2change = true;

		//while aset and bset can both change, continue
		while(true){

			//if s1 smaller than s2 extend s1, and s1 can still be extended for a color:
			//extend s1 within the same color. Always only consider those new nodes.
			if((visited1.size()<=visited2.size() && Change1)|| (!Change2)){

				//System.out.println("expand set 1.");
				//decide the color to extend
				int orgcolor = currentcolor1;
				currentcolor1 = bcvec[step1];
				//if color changes, then all visited nodes need to be considered.
				if(orgcolor!=currentcolor1)
					newnodes1.addAll(visited1);
				neighbors1.clear();
				step1++;
				//System.out.println("step1: "+step1);
				//s1change = true;
				for(gpm_node as: newnodes1){
					neighbors1.addAll(this.GetChildren(as, currentcolor1));
				}

				neighbors1.removeAll(visited1);
				if(neighbors1.size()==0 && pos1<pos.length){
					//s1change = false;
					//reset step and currentcoloridx
					step1 = pos[pos1]; pos1++; //currentcoloridx1++;
					newnodes1.clear();
					newnodes1.addAll(visited1);
					//					if(pos1==pos.length){
					//						Change1=false;
					//					}
					continue;
				}
				else if((neighbors1.size()==0 && pos1==pos.length) || step1== bcvec.length){
					Change1 = false;
				}
				else if(neighbors1.size()!=0){
					visited1.addAll(neighbors1);
					newnodes1.clear();
					newnodes1.addAll(neighbors1);
				}
			}

			//else if s1 larger than s2 extend s2
			else if((visited2.size()<visited1.size() && Change1 && Change2)){

				//System.out.println("expand set 2.");
				//decide the color to extend
				int orgcolor2 = currentcolor2;
				currentcolor2 = bcvec[step2]; 
				if(orgcolor2!=currentcolor2)
					newnodes2.addAll(visited2);
				neighbors2.clear();
				step2--;
				//System.out.println("step2: " +step2);
				//s2change = true;
				for(gpm_node bs: newnodes2){
					neighbors2.addAll(this.GetParents(bs, false, currentcolor2));
				}
				neighbors2.removeAll(visited2);
				if(neighbors2.size()==0 && pos2>0){
					//s1change = false;
					//reset step and currentcoloridx
					step2 = pos[pos2]; pos2--; //currentcoloridx1++;
					newnodes2.clear();
					newnodes2.addAll(visited2);
					//					if(pos1==pos.length){
					//						Change1=false;
					//					}
					continue;
				}
				else if((neighbors2.size()==0 && pos2==0) || step2 == 0){
					Change2 = false;
				}
				else if(neighbors2.size()!=0){
					visited2.addAll(neighbors2);
					newnodes2.clear();
					newnodes2.addAll(neighbors2);
				}
			}

			distQtime3 +=System.currentTimeMillis() - start;

			if((step1>step2 || (!Change1))){
				//System.out.println("test.");
				HashSet<gpm_node> S3 = new HashSet<gpm_node>();
				S3.addAll(visited1);
				S3.retainAll(visited2);
				if(S3.size()>0){
					res =  true;
					break;
				}
				else{
					res = false;
					break;
				}
			}
		}

		distQtime3 += System.currentTimeMillis() - start;
		return res;
	}











	//	============================================================================//
	//	This function checks the distance of two vertex with ad-hoc BFS search
	//	utilizing bounded DijkstraShortestPath
	// 	when start and end vertices are specified -- becomes BFS search.
	//	============================================================================//
	public boolean checkDistanceFromDS(int mode, boolean opt, gpm_node a, gpm_node b, int bound, int color, boolean inc){

		long bibfs = 0;
		long bfs = 0;

		if(a.tag.equals(b.tag)){
			if(this.getEdge(a, b)!=null){
				return true;
			}
			else
				return false;
		}

		//distance stored in dist buffer.
		if(this.dBuff!=null){
			int dist = this.dBuff.getDist(a.tag, b.tag, color);
			int status = 1;
			if(inc)
				status = this.dBuff.getstatus(a.tag, b.tag, color);
			if(dist!=-1 && status==1){
				if(dist<=bound)
					return true;
				return false;
			}
		}

		if(this.dBuff==null){
			this.dBuff = new gpm_distBuffer(gfilename);
		}



		//if unbounded, using regular BFS search.
		//		if(bound>=Short.MAX_VALUE){
		//			DijkstraShortestPath DS = new DijkstraShortestPath(this,a,b,bound);
		//			dBuff.insertDist(0, a.tag, b.tag, new Double(DS.getPathLength()).shortValue());
		//			if(DS.getPathLength()<=bound)
		//				return true; //(short)DS.getPathLength();
		//			return false; //Short.MAX_VALUE;
		//		}
		//if bounded;
		//		else{
		//short dist = Short.MAX_VALUE;
		//boolean flag = false;

		//		if(bibfs){

		long start1 = System.nanoTime();

		HashSet<gpm_node> neighbors1 = new HashSet<gpm_node>();
		HashSet<gpm_node> neighbors2 = new HashSet<gpm_node>();
		HashSet<gpm_node> S3 = new HashSet<gpm_node>();

		Vector<gpm_node> visited1 = new Vector<gpm_node>();
		Vector<gpm_node> visited2 = new Vector<gpm_node>();
		Vector<gpm_node> newnodes1 = new Vector<gpm_node>();
		Vector<gpm_node> newnodes2 = new Vector<gpm_node>();
		neighbors1.add(a);
		neighbors2.add(b);
		visited1.add(a);
		visited2.add(b);
		newnodes1.add(a);
		newnodes2.add(b);

		short distn1 = 0;
		short distn2 = 0;
		boolean s1change = true;
		boolean s2change = true;
		boolean isr = false;
		//		HashSet<gpm_node> cset = new HashSet<gpm_node>();

		short i = 0;
		for(i=0;i<Math.min(bound, this.vertexSet().size());i++){

			//if s1 smaller than s2 extend s1
			if(visited1.size()<=visited2.size() && s1change ){
				s1change = true;
				neighbors1.clear();
				for(gpm_node as: newnodes1){
					neighbors1.addAll(this.GetChildren(as, color));
				}
				neighbors1.removeAll(visited1);
				if(neighbors1.size()==0){
					s1change = false;
				}
				distn1++;
				if(opt){
					for(gpm_node as:neighbors1){	
						dBuff.insertDist(0, a.tag, as.tag, distn1, color, inc);
					}
				}
				visited1.addAll(neighbors1);
				newnodes1.clear();
				newnodes1.addAll(neighbors1);
			}

			//if s1 larger than s2 extend s2.
			else if(visited2.size()<visited1.size() && s1change && s2change){
				s2change = true;
				neighbors2.clear();
				for(gpm_node bs: newnodes2){
					neighbors2.addAll(this.GetParents(bs, false, color));
				}
				neighbors2.removeAll(visited2);
				if(neighbors2.size()==0){
					s2change = false;
				}
				distn2++;
				if(opt){
					for(gpm_node bs:neighbors2){
						dBuff.insertDist(0, bs.tag, b.tag, distn2, color, inc);
					}
				}
				visited2.addAll(neighbors2);
				newnodes2.clear();
				newnodes2.addAll(neighbors2);
			}

			S3.clear();
			S3.addAll(visited1);
			S3.retainAll(visited2);
			if(S3.size()>0){
				dBuff.insertDist(0, a.tag, b.tag, (short)(i+1), color, inc);
				isr =  true;
				break;
			}
			else if((!s1change)|| (s1change && !s2change) ){ //&& !s2change //
				isr =  false;
				break;
			}
			//System.out.println(i);
		}

		bibfs = System.nanoTime() - start1;
		distQtime += bibfs;

		start1 = System.nanoTime();

		//		DijkstraShortestPath<gpm_node, gpm_edge> dpath = new DijkstraShortestPath<gpm_node, gpm_edge>(this,a,b);
		//		dpath.getPathLength();


		//
		//		neighbors1.clear();
		//		neighbors2.clear();
		//		S3.clear();
		//
		//		visited1 = new Vector<gpm_node>();
		//		visited2 = new Vector<gpm_node>();
		//		newnodes1 = new Vector<gpm_node>();
		//		newnodes2 = new Vector<gpm_node>();
		//		neighbors1.add(a);
		//		neighbors2.add(b);
		//		visited1.add(a);
		//		visited2.add(b);
		//		newnodes1.add(a);
		//		newnodes2.add(b);
		//
		//		distn1 = 0;
		//		distn2 = 0;
		//		s1change = true;
		//		s2change = false;
		//		isr = false;
		//		//		HashSet<gpm_node> cset = new HashSet<gpm_node>();
		//
		//		i = 0;
		//		for(i=0;i<Math.min(bound, this.vertexSet().size());i++){
		//
		//			//if s1 smaller than s2 extend s1
		//			if( s1change ){//visited1.size()<=visited2.size() &&
		//				s1change = true;
		//				neighbors1.clear();
		//				for(gpm_node as: newnodes1){
		//					neighbors1.addAll(this.GetChildren(as, color));
		//				}
		//				neighbors1.removeAll(visited1);
		//				if(neighbors1.size()==0){
		//					s1change = false;
		//				}
		//				distn1++;
		//				if(opt){
		//					for(gpm_node as:neighbors1){	
		//						//dBuff.insertDist(0, a.tag, as.tag, distn1, color, inc);
		//					}
		//				}
		//				visited1.addAll(neighbors1);
		//				newnodes1.clear();
		//				newnodes1.addAll(neighbors1);
		//			}
		//
		//			//if s1 larger than s2 extend s2.
		//			else if(visited2.size()<visited1.size() && s2change){
		//				s2change = true;
		//				neighbors2.clear();
		//				for(gpm_node bs: newnodes2){
		//					neighbors2.addAll(this.GetParents(bs, false, color));
		//				}
		//				neighbors2.removeAll(visited2);
		//				if(neighbors2.size()==0){
		//					s2change = false;
		//				}
		//				distn2++;
		//				if(opt){
		//					for(gpm_node bs:neighbors2){
		//						//dBuff.insertDist(0, bs.tag, b.tag, distn2, color, inc);
		//					}
		//				}
		//				visited2.addAll(neighbors2);
		//				newnodes2.clear();
		//				newnodes2.addAll(neighbors2);
		//			}
		//
		//			S3.clear();
		//			S3.addAll(visited1);
		//			S3.retainAll(visited2);
		//			if(S3.size()>0){
		//				//dBuff.insertDist(0, a.tag, b.tag, (short)(i+1), color, inc);
		//				isr =  true;
		//				break;
		//			}
		//			else if((!s1change)){
		//				isr =  false;
		//				break;
		//			}
		//			//System.out.println(i);
		//		}
		//
		//		bfs = System.nanoTime() - start1;
		//		distQtime2 += bfs;

		//System.out.println((bibfs<=bfs? "bibfs faster ":"bfs faster ")+ "bibfs "+ (double)(bibfs/1000000) + " bfs "+ (double)(bfs/1000000));
		//System.out.println((bibfs<=bfs? "bibfs faster ":"bfs faster ")+ "bibfs "+ bibfs + " bfs "+ bfs);
		//System.out.println((distQtime<=distQtime2? "bibfs total faster ":"bfs total faster ")+"bibfs total: "+(double)(distQtime/1000000) + "bfs total: "+(double)(distQtime2/1000000));

		if(i>=this.vertexSet().size()-1){
			dBuff.insertDist(0, a.tag, b.tag, Short.MAX_VALUE, color, inc);
		}


		//using BFS search
		return isr;
	}
	//	}


	//	============================================================================//
	//	This function creates a vector from a string
	//	============================================================================//
	public Vector<String> createVectorFromString(String strContent,
			String strDelimiter) {
		Vector<String> vec = new Vector<String>();
		String[] words = strContent.split(strDelimiter);

		for (int i = 0; i < words.length; i++) {
			vec.addElement(words[i]);
		}
		return vec;
	}

	//	============================================================================//
	//	This function constructs a graph from a list of nodes and edges 
	//	============================================================================//
	public void ConstructGraphFromVec(int type, String gfname, HashMap<String, String> schma, Vector<gpm_node> vlist,HashSet<gpm_edge> elist, Vector<String> ecolor){
		this.gtype = type;
		this.gfilename = gfname;
		this.schema = schma;
		if(this.relschema==null)
			this.relschema = new Vector<String>();
		this.relschema = ecolor;
		int countnode = 0;
		int countedge = 0;
		int nsize = vlist.size();
		int esize = elist.size();

		for(gpm_node n: vlist){
			this.InsertNode(n);
		}
		for(gpm_edge e:elist){
			this.InsertEdge(e);
		}
	}

//	============================================================================//
	//	This function constructs a graph from neo4j 
	//	============================================================================//
	public void ConstructGraphFromNeo4j(String graphDbPath, int idxmode){
		GraphDatabaseService graphDb;
		this.gfilename = graphDbPath;
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( graphDbPath );
		String ntag = "";
		double nweight = 0.0;
		String nlabel = "";
		Vector<AVpair> alist = new Vector<AVpair>();
		int prednum = 0;
		this.simMatchSet = new TreeMap<String, Vector<String>>();
		Vector<String> nodeMatch = new Vector<String>();
		try ( Transaction tx = graphDb.beginTx() )
		{
			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphDb);
			
			Iterable<String> propertyKeys  = globalOperation.getAllPropertyKeys();
			if(this.schema==null)
				this.schema = new HashMap<String, String>();
			for(String kitem: propertyKeys){
				String attname = kitem;
				String type = "String";
				this.schema.put(attname, type);
			}
			if(this.relschema==null)
				this.relschema = new Vector<String>();
			this.relschema.add("0");
			
			Iterable<Node> neoNodes  = globalOperation.getAllNodes();
			for ( Node item : neoNodes )
			{
				alist.clear();
				ntag = Long.toString(item.getId());
				nweight = 0.0;
				Iterable<String> nodePropertyKeys = item.getPropertyKeys();
				for (String kitem : nodePropertyKeys)
				{
					String att="", op="",val="";
					att = kitem;
					op = "=";
					val = item.getProperty(kitem).toString();
					alist.add(new AVpair(att,op,val));
				}
				for (Label klabel : item.getLabels()){
					nlabel = klabel.name();
				}
				gpm_node v = new gpm_node(alist,ntag,nweight,nlabel);
				this.addVertex(v);
				this.simMatchSet.put(v.tag, nodeMatch);
			}
			
			Iterable<Relationship> neoRelationships = globalOperation.getAllRelationships();
			for (Relationship item : neoRelationships)
			{
				gpm_edge e = new gpm_edge(Long.toString(item.getStartNode().getId()),Long.toString(item.getEndNode().getId()),1,0);
				this.InsertEdge(e);
			}
			tx.success();
		}
	}
	
	//	============================================================================//
	//	This function builds up the reversed index for attributes on nodes
	//	============================================================================//
	public void constructRevIndex(Vector<gpm_node> vlist,HashSet<gpm_edge> elist){
		for(gpm_node n: vlist){
			this.InsertNode(n);
		}
		for(gpm_edge e:elist){
			this.InsertEdge(e);
		}
	}

	//	============================================================================//
	//	This function constructs a graph from a given pattern graph
	//  This function can be used in two ways: 
	//  1. to duplicate redundant queries. for this, use exactcopyPN.
	//  2. to generate synthetic data graphs. for this, use copyPN.
	//	============================================================================//
	public void ConstructGraph(gpm_graph G){
		for(gpm_node n: G.vertexSet()){
			gpm_node ncopy = new gpm_node();
			ncopy.copyPN(n); //ncopy.exactcopyPN(n);
			this.InsertNode(ncopy);
		}
		for(gpm_edge e: G.edgeSet()){
			gpm_edge ei = new gpm_edge(e.from_node,e.to_node,e.e_bound,e.e_color);
			this.InsertEdge(ei);
		}
		this.gfilename = G.gfilename; //will be modified later.
		this.schema = G.schema;
		this.relschema = G.relschema;
		this.dBuff = G.dBuff;
		this.FS = G.FS;
		this.simMatchSet = new TreeMap<String, Vector<String>>();
		for(String s : G.simMatchSet.keySet()){
			this.simMatchSet.put(s, G.simMatchSet.get(s));
		}
	}

	//	============================================================================//
	//	This function combines a graph into this graph. graph union.
	//	============================================================================//
	//	public void UnionGraph(gpm_graph G){
	//
	//	}

	//	============================================================================//
	//	This function constructs a graph from a special format graph file 
	//	============================================================================//
	public void ConstructGraphFromFile(String gFile, int idxmode){

		try{

			this.gfilename = gFile;

			System.out.println("Entering this Now!");

			BufferedReader in = MyFileOperation.openFile(gFile);
			String strLine="";
			Vector<String> vec=null;

			//line 0: node schemas
			strLine = in.readLine().trim();
			vec = createVectorFromString(strLine,"	");
			if(this.schema==null)
				this.schema = new HashMap<String, String>();
			for(String avpair: vec){
				String attname = avpair.substring(0, avpair.indexOf(":"));
				String type = avpair.substring(avpair.indexOf(":")+1);
				this.schema.put(attname, type);
			}

			//line 1: added: edge schema
			strLine = in.readLine().trim();
			vec = createVectorFromString(strLine,"	");
			if(this.relschema==null)
				this.relschema = new Vector<String>();
			this.relschema.clear();
			this.relschema.addAll(vec);

			//line 2: graph type: pattern or datagraph
			strLine=in.readLine().trim();
			this.gtype=Integer.parseInt(strLine);

			//line 3: node number
			int nodeNumber = 0;
			strLine=in.readLine();
			nodeNumber = Integer.parseInt(strLine);

			//line 4: node info
			//nid nweight prenumber predicates 

			String ntag = "";
			double nweight = 0.0;
			Vector<AVpair> alist = new Vector<AVpair>();
			int prednum = 0;


			//int ncount = 0;

			//construct node list
			for(int i=0;i<nodeNumber;i++){
				alist.clear();
				strLine=in.readLine().trim();
				vec = createVectorFromString(strLine,"	");
				ntag = vec.elementAt(0);
				nweight = Double.parseDouble(vec.elementAt(1));
				prednum = Integer.parseInt(vec.elementAt(2));

				//attri-val list
				//all op is "=" for now.
				for(int j=0;j<prednum;j++){
					String att="", op="",val="";
					if(vec.elementAt(3+j).contains("="))
						op = "=";
					if(vec.elementAt(3+j).contains("<"))
						op = "<";
					if(vec.elementAt(3+j).contains(">"))
						op = ">";
					att = vec.elementAt(3+j).substring(0,vec.elementAt(3+j).indexOf(op));
					//String type = att.substring(att.indexOf(":")+1);
					val = vec.elementAt(3+j).substring(vec.elementAt(3+j).indexOf(op)+1);
					String type = this.schema.get(att);
					if(type==null)
						System.out.println(att);
					if(type.equals("String"))
						alist.add(new AVpair(att,op,(String)val));
					if(type.equals("Integer"))
						alist.add(new AVpair(att,op,Integer.parseInt(val)));
					if(type.equals("Double"))
						alist.add(new AVpair(att,op,Double.parseDouble(val)));
				}

				gpm_node v = new gpm_node(alist,ntag,nweight,"");
				this.addVertex(v);

				if(i!=0 && (nodeNumber/100)!=0 && i%(nodeNumber/100)==0){
					System.out.println((i/(nodeNumber/100))+"%");
				}
			}


			System.out.println("Nlist loaded.");

			//construct edge list
			//fromid, toid, weight
			strLine=in.readLine().trim();
			vec = null;
			int edgeNumber = Integer.parseInt(strLine);
			for(int j=0; j<edgeNumber; j++){
				strLine=in.readLine().trim();
				vec = createVectorFromString(strLine,"	");
				//System.out.println(vec);
				gpm_edge e = new gpm_edge(vec.elementAt(0),vec.elementAt(1),Integer.parseInt(vec.elementAt(2)), Integer.parseInt(vec.elementAt(3)));
				this.InsertEdge(e);

				if(j!=0 && (edgeNumber/100)!=0 && j%(edgeNumber/100)==0){
					System.out.println((j/(edgeNumber/100))+"%");
				}
			}
			in.close();

			System.out.println("Elist loaded.");

			if(this.gtype==0)
				return;

			//restore matrix if any
			//			if(idxmode==1){
			//				this.FS = new gpm_distFWMatrix(this, false);
			//				this.FS.restoreMatrix();
			//			}
			//
			//			//restore dbuff if any
			//			if(idxmode==2){
			//				this.dBuff = new gpm_distBuffer(gfilename);
			//				this.dBuff.restoreBuff();
			//			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}	


	//	============================================================================//
	//	This function stores a graph to a special format .grp file
	//	============================================================================//
	public void Storegraph() throws IOException{

		System.out.println(gfilename);

		//store graph structure
		FileWriter fw = new FileWriter(gfilename);
		PrintWriter pw = new PrintWriter(fw);

		//node schema
		int i = 0;
		for(String attname: this.schema.keySet()){
			String atp = attname+":"+this.schema.get(attname);
			if(i!=0) atp = "	"+atp;
			pw.print(atp);
			i++;
		}
		pw.println();

		//edge schema
		i = 0;
		for(String rname: this.relschema){
			if(i!=0) rname = "	"+rname;
			pw.print(rname);
			i++;
		}
		pw.println();

		//graph type
		pw.println(this.gtype);

		//node set info
		pw.println(this.vertexSet().size());
		for(gpm_node n: this.vertexSet()){
			pw.print(n.tag+"	"+n.weight+"	");
			pw.print(n.attrmap.keySet().size());
			for(String s:n.attrmap.keySet()){
				pw.print("	"+s+(n.attrmap.get(s)).elementAt(0)+(n.attrmap.get(s)).elementAt(1));
			}
			pw.println();
		}

		//edge set info
		//HashSet<gpm_edge> eset = (HashSet);
		pw.println(this.edgeSet().size());

		for(Object e:this.edgeSet()){
			pw.println(((gpm_edge)e).from_node+"	"+((gpm_edge)e).to_node+"	"+((gpm_edge)e).e_bound+"	"+((gpm_edge)e).e_color);
		}

		pw.close();
		fw.close();

		//store matrix if any
		if(this.FS!=null)
			this.FS.storeMatrix();

		//store distance buffer if any
		if(this.dBuff!=null)
			this.dBuff.storeBuff();
	}

	//	============================================================================//
	//	This function print info of a graph
	//	============================================================================//
	public void Display(){

		HashSet<gpm_node> nset = new HashSet<gpm_node>();
		for(gpm_node n:this.vertexSet())
			nset.add(n);
		System.out.println(nset.size());

		//node set info
		for(gpm_node n: nset){
			System.out.print(n.tag+" "+n.weight);
			for(String s:n.attrmap.keySet()){
				System.out.print(" "+s+(n.attrmap.get(s)).elementAt(0)+(n.attrmap.get(s)).elementAt(1));
			}
			System.out.println();
		}

		//edge set info
		HashSet<gpm_edge> eset = new HashSet<gpm_edge>();
		for(Object e:this.edgeSet())
			eset.add((gpm_edge)e);
		System.out.println(eset.size());

		for(gpm_edge e:eset){
			System.out.println(e.from_node+" "+e.to_node+" "+e.e_bound+"	"+e.e_color);
		}
	}

	//	public void Display(gpm_distMatrix M){
	//		if(M==null){
	//			System.out.println("M not computed yet.");
	//			return;
	//		}
	//		//		NumberFormat nf1 = NumberFormat.getInstance();
	//		//		M.print(nf1, M.getColumnDimension());
	//		for(int i=0; i<M.msize;i++){
	//			for(int j=0;j<M.msize;j++){
	//				System.out.print(M.Matrix[i][j]+" ");
	//			}
	//			System.out.println();
	//		}
	//	}
	
	public gpm_graph GraphConbination(gpm_graph b){
		gpm_graph combinedGraph = new gpm_graph();
		for (gpm_node na : this.vertexSet())
			for(gpm_node nb : b.vertexSet()){
				if(na.tag == nb.tag){
					if(combinedGraph.GetVertex(String.valueOf(na.tag)) == null)
					combinedGraph.addVertex(na);
				}
				else{
					if(combinedGraph.GetVertex(String.valueOf(na.tag)) == null)
						combinedGraph.addVertex(na);
					if(combinedGraph.GetVertex(String.valueOf(nb.tag)) == null)
						combinedGraph.addVertex(nb);
				}
			}
		
		for (gpm_edge ea : this.edgeSet())
			for(gpm_edge eb : b.edgeSet()){
				if(ea.from_node == eb.from_node && ea.to_node == eb.to_node){
					if(combinedGraph.GetEdge(ea.from_node, ea.to_node) == null)
						combinedGraph.InsertEdge(ea);
				}
				else{
					if(combinedGraph.GetEdge(ea.from_node, ea.to_node) == null)
						combinedGraph.InsertEdge(ea);
					if(combinedGraph.GetEdge(eb.from_node, eb.to_node) == null)
						combinedGraph.InsertEdge(eb);
				}
			}
		return combinedGraph;
	}

	public int DiameterCal(){
		int dia = 0;
		int diatemp = 0;
		for (gpm_node na : this.vertexSet())
			for(gpm_node nb : this.vertexSet())
			{
				if(Long.valueOf(nb.tag) > Long.valueOf(na.tag)){
					diatemp =(int)this.getDist(this.GetVertex(na.tag), this.GetVertex(nb.tag), 1, 0);
					if(diatemp != 32767){
						if(diatemp > dia)
							dia = diatemp;
					}
				}
			}
		return dia;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//test graphs.
		int dis = 0;

		gpm_graph gtest1 = new gpm_graph();
		//gpm_graph gtest2 = new gpm_graph();
		gtest1.ConstructGraphFromFile("/Users/qsong/Downloads/combine2.grh",1);
		//gtest2.ConstructGraphFromFile("/Users/qsong/Downloads/combine2.grh",1);
		long start = System.currentTimeMillis();
		System.out.println("diameter: " + gtest1.DiameterCal());
		System.out.println(System.currentTimeMillis() - start);
	}
}

