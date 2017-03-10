package wsu.eecs.mlkd.KGQuery.algo.joinsim;


import java.io.BufferedReader;
import java.io.File;
//import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.MyFileOperation;


//import java.util.*;

//import org.jgrapht.*;

public class gpm_distFWMatrix {

	//~ Instance fields --------------------------------------------------------

	public String fname;

	//public gpm_graph G;

	public int nextIndex = 0;
	//    HashMap<gpm_node, Integer> indices;

	public short [][][] d;

	public double diameter;

	HashMap<Integer, gpm_node> nodeidmap;
	HashMap<gpm_node, Integer> revnodeidmap;

	HashMap<Integer, String> relmap;

	//~ Constructors -----------------------------------------------------------

	/**
	 * Constructs the shortest path array for the given graph.
	 *
	 * @param g input graph
	 */

	public gpm_distFWMatrix(){	
	}

	//recom indicates whether d is computed during initiation phase.
	public gpm_distFWMatrix(gpm_graph g, boolean recom){

		//if(g.gtype==0) return;

		diameter = -1.0;
		String gn = g.gfilename;
		//fname = gn.substring(0, gn.lastIndexOf("."))+"_dM.txt";
		//System.out.println(fname);

		int sz = g.vertexSet().size();
		System.gc();
		//if(g.gtype==1)
			//System.out.println("data graph node size:"+ sz);
		d = new short[g.relschema.size()][sz][sz];
		System.out.println("Assigned.");
		d[0][0][0]=-1;
		//indices = new HashMap<gpm_node, Integer>();

		nodeidmap = new HashMap<Integer, gpm_node>();
		revnodeidmap = new HashMap<gpm_node, Integer>();
		for(gpm_node n: g.vertexSet()){
			nodeidmap.put(Integer.parseInt(n.tag), n);
			revnodeidmap.put(n, Integer.parseInt(n.tag));
		}

		relmap = new HashMap<Integer, String>();
		for(int i=0;i<g.relschema.size();i++){
			relmap.put(i, g.relschema.elementAt(i));
		}

		//Initialise distance to infinity, or the neighbours weight, or 0 if
		//same
		if(recom){
			System.out.println("computing matrix...");


			for(int i=0;i<g.relschema.size();i++){
				for(int j=0;j<sz;j++){
					for(int k=0;k<sz;k++){
						if(j==k)
							d[i][j][k]=0;
						else
							d[i][j][k]=Short.MAX_VALUE;
					}
				}

				System.out.println("propagating..");

				HashSet<gpm_node> neighbors = new HashSet<gpm_node>();
				HashSet<gpm_node> newnodes = new HashSet<gpm_node>();
				HashSet<gpm_node> visited = new HashSet<gpm_node>();
				boolean change = false;
				short step = 0;
				//the following is BFS dist.
				for(int j=0;j<sz;j++){
					step=0;
					visited.clear();
					neighbors.clear();
					newnodes.clear();
					//System.out.println("node "+j+" under process.");
					gpm_node n = nodeidmap.get(j);
					int idxn = Integer.parseInt(n.tag);
					d[i][idxn][idxn] = 0;
					neighbors.add(n);
					newnodes.add(n);
					visited.add(n);
					change = true;
					while(change){
						change = false;
						neighbors.clear();
						for(gpm_node vn: newnodes){
							//neighbors.addAll(g.GetChildren(vn, i));
							neighbors.addAll(g.GetChildren(vn, i));
						}
						neighbors.removeAll(visited);
						if(neighbors.size()==0)
							break;
						else {
							change = true;step++;
							for(gpm_node uvn: neighbors){
								d[i][idxn][revnodeidmap.get(uvn)]=step;
							}
							visited.addAll(neighbors);
							newnodes.clear();
							newnodes.addAll(neighbors);
						}
						//tmp = newnodes;
					}
				}

			}
		}
	}

	//~ Methods ----------------------------------------------------------------

	/**
	 * Retrieves the shortest distance between two vertices.
	 *
	 * @param v1 first vertex
	 * @param v2 second vertex
	 *
	 * @return distance, or positive infinity if no path
	 */
	public short shortestDistance(gpm_node v1, gpm_node v2, int color){
		return d[color][revnodeidmap.get(v1)][revnodeidmap.get(v2)];
	}

	public void setDistance(gpm_node v1, gpm_node v2, int color, short dist){
		d[color][revnodeidmap.get(v1)][revnodeidmap.get(v2)] = dist;
	}

	/**
	 * @return diameter computed for the graph
	 */
	public double getDiameter(){
		return diameter;
	}

	//	private int index(gpm_node vertex){
	//		return Integer.parseInt(vertex.tag);
	//	}

	public void display(){
		for(int i=0;i<d.length;i++){
			for(int j=0;j<d[i].length;j++){
				for(int k=0;k<d[i][j].length;k++){
					System.out.println("color: "+i+" from: "+j+" to: "+k+" "+d[i][j][k]+" ");//
				}
//				System.out.println();
			}
//			System.out.println();
//			System.out.println();
		}
	}

	public void storeMatrix() throws IOException{
		FileWriter fw = new FileWriter(fname);
		PrintWriter pw = new PrintWriter(fw);

		for(int i=0;i<d.length;i++){
			for(int j=0;j<d[i].length;j++){
				for(int k=0;k<d[i][j].length;k++){
					if(d[i][j][k]!=Short.MAX_VALUE)
						pw.println(i+"	"+j+"	"+k+"	"+d[i][j][k]);
				}
			}
		}

		pw.close();
		fw.close();
	}

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
	//	This function returns k-hop given a node and integer k
	//	============================================================================//
	public Vector<String> khop(String node, int k, int color){
		int pos = Integer.parseInt(node);
		Vector<String> nlabels = new Vector<String>();
		for(int i=0;i<d[color][pos].length;i++){
			if(d[color][pos][i]<=k){
				nlabels.add(""+i);
			}
		}
		return nlabels;
	}
	
	//	============================================================================//
	//	This function returns ancesters within k-hop given a node and integer k
	//	============================================================================//
	public Vector<String> prekhop(String node, int k, int color){
		int pos = Integer.parseInt(node);
		Vector<String> nlabels = new Vector<String>();
		for(int i=0;i<d[color][0].length;i++){
			if(d[color][i][pos]<=k){
				nlabels.add(""+i);
			}
		}
		return nlabels;
	}

	public void restoreMatrix() throws IOException{

		File fn = new File(fname);
		if(!fn.exists() || !fn.isFile()){
			System.out.println("matrix file not exists. Needs to be recomputed.");
			return;
		}

		BufferedReader in = MyFileOperation.openFile(fname);

		String strLine = in.readLine().trim();
		Vector<String> vec = createVectorFromString(strLine,"	");


		int ib=0,jb=0,kb=0; 
		short ds=0;
		ib = Integer.parseInt(vec.elementAt(0));
		jb = Integer.parseInt(vec.elementAt(1));
		kb = Integer.parseInt(vec.elementAt(2));
		ds = Short.parseShort(vec.elementAt(3));

		for(int i=0;i<d.length;i++){
			for(int j=0;j<d[i].length;j++){
				for(int k=0;k<d[i][j].length;k++){
					if(i!=ib||j!=jb||k!=kb)
						d[i][j][k]=Short.MAX_VALUE;
					else{
						d[i][j][k]=ds;
						strLine = in.readLine().trim();
						if(strLine!=null){
							vec = createVectorFromString(strLine,"	");
							ib = Integer.parseInt(vec.elementAt(0));
							jb = Integer.parseInt(vec.elementAt(1));
							kb = Integer.parseInt(vec.elementAt(2));
							ds = Short.parseShort(vec.elementAt(3));
						}
						else{
							for(int ic=i;ic<d.length;ic++)
								for(int jc=0;jc<d[i].length;jc++)
									for(int kc=k;kc<d[i][j].length;kc++)
										d[i][j][k]=Short.MAX_VALUE;
							return;
						}
					}
				}
			}
		}

		in.close();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		gpm_graph g = new gpm_graph();
		g.ConstructGraphFromFile("/disk/scratch/CurrentProject/GPMexp/GPM/data/randomDS/P_6_8.grh", 1);
		g.gtype = 1;
		int time = 5;
		long build[] = new long[time];
		for(int i=0;i<time;i++){
			long start = System.currentTimeMillis();
			gpm_distFWMatrix matrix = new gpm_distFWMatrix(g,true);
			build[i] = System.currentTimeMillis() - start;
			if(i==(time-1))
				matrix.display();
		}

		for(int i=0;i<time;i++)
			System.out.print("	"+build[i]);
		//matrix.display();
		//gpm_visualizer gviz = new gpm_visualizer();
		//gviz.vizSinglegraph(g);
	}
}
