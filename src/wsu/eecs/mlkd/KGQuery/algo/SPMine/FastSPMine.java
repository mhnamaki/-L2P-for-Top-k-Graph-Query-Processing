package wsu.eecs.mlkd.KGQuery.algo.SPMine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.AVpair;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Label_Index;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Neo4jGraph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Relationship_Index;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_edge;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_graph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_node;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.IncJoinSimulation;
import wsu.eecs.mlkd.KGQuery.algo.SPMine.MinHash;

public class FastSPMine {
	public static Neo4jGraph dg;
	
	public Label_Index labelIndex;
	//public Hashtable<Integer, HashSet> frePattern;	//used to store patterns generated from each iteration
	public String[] edgeStoreStart;//store edge, each Label pair is a edge, avoid duplicate edge in first iteration
	public String[] edgeStoreEnd;
	public HashSet<gpm_graph> divSP;	//current diversified summary pattern
	public Relationship_Index relIndex;
	public HashSet<gpm_graph> frePatternI[];
	
	public Hashtable<String, Integer> nodeLabelCount;
	public Hashtable<String, Integer> edgeLabelCount;
	
	public int sizeRange;	//size of generated patterns
	public int couverageRange;	//range of couverage for each pattern
	public int graphCounter;	//A pattern Id for each pattern
	public File outputfile;
	public FileWriter fw;
	public PrintWriter pw;
	public MinHash minhash;
	public FastSPMine(Neo4jGraph G) throws IOException{
		labelIndex = new Label_Index(G);
		relIndex = new Relationship_Index(G);
		//frePattern = new Hashtable<Integer,HashSet>();
		frePatternI = new HashSet[10];
		for(int i =0 ;i < 10 ;i ++){
			frePatternI[i] = new HashSet<gpm_graph>();
			//frePattern.put(i, frePatternI[i]);
		}
		divSP = new HashSet<gpm_graph>();
		edgeStoreStart = new String[100000];
		edgeStoreEnd = new String[100000];
		nodeLabelCount = new Hashtable<String, Integer>();
		edgeLabelCount = new Hashtable<String, Integer>();
		sizeRange = 10;
		graphCounter = 1;
		minhash = new MinHash(0.01, 200);
		outputfile = new File("/Users/qsong/Downloads/output.txt");
		fw = new FileWriter("/Users/qsong/Downloads/output.txt");
		pw = new PrintWriter(fw);
	}
	
	//find candidate edge set for iteration 1 based on frequency of edge label and node label
	public boolean Init() throws IOException{	//generate level 0 frequent edges
		
		BuildLabelIdx();
		BuildRelationshipIdx();
		nodeLabelCount = labelIndex.GetNodeLabelCount();
		edgeLabelCount = relIndex.GetEdgeLabelCount();
		gpm_graph newGraph = new gpm_graph();
		for(Relationship r : dg.edges){
			int edgeFre = 0;
			boolean flag = true;
			/*
			for(String k : r.getPropertyKeys()){
				edgeFre = (int)edgeLabelCount.get(r.getProperty(k).toString());
				if(CheckFrequency(edgeFre, 1000, 1000000)){
					flag = true;
					break;
					}
			}
			*/
			if(flag == true){
				Node startNode = r.getStartNode();
				Node endNode = r.getEndNode();
				if(!CheckLabel(startNode) || !CheckLabel(endNode))
					continue;
				CalculateMaxLabel(startNode);
				CalculateMaxLabel(endNode);
				int startNodeFrequency = nodeLabelCount.get(dg.maxLabelofNode.get(startNode.getId()));
				int endNodeFrequency = nodeLabelCount.get(dg.maxLabelofNode.get(endNode.getId()));
				if(CheckFrequency(startNodeFrequency, 100, 10000) && CheckFrequency(endNodeFrequency, 100, 10000)){
					if(CheckEdgeDuplicate(dg.maxLabelofNode.get(startNode.getId()),dg.maxLabelofNode.get(endNode.getId()))){
						newGraph = AddFreEdgetoGraph(r);
						frePatternI[1].add(newGraph);
						edgeStoreStart[graphCounter] = dg.maxLabelofNode.get(startNode.getId());
						edgeStoreEnd[graphCounter] = dg.maxLabelofNode.get(endNode.getId());
						graphCounter ++;
					}
				}
			}
		}
		//frePattern.put(1, frePatternI[0]);
		System.out.println("edge store size: " + graphCounter);
		System.out.println("number of frequent patterns in iteration 1: " + frePatternI[1].size());
		pw.println("number of frequent patterns in iteration 1: " + frePatternI[1].size());
		return true;
	}
	
	public boolean CheckEdgeDuplicate(String label1, String label2){
		for(int i = 1; i < graphCounter; i++)
			if(edgeStoreStart[i].equals(label1) && edgeStoreEnd[i].equals(label2))
				return false;
		return true;
	}
	
	//We do not consider those nodes who do not have labels
	public boolean CheckLabel(Node n){
		Iterable<Label> lset = n.getLabels();
		if(lset.iterator().hasNext())
			return true;
		else
			return false;
	}
	
	public gpm_graph AddFreEdgetoGraph(Relationship r){
		gpm_graph g = new gpm_graph();
		String ntag1 = "";
		String ntag2 = "";
		double nweight = 0.0;
		String nlabel1 = "";
		String nlabel2 = "";
		Vector<AVpair> alist = new Vector<AVpair>();
		g.simMatchSet = new TreeMap<String, Vector<String>>();
		Vector<String> nodeMatch1 = new Vector<String>();
		Vector<String> nodeMatch2 = new Vector<String>();
		
		alist.clear();
		ntag1 = Long.toString(r.getStartNode().getId());
		nweight = 0.0;
		nlabel1 = dg.maxLabelofNode.get(r.getStartNode().getId());
		gpm_node startNode = new gpm_node(alist,ntag1,nweight,nlabel1);	
		g.addVertex(startNode);
		
		alist.clear();
		ntag2 = Long.toString(r.getEndNode().getId());
		nweight = 0.0;
		nlabel2 = dg.maxLabelofNode.get(r.getEndNode().getId());
		gpm_node endNode = new gpm_node(alist,ntag2,nweight,nlabel2);	
		g.addVertex(endNode);
		
		gpm_edge e = new gpm_edge(startNode.tag,endNode.tag,1,0);
		g.InsertEdge(e);
		/*
		for(String s1 : labelIndex.getLabelAnswer(nlabel1))
			for(String s2 : labelIndex.getLabelAnswer(nlabel2)){
				if(dg.containsEdge(dg.GetVertex(Long.valueOf(s1)), dg.GetVertex(Long.valueOf(s2)))){
					nodeMatch1.add(s1);
					nodeMatch2.add(s2);
				}
			}
		*/
		g.graphId = graphCounter;
		g.simMatchSet.put(ntag1, nodeMatch1);
		g.simMatchSet.put(ntag2, nodeMatch2);
		
		return g;
	}
	
	public boolean CheckFrequency(int fre, int low, int upper){
		if(fre >= low && fre <= upper)
			return true;
		else 
			return false;
	}
	
	
	//calculate max label for each node and put the result back to maxLabelofNode of Neo4jGraph
	public void CalculateMaxLabel(Node a){
		String maxLabel = "";
		int maxLabelFrequency = 0;
		if(dg.maxLabelofNode.containsKey(a.getId()))
			return;
		else{
			for(Label l: a.getLabels()){
				if(nodeLabelCount.get(l.toString()) > maxLabelFrequency){
					maxLabel = l.toString();
					maxLabelFrequency = nodeLabelCount.get(maxLabel);	
				}
			}
			dg.maxLabelofNode.put(a.getId(), maxLabel);
		}
	}
	
	//main function of SPMine
	@SuppressWarnings("unchecked")
	public void PatternMining(Neo4jGraph dg, int patternSize, int k, double theta) throws IOException{
		
		for(int i = 1 ; i < 10; ){
			if( i > 1)
				if(frePatternI[i-1].size() == 0){
					System.out.println("Pattern mining stop");
					pw.println("Pattern mining stop");
					return;
				 }
			//HashSet<gpm_graph> frePatternICopy = new HashSet<gpm_graph>(frePatternI[i]);
			System.out.println("for pattern " + i + ": " + frePatternI[i].size());
			for(gpm_graph a : frePatternI[i]){
				if(a.graphId == 0)
					break;
				for(gpm_graph b : frePatternI[i]){
					if(b.graphId == 0)
						break;
					if(a.graphId <= b.graphId)
						continue;
					else{
						gpm_graph c = new gpm_graph();
						c = GraphConbination(a,b);
						if(c == null)
							continue;
						IncJoinSimulation gsim = new IncJoinSimulation(c, dg, true, labelIndex);
						gsim.IsBSim(2, true);
						System.out.println("I(P) = " + c.interestingness);
						pw.println("I(P) = " + c.interestingness);
						if(c.vertexSet().size() > sizeRange || c.interestingness < theta)
							continue;
						else{
							System.out.println("Iteration: " + i);
							pw.println("Iteration: " + i);
							c.graphId = graphCounter ++;
							frePatternI[i+1].add(c);
							if(divSP.size() < k)
								divSP.add(c);
							else
								FastIncDiv(divSP,c);
						}
					}
				}
			}
			i++;
			//frePattern.put(i, frePatternI[i]);
			System.out.println("number of frequent patterns in iteration " + i + ": " + frePatternI[i].size());
			pw.println("number of frequent patterns in iteration " + i + ": " + frePatternI[i].size());
		}
	}
	
	
	//Fast incremental diversification 
	public void FastIncDiv(HashSet<gpm_graph> divSP_k, gpm_graph pat){
		double diversificationOriginal = Diversification(divSP_k);
		double diversificationDifference = 0.0;
		double marginalGain = 0.0;
		gpm_graph patternReplace = new gpm_graph();
		for(gpm_graph p : divSP_k){
			HashSet<gpm_graph> temp = new HashSet<gpm_graph>(divSP_k);
			temp.remove(p);
			temp.add(pat);
			marginalGain = Diversification(temp) - diversificationOriginal;
			System.out.println("marginalGain = " + marginalGain);
			pw.println("marginalGain = " + marginalGain);
			if(marginalGain > diversificationDifference){
				diversificationDifference = marginalGain;
				patternReplace = p;
			}
		}
		if(patternReplace.vertexSet().size() != 0){
			divSP_k.remove(patternReplace);
			divSP_k.add(pat);
			displayTopKPattern(divSP_k);
		}
		
		//save divSP_k;
	}
	
	public void displayTopKPattern(HashSet<gpm_graph> patternSet){
		System.out.println("**********show top k pattern**********");
		pw.println("**********show top k pattern**********");
		HashSet<String> finalMatchSet = new HashSet<String>();
		for(gpm_graph g : patternSet){
			System.out.println(g.interestingness);
			for(String s : g.simMatchSet.keySet())
				finalMatchSet.addAll(g.simMatchSet.get(s));
			for(gpm_node n : g.vertexSet()){
				System.out.println(n.tag + ": " + n.nlabel);
				pw.println(n.tag + ": " + n.nlabel);
			}
			for(gpm_edge e : g.edgeSet()){
				System.out.println(e.from_node + "-----" + e.to_node);
				pw.println(e.from_node + "-----" + e.to_node);
			}
		}
		System.out.println("Final coverage size:" + finalMatchSet.size());
		pw.println("Final coverage size:" + finalMatchSet.size());
		System.out.println("****************************************");
		pw.println("**********show top k pattern**********");
	}
	
	//this function calculate diversification of a summarization
	public double Diversification(HashSet<gpm_graph> sum){
		double alpha = 0.5;
		double diver = 0.0;
		for(gpm_graph g : sum)
			diver += alpha * g.interestingness;
		
		for(gpm_graph g1 : sum)
			for(gpm_graph g2 : sum){
				if(g1.graphId < g2.graphId){
					CheckHashResult(g1);
					CheckHashResult(g2);
					diver += Distance(g1,g2)*(1-alpha)/(sum.size() - 1);
				}
			}
		return diver;
	}
	
	//this function calculate distance of two graphs
	public double Distance(gpm_graph g1, gpm_graph g2){
		return (minhash.similarity(g1.hashResult, g2.hashResult));
	}
	
	//this function checks if a pattern already have a hash result
	public void CheckHashResult(gpm_graph g){
		Set<Long> set = new HashSet<Long>();
		if(g.hashResult != null)
			return;
		else{
			for(String ss : g.simMatchSet.keySet())
				for(String sg :g.simMatchSet.get(ss))
					set.add(Long.valueOf(sg));
			g.hashResult = minhash.signature(set);
		}
	}
	
	
	//this function is used to combine two gpm_graph
	//if two gpm_graph share no common nodes, they can not be combined.
	public gpm_graph GraphConbination(gpm_graph a, gpm_graph b){
		gpm_graph combinedGraph = new gpm_graph();
		combinedGraph.ConstructGraph(a);;
		boolean combineTag = false;

		
		combinedGraph.graphId = 0;
		for(gpm_node na : a.vertexSet())
			for(gpm_node nb : b.vertexSet()){
				if(na.tag.equals(nb.tag)){
					combineTag = true;
					continue;
				}
			}
		
		if(combineTag == false)			//two gpm_graph can not be combined
		{	
			if(a.vertexSet().size() > 2)
				System.out.println("can not combine");
			return null;
		}
		
		/*
		for(String s : a.simMatchSet.keySet())
			System.out.print(s + ": " + a.simMatchSet.get(s));
		System.out.println();
		
		for(String s : b.simMatchSet.keySet())
			System.out.print(s + ": " + b.simMatchSet.get(s));
		System.out.println();
		*/
		
		System.out.println("can be combined");
		combinedGraph.gfilename = Integer.toString(graphCounter++) + "." + "sq";
		for (gpm_node na : a.vertexSet())
			for(gpm_node nb : b.vertexSet()){
				if(na.tag.equals(nb.tag)){
					continue;
				}
				else{
					if(combinedGraph.GetVertex(String.valueOf(nb.tag)) == null){
						gpm_node ncopy = new gpm_node();
						ncopy.copyPN(nb);
						combinedGraph.addVertex(ncopy);
						combinedGraph.simMatchSet.put(ncopy.tag, b.simMatchSet.get(ncopy.tag));
					}
				}
			}
		
		for (gpm_edge ea : a.edgeSet())
			for(gpm_edge eb : b.edgeSet()){
				if(ea.from_node.equals(eb.from_node) && ea.to_node.equals(eb.to_node)){
					continue;
				}
				else{
					if(combinedGraph.GetEdge(eb.from_node, eb.to_node) == null){
						gpm_edge ei = new gpm_edge(eb.from_node,eb.to_node,eb.e_bound,eb.e_color);
						combinedGraph.InsertEdge(ei);
					}
				}
			}
		
		//for(String s : combinedGraph.simMatchSet.keySet())
			//System.out.print(s + ": " + combinedGraph.simMatchSet.get(s));
		//System.out.println();
		
		return combinedGraph;
	}
	
	public void BuildLabelIdx() throws IOException{
		if(labelIndex.invIndex.size()==0){
			labelIndex.buildLabelIndex(false);
		}
		System.out.println("Label index build success");
	}
	
	public void BuildRelationshipIdx() throws IOException{
		if(relIndex.relIndex.size() == 0){
			relIndex.buildRelIndex(true);
		}
		System.out.println("Relationship index build success");
	}
	
	public void Finish() throws IOException{
		pw.close();
		fw.close();
	}
	
	public static void main(String[] args) throws IOException {
		String filename = "/Users/qsong/Downloads/YagoCores_graph_small_sample1.db";
		dg = new Neo4jGraph(filename,2);
		try(Transaction tx1 = dg.getGDB().beginTx() )
		{
			FastSPMine mine = new FastSPMine(dg);
			mine.Init();
			mine.PatternMining(dg, 10, 10, 0.1);
			mine.Finish();
			tx1.success();
		}
	}
}
