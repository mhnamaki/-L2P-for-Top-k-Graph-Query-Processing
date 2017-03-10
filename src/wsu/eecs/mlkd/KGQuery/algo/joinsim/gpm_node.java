package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.util.HashMap;
import java.util.Vector;

public class gpm_node extends Object{

	public HashMap<String, Vector<Object>> attrmap = null; //node attribute pairs
	public String tag=""; //node tag should be "0,1,2,...,k";(for mapping to distance matrix)
	public double weight;
	public String addinfo = "";
	public String nlabel;
	public int attidx = 0;
	//String libatt = "";//this additional attribute is for compression only 
	//attribute as a label from a predefined library
	//predefined as integers 0,1,...N
	
	//the following is for rankings for possible applications.
	public int rank = Integer.MIN_VALUE;
	
	//the following is for landmark vectors.
	//only used in landmark.
	public Vector<Short> distvf = null;
	public Vector<Short> distvt = null;

	public gpm_node(){
	}

	@SuppressWarnings("unchecked")
	public gpm_node(Vector<AVpair> alist, String tag, double weight, String label) {
		this.tag = tag;
		this.weight=weight;
		this.addinfo="";
		if(attrmap==null)
			attrmap = new HashMap<String, Vector<Object>>();
		if(alist!=null){
			for(AVpair av: alist){
				attrmap.put(av.Att, av.opval);
			}
		}
		this.nlabel = label;
	}

	public gpm_node(gpm_node a){
		this.tag = a.tag;
		this.weight = a.weight;
		this.addinfo = a.addinfo;
		if(attrmap==null)
			attrmap = new HashMap<String, Vector<Object>>();
		attrmap = a.attrmap;
	}
	


	
	//init a data node with a pattern node
	public void exactcopyPN(gpm_node a){
		this.tag = a.tag;
		this.weight = a.weight;
		this.addinfo = a.addinfo;
		//if(attrmap==null)
		this.attrmap = a.attrmap;
		
//		for(String attr: a.attrmap.keySet()){
//			Vector<Object> opval = a.attrmap.get(attr);
//			String op = (String)opval.elementAt(0);
//			Integer val = (Integer)opval.elementAt(1);
//			Integer newval = val;
//			if(op.equals(">"))
//				newval = newval + (int)(Math.random()*val) + 1;
//			if(op.equals("<"))
//				newval = newval - (int)(Math.random()*val) - 1;
//			Vector<Object> newopval = new Vector<Object>();
//			newopval.add("=");
//			newopval.add(newval);
//			attrmap.put(attr, newopval);
//		}
	}
	
	//init a data node with a pattern node
	public void copyPN(gpm_node a){
		this.tag = a.tag;
		this.weight = a.weight;
		this.addinfo = a.addinfo;
		this.nlabel = a.nlabel;
		if(attrmap==null)
			attrmap = new HashMap<String, Vector<Object>>();
		for(String attr: a.attrmap.keySet()){
			Vector<Object> opval = a.attrmap.get(attr);
			String op = (String)opval.elementAt(0);
			Integer val = (Integer)opval.elementAt(1);
			Integer newval = val;
			if(op.equals(">"))
				newval = newval + (int)(Math.random()*val) + 1;
			if(op.equals("<"))
				newval = newval - (int)(Math.random()*val) - 1;
			Vector<Object> newopval = new Vector<Object>();
			newopval.add("=");
			newopval.add(newval);
			attrmap.put(attr, newopval);
		}
	}
	
	//construct a node NOT satisfying a set of predicates
	public void copyPN_evil(gpm_node a){
		this.tag = a.tag;
		this.weight = a.weight;
		this.addinfo = a.addinfo;
		if(attrmap==null)
			attrmap = new HashMap<String, Vector<Object>>();
		for(String attr: a.attrmap.keySet()){
			Vector<Object> opval = a.attrmap.get(attr);
			String op = (String)opval.elementAt(0);
			Integer val = (Integer)opval.elementAt(1);
			Integer newval = val;
			if(op.equals(">"))
				newval = newval - ((int)(Math.random()*val) - 1);
			if(op.equals("<")||op.equals("="))
				newval = newval + ((int)(Math.random()*val) + 1);
			Vector<Object> newopval = new Vector<Object>();
			newopval.add("=");
			newopval.add(newval);
			attrmap.put(attr, newopval);
		}
	}
	


	//return attribute lists/predicts
	public HashMap<String, Vector<Object>> getAttlist(){
		return this.attrmap;
	}

	//return attribute lists/predicts
	public Vector<AVpair> getAVlist(){
		Vector<AVpair> avlist = new Vector<AVpair>();
		for(String key:this.attrmap.keySet()){
			Vector<Object> val = attrmap.get(key);
			AVpair av = new AVpair(key,(String)val.elementAt(0),val.elementAt(1));
			avlist.add(av);
		}
		return avlist;
	}

	//return tag
	public String getTag(){
		return this.tag;
	}

	//compare avlist
	public boolean IssamePredict(Vector<AVpair> prec){
		for(AVpair predict: prec){
			String att = (String)predict.Att;
			String op = (String)predict.opval.elementAt(0);
			String val = (String)predict.opval.elementAt(1);
			Vector<Object> vals = this.attrmap.get(att);
			if(vals==null)
				return false;
			else{
				if(op.equals(vals.elementAt(0)) && val.equals(vals.elementAt(1)))
					continue;
				else
					return false;
			}
		}
		return true;
	}


	/*//condition tests: single predict
	public Boolean sat_single(AVpair predict){
		String att = (String)predict.Att;
		String op = (String)predict.opval.elementAt(0);
		String val = (String)predict.opval.elementAt(1);
		if(op.equals("="))
			return (val.equals(this.attrmap.get(att)));
		//case ">":
		//case "<":
		//case ">=":
		//case "<=":
		//... add more in future.
		return null;
	}

	//condition tests: conjunctive predicts
	public Boolean sat_conjunct(Vector<AVpair> predict){
		Boolean assertion = true;
		for(AVpair av: predict){
			if(this.sat_single(av)!=null)
				assertion &= this.sat_single(av);
			else return null;
		}
		return assertion;
	}*/

	//display
	public void display(){
		System.out.println(this.tag);
		for(String att: this.attrmap.keySet()){
			System.out.println(att+" "+this.attrmap.get(att).toString());
		}

	}
	

	@Override
	public String toString() {
		if(attrmap.get("Gname")!=null)
			return attrmap.get("Gname").elementAt(1).toString();
		return tag+":"+(attrmap==null?"none":attrmap.toString());
	}	

}

