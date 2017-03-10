package wsu.eecs.mlkd.KGQuery.graph;

import java.util.HashSet;
import java.util.Set;

/**
 * Representation of a Vertex in a Graph 
 * @author Dharmesh Kakadia
 *
 * @param <V> Content type of @Vertex
 * @param <E> Label type of @Edge
 */
public class GraphIsomorphismVertex<V extends Comparable<V>, E extends Comparable<E>> implements Comparable<GraphIsomorphismVertex<V, E>>{
	int id;
	V content;
	Set<GraphIsomorphismEdge<V,E>> edges;
	private static int idCounter=0;
	
	public GraphIsomorphismVertex(int id, V content, Set<GraphIsomorphismEdge<V,E>> edges){
		this.id=id;
		this.content=content;
		this.edges=edges;
	}
	
	public GraphIsomorphismVertex(int id, V content){
		this(id, content, new HashSet<GraphIsomorphismEdge<V,E>>());
	}
	
	public GraphIsomorphismVertex(V content){
		this(idCounter++, content);
	}
	
	public GraphIsomorphismVertex() {
		this(-1, null);
	}
	
	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public V getContent() {
		return content;
	}
	
	public void setContent(V content) {
		this.content = content;
	}
	
	public Set<GraphIsomorphismEdge<V, E>> getEdges() {
		return edges;
	}
	
	public void setEdges(Set<GraphIsomorphismEdge<V, E>> edges) {
		this.edges = edges;
	}
	
	/** Adds an @Edge from current @Vertex to @destination @Vertex with label
	 * @param label
	 * @param desitnation
	 * @return
	 */
	public boolean addEdge(E label, GraphIsomorphismVertex<V,E> desitnation){
		return edges.add(new GraphIsomorphismEdge<V,E>(label, desitnation));
	}
	
	public int getEdgeCount(){
		return edges.size();
	}
	
	@Override
	public int compareTo(GraphIsomorphismVertex<V, E> v2) {
		int contentComparison = getContent().compareTo(v2.getContent());
		if(contentComparison != 0){
			return contentComparison;
		} 
		
		if(getEdgeCount() < v2.getEdgeCount()){
			return -1;
		}else if(getEdgeCount() > v2.getEdgeCount()){
			return 1;
		}else{
			return 0;
		}
	}
	
	@Override
	public String toString() {
		return getId() + ":" + getContent().toString() + ":" + getEdgeCount();
	}
}
