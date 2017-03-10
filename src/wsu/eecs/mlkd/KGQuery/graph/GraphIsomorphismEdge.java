package wsu.eecs.mlkd.KGQuery.graph;

/**
 * Representation of a directed Edge in a Graph
 * @author Dharmesh Kakadia
 *
 * @param <V> Content type of @Vertex
 * @param <E> Label type of @Edge
 */
public class GraphIsomorphismEdge<V extends Comparable<V>, E extends Comparable<E>> implements Comparable<GraphIsomorphismEdge<V, E>>{
	E label;
	GraphIsomorphismVertex<V,E> destination;
	
	public GraphIsomorphismEdge(E label, GraphIsomorphismVertex<V, E> destination) {
		this.label=label;
		this.destination=destination;
	}
	
	public GraphIsomorphismEdge(E label) {
		this(label,null);
	}
	
	public E getLabel() {
		return label;
	}

	public void setLabel(E label) {
		this.label = label;
	}

	public GraphIsomorphismVertex<V,E> getDestination() {
		return destination;
	}

	public void setDestination(GraphIsomorphismVertex<V,E> destination) {
		this.destination = destination;
	}

	@Override
	public int compareTo(GraphIsomorphismEdge<V, E> e2) {
		if(e2 == null)
			return -1;
		int labelComparision = getLabel().compareTo(e2.getLabel());
		if(labelComparision!=0)
			return labelComparision;
		GraphIsomorphismVertex<V,E> destination1 = getDestination();
		GraphIsomorphismVertex<V,E> destination2 = e2.getDestination();
		if(destination1 == null && destination2 == null){
			return 0;
		}else if((destination1 == null && destination2 != null) || (destination1 != null && destination2 == null)){
			return -1;
		}else{
			return getDestination().compareTo(e2.getDestination());
		}
	}
	
	@Override
	public String toString() {
		return "(" + getLabel().toString() + 
				(getDestination() == null ? "" : 
					"->" + getDestination().getId()) + ")";
	}
}
