package wsu.eecs.mlkd.KGQuery.example;

import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;

public class SimpleTests {

	public static void main(String[] args) {
		PriorityQueue<IntegerTesti> pq = new PriorityQueue<IntegerTesti>(2, new IntegerTestiComparator());
		
		pq.add(new IntegerTesti(10));
		pq.add(new IntegerTesti(8));
		pq.add(new IntegerTesti(20));
		pq.add(new IntegerTesti(15));
		pq.add(new IntegerTesti(17));
		
		System.out.println(pq.poll().testi);
	}

}

class IntegerTesti implements Comparator<IntegerTesti> {
	public int testi = 0;

	public IntegerTesti(int t) {
		this.testi = t;
	}

	@Override
	public int compare(IntegerTesti o1, IntegerTesti o2) {
		// TODO Auto-generated method stub
		return Integer.compare(o2.testi, o1.testi);
	}

}
class IntegerTestiComparator implements Comparator<IntegerTesti>{

	@Override
	public int compare(IntegerTesti o1, IntegerTesti o2) {
		// TODO Auto-generated method stub
		return Integer.compare(o2.testi, o1.testi);
	}
	
}
