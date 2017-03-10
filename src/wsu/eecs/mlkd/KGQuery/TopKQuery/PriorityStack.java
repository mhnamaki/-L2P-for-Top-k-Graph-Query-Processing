package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EmptyStackException;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.TreeMap;

public class PriorityStack<E, P extends Comparable<? super P>> {

	private final Map<P, List<E>> map;
	private int size;

	public PriorityStack() {
		this.map = new TreeMap<>();
	}

	public boolean empty() {
		return size == 0;
	}

	/**
	 * Pushes the input element to this stack.
	 * 
	 * @param element
	 *            the element to push.
	 * @param priority
	 *            the priority of the input element.
	 */
	public void push(E element, P priority) {
		if (!map.containsKey(priority)) {
			map.put(priority, new ArrayList<E>());
		}

		map.get(priority).add(element);
		++size;
	}

	/**
	 * Returns but not removes the topmost element of the highest priority
	 * substack.
	 * 
	 * @return the top element.
	 */
	public E peek() {
		if (size == 0) {
			throw new EmptyStackException();
		}

		List<E> list = map.entrySet().iterator().next().getValue();
		return list.get(list.size() - 1);
	}

	/**
	 * Pops the topmost element from the stack with the highest priority.
	 * 
	 * @return an element.
	 * @throws EmptyStackException
	 *             if the stack is empty.
	 */
	public E pop() {
		if (size == 0) {
			throw new EmptyStackException();
		}

		Map.Entry<P, List<E>> entry = map.entrySet().iterator().next();
		P priority = entry.getKey();
		List<E> list = entry.getValue();
		E ret = list.remove(list.size() - 1);

		if (list.isEmpty()) {
			map.remove(priority);
		}

		--size;
		return ret;
	}

	public static void main(final String... args) throws Exception {
		ArrayList<StrInt> strInts = new ArrayList<StrInt>();
		Random rnd = new Random();
		for (int i = 0; i < 1000; i++) {
			Thread.sleep(5);
			strInts.add(new StrInt("a", rnd.nextInt(4)));
		}

		PriorityStack<StrInt, Integer> stack = new PriorityStack<StrInt, Integer>();

		// -1: He is a
		// 2: funky programmer
		// 3: that likes to
		// 5: code algorithms
		// 7: and
		// 8: data structures! :-]

		double start_time = System.nanoTime();
		for (StrInt strInt : strInts) {
			stack.push(new StrInt(strInt.name, strInt.val), strInt.val);
		}
		// stack.push("a", -1);
		// stack.push("and", 7);
		// stack.push("is", -1);
		// stack.push("programmer", 2);
		// stack.push("algorithms", 5);
		// stack.push("code", 5);
		// stack.push(":-]", 8);
		// stack.push("He", -1);
		// stack.push("to", 3);
		// stack.push("structures!", 8);
		// stack.push("likes", 3);
		// stack.push("data", 8);
		// stack.push("who", 3);
		// stack.push("funky", 2);

		while (!stack.empty()) {
			System.out.print(stack.pop().val);
			System.out.print(" ");
		}
		System.out.print(" ");
		double end_time = System.nanoTime();
		double difference = (end_time - start_time) / 1e6;
		System.out.println();
		System.out.println("difference PQ:" + difference);

		PriorityQueue<StrInt> pq = new PriorityQueue<StrInt>(3, new Comparator<StrInt>() {
			@Override
			public int compare(StrInt o1, StrInt o2) {
				return Integer.compare(o1.val, o2.val);
			}
		});

		start_time = System.nanoTime();
		
		for (StrInt strInt : strInts) {
			pq.add(new StrInt(strInt.name, strInt.val));
		}

		while (!pq.isEmpty()) {
			System.out.print(pq.poll().name);
			System.out.print(" ");
		}
		System.out.print(" ");
		end_time = System.nanoTime();
		difference = (end_time - start_time) / 1e6;
		System.out.println();
		System.out.println("difference PQ:" + difference);
	}

}

class StrInt {
	String name;
	Integer val;

	public StrInt(String name, Integer val) {
		this.name = name;
		this.val = val;
	}
}