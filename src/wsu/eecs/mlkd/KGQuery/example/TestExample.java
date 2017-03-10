package wsu.eecs.mlkd.KGQuery.example;

import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import org.parboiled.common.StringUtils;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Dummy;
import wsu.eecs.mlkd.KGQuery.TopKQuery.StarGraphResult;

public class TestExample {

	public static int kTop = 5;
	public static int numberOfLists = 3;
	public static int maxPossibleElementsInAList = 20;

	public static void main(String[] args) {

		HashMap<String, NeighborNodeList> allNeighborsLists = new HashMap<String, NeighborNodeList>();
		HashMap<String, NeighborNodeList> results = new HashMap<String, NeighborNodeList>();

		String a = "A";
		Random rnd = new Random();
		PriorityQueue<LHat> minHeap = new PriorityQueue<LHat>();
		// int numberOfLists = 5;
		for (int i = 0; i < numberOfLists; i++) {
			NeighborNodeList f = new NeighborNodeList();
			for (int j = 0; j < rnd.nextInt(maxPossibleElementsInAList) + 1; j++) {
				f.values.add((double) rnd.nextInt(20) + 1);
			}
			allNeighborsLists.put(a + i, f);
			System.out.println(a + i + ": " + Dummy.DummyFunctions.collectionToCommaDelimitedString(f.values));
		}

		System.out.println();
		/// start
		long start_time = System.nanoTime();
		for (int i = 0; i < numberOfLists; i++) {

			NeighborNodeList nnl = allNeighborsLists.get(a + i);
			nnl.max = quickselect(nnl.values, 1);

			NeighborNodeList forResult = new NeighborNodeList();
			forResult.values.add(nnl.max);
			results.put(a + i, forResult);
			boolean maxDeleted = false;

			for (int j = 0; j < nnl.values.size(); j++) {
				LHat lHat = new LHat();
				lHat.a = a + i;
				lHat.originalValue = nnl.values.get(j);
				lHat.value = lHat.originalValue - nnl.max;

				if (lHat.originalValue == nnl.max && !maxDeleted) {
					maxDeleted = true;
					continue;
				}

				minHeap.add(lHat);
			}
		}

		for (int i = 0; i < numberOfLists; i++) {
			NeighborNodeList nnl = new NeighborNodeList();
			nnl.values.add(allNeighborsLists.get(a + i).max);

			// System.out.println("maximums: " + a + i + " , " +
			// nnl.values.get(0));
		}
		// k + s - 1
		for (int j = 0; j < (kTop - 1) && minHeap.size() > 0; j++) {

			LHat lHat = minHeap.poll();
			NeighborNodeList nnl = results.get(lHat.a);
			if (nnl == null) {
				nnl = new NeighborNodeList();
			}

			nnl.values.add(lHat.originalValue);
			results.put(lHat.a, nnl);

		}
		long end_time = System.nanoTime();
		double difference = (end_time - start_time) / 1e6;
		System.out.println("difference: " + difference);
		for (String key : results.keySet()) {
			System.out.println(
					key + " :" + Dummy.DummyFunctions.collectionToCommaDelimitedString(results.get(key).values));
		}

		// second method:
		HashMap<String, ArrayList<Double>> resultsWithOneHeap = new HashMap<String, ArrayList<Double>>();
		PriorityQueue<LHat> heap = new PriorityQueue<LHat>();
		start_time = System.nanoTime();

		for (String key : allNeighborsLists.keySet()) {

			ArrayList<Double> dd = allNeighborsLists.get(key).values;
			Double maxInThisList = quickselect(dd, 1);
			ArrayList<Double> dr = new ArrayList<Double>();
			dr.add(maxInThisList);
			boolean maxDeleted = false;
			resultsWithOneHeap.put(key, dr);
			for (int i = 0; i < dd.size(); i++) {
				LHat lHat = new LHat();
				lHat.a = key;
				lHat.value = dd.get(i);
				if (lHat.value == maxInThisList && !maxDeleted) {
					maxDeleted = true;
					continue;
				}
				heap.add(lHat);
			}
		}

		for (int i = 0; i < (kTop - 1); i++) {
			LHat lhat = heap.poll();
			ArrayList<Double> res = resultsWithOneHeap.get(lhat.a);
			res.add(lhat.value);
			resultsWithOneHeap.put(lhat.a, res);
		}
		end_time = System.nanoTime();
		difference = (end_time - start_time) / 1e6;
		System.out.println("method 2 time difference: " + difference);
		for (String key : resultsWithOneHeap.keySet()) {
			System.out.println(
					key + " :" + Dummy.DummyFunctions.collectionToCommaDelimitedString(resultsWithOneHeap.get(key)));
		}
	}

	public static Double quickselect(ArrayList<Double> G, int k) {
		return quickselect(G, 0, G.size() - 1, k - 1);
	}

	private static Double quickselect(ArrayList<Double> G, int first, int last, int k) {
		if (first <= last) {
			int pivot = partition(G, first, last);
			if (pivot == k) {
				return G.get(k);
			}
			if (pivot > k) {
				return quickselect(G, first, pivot - 1, k);
			}
			return quickselect(G, pivot + 1, last, k);
		}
		return Double.MIN_VALUE;
	}

	private static int partition(ArrayList<Double> G, int first, int last) {
		int pivot = first + new Random().nextInt(last - first + 1);
		swap(G, last, pivot);
		for (int i = first; i < last; i++) {
			if (G.get(i) > G.get(last)) {
				swap(G, i, first);
				first++;
			}
		}
		swap(G, first, last);
		return first;
	}

	private static void swap(ArrayList<Double> G, int x, int y) {
		double tmp = G.get(x);
		G.set(x, G.get(y));
		G.set(y, tmp);
	}
}

class NeighborNodeList {
	ArrayList<Double> values = new ArrayList<Double>();
	double max;
}

class LHat implements Comparable<LHat> {
	String a;
	Double originalValue;
	public Double value;

	@Override
	public int compareTo(LHat other) {
		return Double.compare(other.value, this.value);
	}

}
