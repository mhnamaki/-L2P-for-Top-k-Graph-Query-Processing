package wsu.eecs.mlkd.KGQuery.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class TestMain {

	static <K, V extends Comparable<? super V>> SortedSet<Map.Entry<K, V>> entriesSortedByValues(Map<K, V> map) {
		SortedSet<Map.Entry<K, V>> sortedEntries = new TreeSet<Map.Entry<K, V>>(new Comparator<Map.Entry<K, V>>() {
			@Override
			public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {
				int res = e1.getValue().compareTo(e2.getValue());
				return res != 0 ? res : 1;
			}
		});
		sortedEntries.addAll(map.entrySet());
		return sortedEntries;
	}

	public static void main(String[] args) throws Exception {
		TreeMap<Integer, Integer> degreeMap = new TreeMap<Integer, Integer>();
		for (int i = 0; i < 8; i++) {
			FileInputStream fis = new FileInputStream("nodeAndTheirDegrees" + i + ".txt");

			// Construct BufferedReader from InputStreamReader
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));

			String line = null;

			while ((line = br.readLine()) != null) {
				String[] splitted = line.split(";");
				degreeMap.put(Integer.parseInt(splitted[0]), Integer.parseInt(splitted[1]));
			}

			br.close();
		}

		System.out.println("degreeMap size: " + degreeMap.size());

		File fout = new File("nodeAndTheirDegreesDesending.txt");
		FileOutputStream fos = new FileOutputStream(fout);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		// NavigableSet<Integer> navigateSet = degreeMap.descendingKeySet();
		SortedSet<Map.Entry<Integer, Integer>> sortedSet = entriesSortedByValues(degreeMap);
		System.out.println("after sort");
		Iterator<Map.Entry<Integer, Integer>> iterator = sortedSet.iterator();
		int cnt = 0;
		while (iterator.hasNext()) {
			Map.Entry<Integer, Integer> map = iterator.next();
			cnt++;
			int id = map.getKey();
			bw.write(id + ";" + degreeMap.get(id));
			bw.newLine();

			if ((cnt % 1000000) == 0) {
				bw.flush();
				System.out.println(cnt);
			}
		}
		bw.close();

	}
}
