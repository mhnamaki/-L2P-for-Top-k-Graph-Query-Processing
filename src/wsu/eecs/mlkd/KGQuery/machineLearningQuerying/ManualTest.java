package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import weka.classifiers.CostMatrix;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Loads the cost matrix "args[0]" and prints its content to the console.
 *
 * @author FracPete (fracpete at waikato dot ac dot nz)
 */
public class ManualTest {

	public static void main(String[] args) throws Exception {

		// CostMatrix matrix = new CostMatrix(new BufferedReader(new
		// FileReader("costMatrixBoth.txt")));
		// System.out.println(matrix);

		HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
		map.put(0, 0);

		if (map.get(1) == null) {
			System.out.println("null");
		}

	}
}
