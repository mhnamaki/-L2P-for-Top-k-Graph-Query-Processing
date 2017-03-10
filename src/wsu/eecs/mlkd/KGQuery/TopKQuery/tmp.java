package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.math3.util.Combinations;
import org.jgrapht.experimental.permutation.PermutationFactory;

public class tmp {
	public static void generatePermutations(char[] arr, int size, char[] branch, int level, boolean[] visited)
	{
	    if (level >= size-1)
	    {
	        System.out.println(branch);
	        return;
	    }
	    
	    for (int i = 0; i < size; i++)
	    {
	        if (!visited[i])
	        {
	            branch[++level] = arr[i];
	            visited[i] = true;
	            generatePermutations(arr, size, branch, level, visited);
	            visited[i] = false;
	            level--;
	        }
	    }
	}
	public static void main(String[] args) {
		String str = "ABCD";
		int n = str.length();
		char[] arr = str.toCharArray();
		boolean[] visited = new boolean[n];
		for (int i = 0; i < n; i++)
		    visited[i] = false;
		char[] branch = new char[n];
		generatePermutations(arr, n, branch, -1, visited);
    }

}
