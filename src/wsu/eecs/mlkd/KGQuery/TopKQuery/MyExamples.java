package wsu.eecs.mlkd.KGQuery.TopKQuery;

import java.util.Random;

public class MyExamples {

//	Find the kth biggest element, using selection algorithm. 
//	Next, iterate the array and find all elements which are larger/equal it.
//
//	complexity: O(n) for selection and O(n) for iterating, so the total is also O(n)
	public static void main(String[] args) {
		Integer[] arr = new Integer[10];
		for (int i = 0; i < arr.length; i++) {
			arr[i]  = (int) Math.ceil(Math.random() * 100);
			System.out.println(i+ ", "+arr[i]);
		}
		//System.out.println("k elem:" + selectKth(arr,4));
		System.out.print("k elem:" + select(arr, 4));
	}

	private static <E extends Comparable<? super E>> int partition(E[] arr, int left, int right, int pivot) {
		E pivotVal = arr[pivot];
		swap(arr, pivot, right);
		int storeIndex = left;
		for (int i = left; i < right; i++) {
			if (arr[i].compareTo(pivotVal) < 0) {
				swap(arr, i, storeIndex);
				storeIndex++;
			}
		}
		swap(arr, right, storeIndex);
		return storeIndex;
	}
 
	private static <E extends Comparable<? super E>> E select(E[] arr, int n) {
		int left = 0;
		int right = arr.length - 1;
		Random rand = new Random();
		while (right >= left) {
			int pivotIndex = partition(arr, left, right, rand.nextInt(right - left + 1) + left);
			if (pivotIndex == n) {
				return arr[pivotIndex];
			} else if (pivotIndex < n) {
				left = pivotIndex + 1;
			} else {
				right = pivotIndex - 1;
			}
		}
		return null;
	}
 
	private static void swap(Object[] arr, int i1, int i2) {
		if (i1 != i2) {
			Object temp = arr[i1];
			arr[i1] = arr[i2];
			arr[i2] = temp;
		}
	}
}
