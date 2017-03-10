package wsu.eecs.mlkd.KGQuery.test;

public class DameDasti {

	public static void generate(int[][] sets) {
	    int solutions = 1;
	    for(int i = 0; i < sets.length; solutions *= sets[i].length, i++);
	    for(int i = 0; i < solutions; i++) {
	        int j = 1;
	        for(int[] set : sets) {
	            System.out.print(set[(i/j)%set.length] + " ");
	            j *= set.length;
	        }
	        System.out.println();
	    }
	}

	public static void main(String[] args) {
	    generate(new int[][]{{1,2,3}, {3,2}, {5,6,7}});
	}

}
