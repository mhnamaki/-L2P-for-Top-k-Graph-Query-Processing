package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

public class ManualQuickTester {

	public static void main(String[] args) throws Exception {

//		Object false1 = new Object();
//		false1 = "false";
//		Object true1 = new Object();
//		true1 = "true";
//
//		System.out.println(Float.parseFloat(false1.toString()));
//		System.out.println(Float.parseFloat(true1.toString()));

		// Date date = new Date("01-JUL-2004");
		// DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		// System.out.println(dateFormat.format(date));
	}

	//// File fout = new File("out.txt");
	//// FileOutputStream fos = new FileOutputStream(fout);
	////
	//// BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
	//// try {
	//// if (args.length < 2) {
	//// bw.write("error! args less than 2!");
	//// } else {
	//// double x = Double.parseDouble(args[0]);
	//// double x_2 = x * x;
	//// double x_3 = x_2 * x;
	//// double y = Double.parseDouble(args[1]);
	//// double y_2 = y * y;
	////
	//// Double result = (-2 * x + Math.pow((1 - x_2), 2)) * Math.exp(-1 / 3 *
	//// x_3 + x - y_2);
	//// bw.write(result.toString());
	//// }
	////
	//// } catch (Exception exc) {
	//// bw.write(exc.getMessage());
	//// bw.close();
	//// throw exc;
	//// }
	// bw.close();
	// }

}

class Test implements Cloneable {
	int i;
	int j;

	public Test(int i, int j) {
		this.i = i;
		this.j = j;
	}

	@Override
	public Test clone() {
		Test you_class = new Test(i, j);
		return you_class;
	}
}
