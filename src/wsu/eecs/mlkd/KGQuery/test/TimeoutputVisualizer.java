package wsu.eecs.mlkd.KGQuery.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;

public class TimeoutputVisualizer {

	public static int kFrom = 0;
	public static int kTo = 0;
	public static int querySizeFrom = 0;
	public static int querySizeTo = 0;
	public static String timeFilePath = null;
	public static String gName = null;
	public static String algorithm = null;

	public static void main(String[] args) throws Exception {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-kFrom")) {
				kFrom = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-kTo")) {
				kTo = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-querySizeFrom")) {
				querySizeFrom = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-querySizeTo")) {
				querySizeTo = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-timeFilePath")) {
				timeFilePath = args[++i];
			} else if (args[i].equals("-gName")) {
				gName = args[++i];
			} else if (args[i].equals("-algorithm")) {
				algorithm = args[++i];
			}
		}

		if (timeFilePath == null) {
			throw new Exception("where parameters?!");
		}

		// create directory if doens't exist
		File parentDir = new File("timeCsvOutput");
		File fixedKDir = new File("timeCsvOutput/fixedKDir");
		File fixedQDir = new File("timeCsvOutput/fixedQDir");
		if (parentDir.exists()) {
			FileUtils.deleteDirectory(parentDir);
		}
		if (fixedKDir.exists()) {
			FileUtils.deleteDirectory(fixedKDir);
		}
		if (fixedQDir.exists()) {
			FileUtils.deleteDirectory(fixedQDir);
		}

		parentDir.mkdir();
		fixedQDir.mkdir();
		fixedKDir.mkdir();

		// Read timeoutput file
		ArrayList<TimeOutputLineItem> timeOutputLines = new ArrayList<TimeOutputLineItem>();
		File fin = new File(timeFilePath);
		FileInputStream fis = new FileInputStream(fin);

		// Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		while ((line = br.readLine()) != null) {
			String[] splittedLine = line.split(";");
			TimeOutputLineItem temp = new TimeOutputLineItem(Integer.parseInt(splittedLine[1]), splittedLine[2],
					splittedLine[3], Integer.parseInt(splittedLine[4]), splittedLine[5],
					Float.parseFloat(splittedLine[6]));

			timeOutputLines.add(temp);
		}

		br.close();

		System.out.println("timeOutputLines.size(): " + timeOutputLines.size());

		// fixed k
		File foutK = new File(fixedKDir + "/timeForExcel_alg_" + algorithm + "_gname_" + gName + "_qSizeFrom_"
				+ querySizeFrom + "_qSizeTo_" + querySizeTo + ".csv");
		FileOutputStream fosK = new FileOutputStream(foutK);

		BufferedWriter bwK = new BufferedWriter(new OutputStreamWriter(fosK));
		for (int k = kFrom; k <= kTo; k++) {

			float[] sumTimeInDiffQs = new float[querySizeTo - querySizeFrom + 1];
			int[] numberOfTimeInDiffQs = new int[querySizeTo - querySizeFrom + 1];

			for (TimeOutputLineItem timeOutputLine : timeOutputLines) {
				if (timeOutputLine.k == k) {
					sumTimeInDiffQs[timeOutputLine.querySize - querySizeFrom] += timeOutputLine.time;
					numberOfTimeInDiffQs[timeOutputLine.querySize - querySizeFrom]++;
				}
			}

			for (int index = 0; index < sumTimeInDiffQs.length; index++) {
				if (numberOfTimeInDiffQs[index] > 0) {
					bwK.write(k + ",");
					bwK.write((index + querySizeFrom) + ",");
					bwK.write((sumTimeInDiffQs[index] / numberOfTimeInDiffQs[index]) + ",");
					bwK.newLine();
				} else {
					System.out.println("there is no result for q: " + (index + querySizeFrom));
				}
			}
			bwK.newLine();

		}
		bwK.close();

		// fixed qSize
		File foutQ = new File(fixedQDir + "/timeForExcel_alg_" + algorithm + "_gname_" + gName + "_kFrom_" + kFrom
				+ "_kTo_" + kTo + ".csv");
		FileOutputStream fosQ = new FileOutputStream(foutQ);

		BufferedWriter bwQ = new BufferedWriter(new OutputStreamWriter(fosQ));
		for (int q = querySizeFrom; q <= querySizeTo; q++) {

			float[] sumTimeInDiffKs = new float[kTo - kFrom + 1];
			int[] numberOfTimeInDiffKs = new int[kTo - kFrom + 1];

			for (TimeOutputLineItem timeOutputLine : timeOutputLines) {
				if (timeOutputLine.querySize == q) {
					sumTimeInDiffKs[timeOutputLine.k - kFrom] += timeOutputLine.time;
					numberOfTimeInDiffKs[timeOutputLine.k - kFrom]++;
				}
			}

			for (int index = 0; index < sumTimeInDiffKs.length; index++) {
				if (numberOfTimeInDiffKs[index] > 0) {
					bwQ.write((index + kFrom) + ",");
					bwQ.write((sumTimeInDiffKs[index] / numberOfTimeInDiffKs[index]) + ",");
					bwQ.newLine();
				} else {
					System.out.println("there is no result for k: " + (index + kFrom));
				}
			}
			bwQ.newLine();
		}
		bwQ.close();

	}

}

class TimeOutputLineItem {
	public Integer querySize;
	public Integer k;
	public String gName;
	public String algorithm;
	public Float time;
	public String finalResultSize;

	public TimeOutputLineItem(Integer querySize, String gName, String finalResultSize, Integer k, String algorithm,
			Float time) {
		this.algorithm = algorithm;
		this.querySize = querySize;
		this.k = k;
		this.gName = gName;
		this.time = time;
		this.finalResultSize = finalResultSize;

	}

}
