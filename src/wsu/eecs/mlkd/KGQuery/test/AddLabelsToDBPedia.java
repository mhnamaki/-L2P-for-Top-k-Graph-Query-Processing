package wsu.eecs.mlkd.KGQuery.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;

public class AddLabelsToDBPedia {
	public static final String ITEM_SEP = ";";
	
	public void ChangeLabels (String prefix, String directory, String fileNamePattern) {
		File folder = new File(directory);
		File[] listOfFiles = folder.listFiles();

		    for (int i = 0; i < listOfFiles.length; i++) {
		      if (listOfFiles[i].isFile() && listOfFiles[i].getName().contains(fileNamePattern)) {
		        System.out.println(listOfFiles[i].getName());
		        File modifiedFile = new File("changed" + listOfFiles[i].getName());
		        try {
					modifiedFile.createNewFile();
					BufferedReader brOriginalFile =  new BufferedReader(new FileReader(listOfFiles[i].getName()));
					BufferedWriter bwModifiedFile =  new BufferedWriter(new FileWriter(modifiedFile));
					String line = "";
					while ((line = brOriginalFile.readLine()) != null) {
						int nodeNumber = -1;
						if (!line.contains(ITEM_SEP)) {
							nodeNumber = Integer.parseInt(line);
							bwModifiedFile.write(line+ "\n");
						}
						for (int lineNumber = 0; lineNumber < nodeNumber; ++lineNumber) {
							line = brOriginalFile.readLine();
							String[] splittedString = line.split(ITEM_SEP);
							int labelsCount = splittedString.length;
							for (int splittedIndex = 0; splittedIndex < labelsCount ; ++splittedIndex) {
								if (splittedIndex == 0) { 
									bwModifiedFile.write(splittedString[splittedIndex]);
								}
								else if (splittedString[splittedIndex].startsWith("dbp_uri_")) {
									bwModifiedFile.write(";" + splittedString[splittedIndex]);
									continue;
								}
								else {
									bwModifiedFile.write(";dbp_lbl_" + splittedString[splittedIndex]);
									continue;
								}
							}
							bwModifiedFile.write("\n");
						}						
						for (int lineNumber = 0; lineNumber < nodeNumber; ++lineNumber) {
							line = brOriginalFile.readLine();
							bwModifiedFile.write(line + "\n");
						}
					}
					brOriginalFile.close();
					bwModifiedFile.close();
					System.out.println("finsihed and closed file " + modifiedFile.getName());
				} catch (IOException e) {
					e.printStackTrace();
				}
		        
		      } 
		    }
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		AddLabelsToDBPedia addLabelsToDBPedia =  new AddLabelsToDBPedia();
		addLabelsToDBPedia.ChangeLabels("dummy", ".", "freqGenQueries");

	}

}
