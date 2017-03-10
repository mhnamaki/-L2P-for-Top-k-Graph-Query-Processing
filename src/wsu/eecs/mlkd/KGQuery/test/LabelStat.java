package wsu.eecs.mlkd.KGQuery.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

public class LabelStat {
	public String dbPath;
	private long labelCount;
	private Map <String, Integer> labelMap;
	private GraphDatabaseService knowledgeGraph ;
	
	public LabelStat (String dbPath) {
		this.dbPath = dbPath;
		if (!dbPath.isEmpty()) {
			this.knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(this.dbPath);
			registerShutdownHook(this.knowledgeGraph);
		}
		this.labelMap = new HashMap<String, Integer>();
	}
	
	public LabelStat (GraphDatabaseService db) {
		
		this.knowledgeGraph = db;
		this.labelMap = new HashMap<String, Integer>();
	}
	public long getLabelCount() {
		return labelCount;
	}
	public Map<String, Integer> getLabelMap(){
		return labelMap;
	} 
	
	public void ConstructLabelMap() {
		try (Transaction tx = knowledgeGraph.beginTx()) {
			ResourceIterable<Node> allNodes = GlobalGraphOperations.at(knowledgeGraph).getAllNodes();
			
			for (Node node : allNodes) {
				boolean gotLabels = false;
				Iterable<Label> nodeLabel = node.getLabels();
				Iterator<Label> labelIterator = nodeLabel.iterator();
				while (labelIterator.hasNext()) {					
					Label label = labelIterator.next();
					if (!label.name().isEmpty() && !label.name().contains("http://") && label.name().length() >= 4){
						gotLabels = true;
						if (labelMap.containsKey(label.name())){
							int count = labelMap.get(label.name());
							labelMap.put(label.name(), ++count);
							//System.out.println("changed " + label.name() + " " + count);
						}
						else {
							labelMap.put(label.name(), 1);
						}
					}
				}
				if (!gotLabels) {
					Iterable<String> nodeProperties = node.getPropertyKeys();
					Iterator<String> propertyIterator = nodeProperties.iterator();
					while (propertyIterator.hasNext()) {
						String nextKey = propertyIterator.next();
						String nextProp = node.getProperty(nextKey).toString().replace("\n", "");
						if (!nextProp.isEmpty() && !nextProp.contains("http://") && nextProp.length() >= 4){
							if (labelMap.containsKey(nextProp)){
								int count = labelMap.get(nextProp);
								labelMap.put(nextProp, ++count);
								//System.out.println("changed " + nextProp + " " + count);
							}
							else {
								labelMap.put(nextProp, 1);
							}
						}
					}
				}
			}
			System.out.println(labelMap.size());
			Set<Entry<String, Integer>> labelSet = labelMap.entrySet();
			//do some stat on labels
			int maxCount = -1;
			int minCount = 10000;
			int totalCount = 0;
			for (Entry <String, Integer> entry : labelSet) {
				int countForLabel = entry.getValue();
				if (countForLabel < minCount) minCount = countForLabel;
				else if (countForLabel > maxCount) maxCount = countForLabel;
				totalCount += countForLabel;
			}
			System.out.println("Max Count: " + maxCount + " Min Count: " + minCount + " Avg. Count: " + totalCount/labelSet.size());
			
			Map <Integer, List <String>> labelsOnCount = new TreeMap<Integer, List<String>>();
			for (Entry<String, Integer> item : labelSet) {
				if (labelsOnCount.containsKey(item.getValue())) {
					labelsOnCount.get(item.getValue()).add(item.getKey());
				}
				else {
					List <String> list = new ArrayList<>();
					list.add(String.valueOf(item.getKey()));
					labelsOnCount.put(item.getValue(), list);
				}
				
			}
			Set<Entry<Integer, List<String>> > labelsOnCountSet = labelsOnCount.entrySet();
			
			for (Entry<Integer, List<String>> entry : labelsOnCountSet) {
				System.out.println(entry.getKey() + " " + entry.getValue().size());
			}
			
			System.out.println("labels on size: " + labelsOnCount.size());

			tx.success();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	
	public static void main(String[] args) {
		LabelStat labelStat = new LabelStat("/Users/mislam1/Documents/courses/BigData/Data Sets/dbpedia_infobox_properties_en.db");
		labelStat.ConstructLabelMap();		
		labelStat.knowledgeGraph.shutdown();
	}
	
	

}
