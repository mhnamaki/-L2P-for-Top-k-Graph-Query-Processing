package wsu.eecs.mlkd.KGQuery.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.traversal.BranchOrderingPolicies;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.tooling.GlobalGraphOperations;



public class DatasetStatisticGrabberForML {

	// getting a dataset path and printing the ....
	public static void main(String[] args) throws IOException {
		String dataGraphPath = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.3.1/data/yago.db4";
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-dataGraph")) {
				dataGraphPath = args[++i];
			}
		}

		File storeDir = new File(dataGraphPath);
		GraphDatabaseService dataGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(storeDir)
				.setConfig(GraphDatabaseSettings.pagecache_memory, "6g")
				.setConfig(GraphDatabaseSettings.allow_store_upgrade, "true")
				.newGraphDatabase();

		System.out.println("dataset: " + dataGraphPath);

		File readme = new File(dataGraphPath + "/README.txt");
		FileWriter readmeWriter = new FileWriter(readme);

		// Transaction tx2 = dataGraph.beginTx();
		//
		// HashMap<Long, HashMap<Long, Integer>> freq = new HashMap<Long,
		// HashMap<Long, Integer>>();
		//
		// for (Relationship rel : dataGraph.getAllRelationships()) {
		// freq.putIfAbsent(rel.getStartNode().getId(), new HashMap<Long,
		// Integer>());
		// freq.get(rel.getStartNode().getId()).putIfAbsent(rel.getEndNode().getId(),
		// 0);
		// freq.get(rel.getStartNode().getId()).put(rel.getEndNode().getId(),
		// freq.get(rel.getStartNode().getId()).get(rel.getEndNode().getId()) +
		// 1);
		// }
		//
		// for (Relationship rel : dataGraph.getAllRelationships()) {
		// if
		// (freq.get(rel.getStartNode().getId()).get(rel.getEndNode().getId()) >
		// 1) {
		// readmeWriter.write(rel.getStartNode().getId() + "; " +
		// rel.getEndNode().getId() + "\n");
		// }
		// }
		//
		// tx2.success();
		// tx2.close();

		// number of RelatinshipType
		try (Transaction tx1 = dataGraph.beginTx()) {
			int allNodesCnt = 0;

			HashMap<String, Integer> relTypeFreq = new HashMap<>();

			for (Relationship rel : GlobalGraphOperations.at(dataGraph).getAllRelationships()) {

				if (relTypeFreq.containsKey(rel.getType().name())) {
					relTypeFreq.put(rel.getType().name(), relTypeFreq.get(rel.getType().name()) + 1);
				} else {
					relTypeFreq.put(rel.getType().name(), 1);
				}
			}

			readmeWriter.write("Distinct relationship types: " + relTypeFreq.keySet().size() + "\n");

			for (String s : relTypeFreq.keySet()) {
				readmeWriter.write(s + " : " + relTypeFreq.get(s) + "\n");
			}

			readmeWriter.write("number of nodes: " + allNodesCnt + "\n");

			tx1.success();
		} catch (Exception e) {
			// TODO: handle exception
		}

		// number of nodes
		try (Transaction tx1 = dataGraph.beginTx()) {
			int allNodesCnt = 0;
			for (Node node : dataGraph.getAllNodes()) {
				allNodesCnt++;
			}

			readmeWriter.write("number of nodes: " + allNodesCnt + "\n");

			tx1.success();
		} catch (Exception e) {
			// TODO: handle exception
		}

		// number of relationships
		try (Transaction tx1 = dataGraph.beginTx()) {
			int allEdgesCnt = 0;
			for (Relationship edge : GlobalGraphOperations.at(dataGraph).getAllRelationships()) {
				allEdgesCnt++;
			}

			readmeWriter.write("number of relationships: " + allEdgesCnt + "\n");
			readmeWriter.flush();
			tx1.success();
		} catch (Exception e) {
			// TODO: handle exception
		}

		// max degree
		try (Transaction tx1 = dataGraph.beginTx()) {
			int maxDegree = 0;
			for (Node node : dataGraph.getAllNodes()) {
				if (node.getDegree() > maxDegree) {
					maxDegree = node.getDegree();
				}
			}

			readmeWriter.write("max degree: " + maxDegree + "\n");

			tx1.success();
		} catch (Exception e) {
			// TODO: handle exception
		}

		// avg degree
		try (Transaction tx1 = dataGraph.beginTx()) {
			int sumDegree = 0;
			int allNodesCnt = 0;
			for (Node node : dataGraph.getAllNodes()) {
				sumDegree += node.getDegree();
				allNodesCnt++;
			}

			readmeWriter.write("avg degree: " + (sumDegree / allNodesCnt) + "\n");
			readmeWriter.flush();
			tx1.success();
		} catch (Exception e) {
			// TODO: handle exception
		}

		// degree histogram
		// try (Transaction tx1 = dataGraph.beginTx()) {
		// TreeMap<Integer, Integer> degreeNodeCountMap = new TreeMap<Integer,
		// Integer>();
		//
		// for (Node node : dataGraph.getAllNodes()) {
		// if (degreeNodeCountMap.containsKey(node.getDegree())) {
		// degreeNodeCountMap.put(node.getDegree(),
		// degreeNodeCountMap.get(node.getDegree()) + 1);
		// } else {
		// degreeNodeCountMap.put(node.getDegree(), 1);
		// }
		// }
		//
		// readmeWriter.write("degree histogram" + "\n");
		// for (Integer degree : degreeNodeCountMap.navigableKeySet()) {
		// readmeWriter.write(degree + " : " + degreeNodeCountMap.get(degree) +
		// "\n");
		// }
		//
		// tx1.success();
		// } catch (Exception e) {
		// // TODO: handle exception
		// }

		// all distinct labels into a file alphabetically ordered and their
		// frequency
		try (Transaction tx1 = dataGraph.beginTx()) {
			TreeMap<String, Integer> distinctLabelsMap = new TreeMap<String, Integer>();
			for (Node node : dataGraph.getAllNodes()) {
				for (Label label : node.getLabels()) {
					if (distinctLabelsMap.containsKey(label.toString())) {
						distinctLabelsMap.put(label.toString(), distinctLabelsMap.get(label.toString()) + 1);
					} else {
						distinctLabelsMap.put(label.toString(), 1);
					}
				}
			}

			readmeWriter.write("the number of distinct labels: " + distinctLabelsMap.size() + "\n");
			for (String label : distinctLabelsMap.navigableKeySet()) {
				readmeWriter.write(label + " : " + distinctLabelsMap.get(label) + "\n");
			}
			readmeWriter.flush();
			tx1.success();
		} catch (Exception e) {
			// TODO: handle exception
		}

		// all distinct property keys and their frequency in alphabetical order
		try (Transaction tx1 = dataGraph.beginTx()) {
			TreeMap<String, Integer> distinctPropertiesKeysMap = new TreeMap<String, Integer>();
			for (Node node : dataGraph.getAllNodes()) {
				Map<String, Object> propertyMap = node.getAllProperties();
				for (String key : propertyMap.keySet()) {
					if (distinctPropertiesKeysMap.containsKey(key)) {
						distinctPropertiesKeysMap.put(key, distinctPropertiesKeysMap.get(key) + 1);
					} else {
						distinctPropertiesKeysMap.put(key, 1);
					}
				}
			}

			readmeWriter.write("the number of distinct property keys: " + distinctPropertiesKeysMap.size() + "\n");
			for (String key : distinctPropertiesKeysMap.navigableKeySet()) {
				readmeWriter.write(key + " : " + distinctPropertiesKeysMap.get(key) + "\n");
			}
			readmeWriter.flush();
			tx1.success();
		} catch (Exception e) {
			// TODO: handle exception
		}

		// node's labels frequency
		try (Transaction tx1 = dataGraph.beginTx()) {
			TreeMap<Integer, Integer> nodeNumberOfLabelsMap = new TreeMap<Integer, Integer>();
			for (Node node : dataGraph.getAllNodes()) {
				HashSet<String> distinctLabels = new HashSet<String>();
				for (Label label : node.getLabels()) {
					distinctLabels.add(label.toString());
				}
				if (nodeNumberOfLabelsMap.containsKey(distinctLabels.size())) {
					nodeNumberOfLabelsMap.put(distinctLabels.size(),
							nodeNumberOfLabelsMap.get(distinctLabels.size()) + 1);
				} else {
					nodeNumberOfLabelsMap.put(distinctLabels.size(), 1);
				}

			}

			for (Integer key : nodeNumberOfLabelsMap.navigableKeySet()) {
				readmeWriter.write(key + " : " + nodeNumberOfLabelsMap.get(key) + "\n");
			}
			readmeWriter.flush();
			tx1.success();
		} catch (Exception e) {
			// TODO: handle exception
		}

		// distinct date of the nodes
		// try (Transaction tx1 = dataGraph.beginTx()) {
		// DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
		//
		// TreeMap<Date, Integer> dateOfTheIncNodesMap = new TreeMap<Date,
		// Integer>();
		// TreeMap<Date, Integer> dateOfTheInactivNodesMap = new TreeMap<Date,
		// Integer>();
		// TreeMap<Date, Integer> dateOfTheDormNodesMap = new TreeMap<Date,
		// Integer>();
		// TreeMap<Date, Integer> dateOfTheStruckNodesMap = new TreeMap<Date,
		// Integer>();
		//
		// for (Node node : dataGraph.getAllNodes()) {
		// Map<String, Object> props = node.getAllProperties();
		// for (String key : props.keySet()) {
		// if (key.toLowerCase().contains("incorporation_date")) {
		//
		// Date tempDate = new Date(props.get(key).toString());
		// if (dateOfTheIncNodesMap.containsKey(tempDate)) {
		// dateOfTheIncNodesMap.put(tempDate, dateOfTheIncNodesMap.get(tempDate)
		// + 1);
		// } else {
		// dateOfTheIncNodesMap.put(tempDate, 1);
		// }
		//
		// } else if (key.toLowerCase().contains("inactivation_date")) {
		// Date tempDate = new Date(props.get(key).toString());
		// if (dateOfTheInactivNodesMap.containsKey(tempDate)) {
		// dateOfTheInactivNodesMap.put(tempDate,
		// dateOfTheInactivNodesMap.get(tempDate) + 1);
		// } else {
		// dateOfTheInactivNodesMap.put(tempDate, 1);
		// }
		//
		// } else if (key.toLowerCase().contains("dorm_date")) {
		// Date tempDate = new Date(props.get(key).toString());
		// if (dateOfTheDormNodesMap.containsKey(tempDate)) {
		// dateOfTheDormNodesMap.put(tempDate,
		// dateOfTheDormNodesMap.get(tempDate) + 1);
		// } else {
		// dateOfTheDormNodesMap.put(tempDate, 1);
		// }
		// } else if (key.toLowerCase().contains("struck_off_date")) {
		// Date tempDate = new Date(props.get(key).toString());
		// if (dateOfTheStruckNodesMap.containsKey(tempDate)) {
		// dateOfTheStruckNodesMap.put(tempDate,
		// dateOfTheStruckNodesMap.get(tempDate) + 1);
		// } else {
		// dateOfTheStruckNodesMap.put(tempDate, 1);
		// }
		// }
		// }
		// }
		//
		// readmeWriter.write("incorporation_date" + "\n");
		// for (Date key : dateOfTheIncNodesMap.navigableKeySet()) {
		// readmeWriter.write(dateFormat.format(key) + " : " +
		// dateOfTheIncNodesMap.get(key));
		// }
		// readmeWriter.write("\n");
		//
		// readmeWriter.write("inactivation_date");
		// for (Date key : dateOfTheInactivNodesMap.navigableKeySet()) {
		// readmeWriter.write(dateFormat.format(key) + " : " +
		// dateOfTheInactivNodesMap.get(key));
		// }
		// readmeWriter.write("\n");
		//
		// readmeWriter.write("dorm_date");
		// for (Date key : dateOfTheDormNodesMap.navigableKeySet()) {
		// readmeWriter.write(dateFormat.format(key) + " : " +
		// dateOfTheDormNodesMap.get(key));
		// }
		// readmeWriter.write("\n");
		//
		// readmeWriter.write("struck_off_date");
		// for (Date key : dateOfTheStruckNodesMap.navigableKeySet()) {
		// readmeWriter.write(dateFormat.format(key) + " : " +
		// dateOfTheStruckNodesMap.get(key));
		// }
		// readmeWriter.write("\n");
		//
		// tx1.success();
		// } catch (Exception e) {
		// // TODO: handle exception
		// }

		// distinct property values for all of the nodes
		// readmeWriter.write();
		// try (Transaction tx1 = dataGraph.beginTx()) {
		// HashMap<String, HashSet<String>> propKeyMap = new HashMap<String,
		// HashSet<String>>();
		// for (Node node : dataGraph.getAllNodes()) {
		// Map<String, Object> props = node.getAllProperties();
		// for (String key : props.keySet()) {
		// HashSet<String> values;
		// if (propKeyMap.containsKey(key)) {
		// values = propKeyMap.get(key);
		// } else {
		// values = new HashSet<String>();
		// }
		// values.add(node.getProperty(key).toString());
		// propKeyMap.put(key, values);
		// }
		//
		// }
		//
		// for (String key : propKeyMap.keySet()) {
		// readmeWriter.write("property key:" + key);
		// readmeWriter.write("size:" + propKeyMap.get(key).size());
		// for (String value : propKeyMap.get(key)) {
		// readmeWriter.write(value + ", ");
		// }
		// readmeWriter.write();
		// readmeWriter.write();
		// }
		//
		// tx1.success();
		// } catch (Exception e) {
		// // TODO: handle exception
		// }

		// Get the statistics of the distribution of papers for each year
		// HashMap<String, Long> yearDistribution = new HashMap<>();
		// try (Transaction tx1 = dataGraph.beginTx()) {
		// for (Node n : dataGraph.getAllNodes()) {
		// if (n.hasLabel(Label.label("Paper"))) {
		// String year = n.getProperty("Year").toString();
		//
		// if (yearDistribution.containsKey(year)) {
		// yearDistribution.put(year, yearDistribution.get(year) + 1);
		// } else {
		// yearDistribution.put(year, 1l);
		// }
		// }
		// }
		// tx1.success();
		// readmeWriter.write("YEAR DISTRIBUTION: " + "\n");
		//
		// for (String str : yearDistribution.keySet()) {
		// readmeWriter.write(str + ":" + yearDistribution.get(str) + "\n");
		// }
		// } catch (Exception e) {
		// e.printStackTrace();
		// }

		// connected components and what is the maximum connected component?:
		// ? SHAYAN

		// try (Transaction tx1 = dataGraph.beginTx()) {
		//
		// readmeWriter.write("\n\n*Connected Components: " + "\n");
		//
		// // Try the built-in traverse
		// TraversalDescription graphTraverse = dataGraph.traversalDescription()
		// .order(BranchOrderingPolicies.POSTORDER_DEPTH_FIRST).uniqueness(Uniqueness.NODE_GLOBAL)
		// .evaluator(Evaluators.all());
		//
		// // To keep the frequency of each size
		//
		// TreeMap<Integer, Integer> componentSizeFrequency = new
		// TreeMap<Integer, Integer>();
		//
		// // Add every node to an ArrayList
		// int connectedComp = 0;
		// int connectedSize = 0;
		// int maxConnectedComp = 0;
		// Node currentNode;
		// ArrayList<Node> nodeList = new ArrayList<>();
		// for (Node n : dataGraph.getAllNodes()) {
		// nodeList.add(n);
		// }
		//
		// while (nodeList.size() != 0) {
		// currentNode = nodeList.get(0);
		// nodeList.remove(0);
		//
		// for (Node n : graphTraverse.traverse(currentNode).nodes()) {
		// connectedSize++;
		// nodeList.remove(n);
		// // readmeWriter.write("Please wait... Number of remaining
		// // nodes to visit: " + nodeList.size());
		// // System .out.print(connectedSize + " | ");
		// }
		//
		// if (componentSizeFrequency.containsKey(connectedSize)) {
		// componentSizeFrequency.put(connectedSize,
		// componentSizeFrequency.get(connectedSize) + 1);
		// } else {
		// componentSizeFrequency.put(connectedSize, 1);
		// }
		//
		// if (connectedSize > maxConnectedComp)
		// maxConnectedComp = connectedSize;
		//
		// connectedComp++;
		// connectedSize = 0;
		//
		// readmeWriter.write("\n");
		// readmeWriter.write("connectedComp = " + connectedComp + "\n");
		// }
		//
		// readmeWriter.write("FREQUENCIES OF CONNECTED COMPONENT SIZE: " +
		// "\n");
		//
		// readmeWriter.write("END OF FREQUENCIES OF CONNECTED COMPONENT SIZE" +
		// "\n");
		//
		// readmeWriter.write("Number of connected components: " + connectedComp
		// + "\n");
		// readmeWriter.write("Size of the largest connected component: " +
		// maxConnectedComp + "\n");
		//
		// for (Integer key : componentSizeFrequency.navigableKeySet()) {
		// readmeWriter.write(key + " : " + componentSizeFrequency.get(key) +
		// "\n");
		// }
		//
		// readmeWriter.write("\n#End of Connected Components.");
		//
		// tx1.success();
		// } catch (Exception e) {
		// // Handle exception
		// }
		// readmeWriter.flush();
		// readmeWriter.close();
	}

}
