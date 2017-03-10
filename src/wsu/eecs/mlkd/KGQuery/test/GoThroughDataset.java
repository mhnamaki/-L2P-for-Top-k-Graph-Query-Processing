package wsu.eecs.mlkd.KGQuery.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.Collect;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.tooling.GlobalGraphOperations;

import java_cup.sym;
import jdsl.core.ref.ComparableComparator;
import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;
import wsu.eecs.mlkd.KGQuery.TopKQuery.PreProcessingLabels;

public class GoThroughDataset {

	// public static String dbPath =
	// "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.3.1/data/dbpedia_infobox_properties_en.db";
	// public static String dbPath =
	// "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.3.1/data/dbpedia_old.db";
	public static String dbPath = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.3.1/data/dbpedia.db";
	public static int numberOfPrefixChars = 4;
	public static GraphDatabaseService knowledgeGraph;

	public static void main(String[] args) {
		File graph = new File(dbPath);
		knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(graph)

				// .setConfig(GraphDatabaseSettings.pagecache_memory, "8g")
				.setConfig(GraphDatabaseSettings.allow_store_upgrade, "true").newGraphDatabase();

		registerShutdownHook(knowledgeGraph);
		System.out.println("after knowledge graph initialization");
		// HashMap<String, HashSet<Long>> nodeLabelsIndex =
		// PreProcessingLabels.getPrefixLabelsIndex(knowledgeGraph,
		/// numberOfPrefixChars);
		// System.out.println("after PreProcessingLabels");
		// Levenshtein levenshtein = new Levenshtein(nodeLabelsIndex,
		// numberOfPrefixChars);
		HashSet<Long> ids = new HashSet<Long>();
		try (Transaction tx1 = knowledgeGraph.beginTx()) {

			for (Node jenNode : knowledgeGraph.getAllNodes()) {
				for (Label lbl : jenNode.getLabels()) {
					if (lbl.name().toString().toLowerCase().equals("band")) {
						ids.add(jenNode.getId());
					}
				}
			}

			System.out.println("ids.size(): " + ids.size());
			
			ids.clear();
			for (Node jenNode : knowledgeGraph.getAllNodes()) {
				for (Label lbl : jenNode.getLabels()) {
					if (lbl.name().toString().toLowerCase().contains("artist")) {
						ids.add(jenNode.getId());
					}
				}
			}

			System.out.println("ids.size(): " + ids.size());

			// HashSet<Long> allJenNodesWithMusicNeighbors = new
			// HashSet<Long>();
			// for (Node jenNode : knowledgeGraph.getAllNodes()) {
			// for (Label lbl : jenNode.getLabels()) {
			// if (lbl.toString().toLowerCase().contains("jennifer_lopez")) {
			// System.out.println("jenNode:" + jenNode.getId());
			// for (Relationship rel : jenNode.getRelationships()) {
			// Node other = rel.getOtherNode(jenNode);
			// for (Label lbl2 : other.getLabels()) {
			// if (lbl2.toString().toLowerCase().contains("band")
			// // ||
			// // lbl2.toString().toLowerCase().contains("song")
			// // ||
			// // lbl2.toString().toLowerCase().contains("television")
			// ) {
			// allJenNodesWithMusicNeighbors.add(jenNode.getId());
			// System.out.println(other.getId());
			// }
			// }
			// }
			// }
			// }

			// Map<String, Object> props = jenNode.getAllProperties();
			// for (String key : props.keySet()) {
			// if
			// (props.get(key).toString().toLowerCase().contains("jennifer_lopez"))
			// {
			// for (Relationship rel : jenNode.getRelationships()) {
			// Node other = rel.getOtherNode(jenNode);
			// for (Label lbl2 : other.getLabels()) {
			// if (lbl2.toString().toLowerCase().equals("band")
			// // ||
			// // lbl2.toString().toLowerCase().contains("song")
			// // ||
			// // lbl2.toString().toLowerCase().contains("television")
			// ) {
			// allJenNodesWithMusicNeighbors.add(jenNode.getId());
			// }
			// }
			// }
			// }
			// }

			// }

			// System.out.println(allJenNodesWithMusicNeighbors);

			// Node jen = knowledgeGraph.getNodeById(21363);
			//
			// HashSet<Long> allJenNeighbors = new HashSet<Long>();
			// HashMap<Long, HashSet<Long>> allArtistsNeighbors = new
			// HashMap<Long, HashSet<Long>>();
			//
			// HashSet<Long> artists = new HashSet<Long>();
			// for (Relationship rel : jen.getRelationships()) {
			// allJenNeighbors.add(rel.getOtherNode(jen).getId());
			// Node other = rel.getOtherNode(jen);
			// for (Label lbl : other.getLabels()) {
			// if (lbl.toString().toLowerCase().contains("artist")) {
			// artists.add(other.getId());
			// }
			// }
			// }
			//
			// for (Long artistId : artists) {
			// allArtistsNeighbors.put(artistId, new HashSet<Long>());
			// Node artistNode = knowledgeGraph.getNodeById(artistId);
			// for (Relationship rel : artistNode.getRelationships()) {
			// if (rel.getOtherNode(artistNode).getId() != 21363)
			// allArtistsNeighbors.get(artistId).add(rel.getOtherNode(artistNode).getId());
			// }
			// }
			// for (Long artistId : allArtistsNeighbors.keySet()) {
			// Set<Long> intersection = new
			// HashSet<Long>(allArtistsNeighbors.get(artistId));
			// intersection.retainAll(allJenNeighbors);
			// if (intersection.size() > 0) {
			// // System.out.println(intersection + "" + artistId + "," +
			// // 21363);
			// for (Long bandId : intersection) {
			// Node bandNode = knowledgeGraph.getNodeById(bandId);
			// for(Label lbl : bandNode.getLabels()){
			// if(lbl.name().toLowerCase().contains("band")){
			// System.out.println("[21363, " + artistId + ", " + bandId+ "]");
			// break;
			// }
			// }
			// }
			// }
			// }

			// for (Node node : knowledgeGraph.getAllNodes()) {
			// for (Label lbl : node.getLabels()) {
			// if (lbl.toString().toLowerCase().contains("jennifer_lo")) {
			// ids.add(node.getId());
			// }
			// }
			// Map<String, Object> props = node.getAllProperties();
			// for (String key : props.keySet()) {
			// if
			// (props.get(key).toString().toLowerCase().contains("jennifer_lo"))
			// {
			// ids.add(node.getId());
			// }
			// }
			// }
			//
			// for (Long id : ids) {
			// System.out.print(id + ", ");
			// }

			// System.out.println("lbls:");
			// for (Node node :
			// GlobalGraphOperations.at(knowledgeGraph).getAllNodes()) {
			// for (Label lbl : node.getLabels()) {
			// if (lbl.toString().toLowerCase().equals("country")) {
			// Map<String, Object> props = node.getAllProperties();
			//
			// long artistId = 0;
			// long albumId = 0;
			// long filmId = 0;
			// boolean knownPerson = false;
			//
			// // long countryId = 0;
			//
			// for (String key : props.keySet()) {
			// if
			// (props.get(key).toString().toLowerCase().contains("united_kingdom"))
			// {
			// // if
			// // (props.get(key).toString().toLowerCase().contains("eminem")
			// // ||
			// // props.get(key).toString().toLowerCase().contains("madonna")
			// // ||
			// // props.get(key).toString().toLowerCase().contains("brithney")
			// // ||
			// // props.get(key).toString().toLowerCase().contains("beyon")
			// // ||
			// // props.get(key).toString().toLowerCase().contains("sunny")
			// // ||
			// // props.get(key).toString().toLowerCase().contains("taylor"))
			// // {
			// knownPerson = true;
			// }
			// }
			// //
			// if (!knownPerson) {
			// break;
			// }
			// for (Relationship rel : node.getRelationships()) {
			// Node otherNode = rel.getOtherNode(node);
			// for (Label lblNeighbor : otherNode.getLabels()) {
			// if (lblNeighbor.toString().toLowerCase().equals("film")) {
			// filmId = otherNode.getId();
			// break;
			// }
			// if (lblNeighbor.toString().toLowerCase().equals("album")) {
			// albumId = otherNode.getId();
			// break;
			// }
			// // if
			// // (lblNeighbor.toString().toLowerCase().contains("person"))
			// // {
			// // artistId = otherNode.getId();
			// // break;
			// // }
			// // if
			// //
			// (lblNeighbor.toString().toLowerCase().contains("dbp_lbl_country"))
			// // {
			// // countryId = otherNode.getId();
			// // ;
			// // break;
			// // }
			//
			// }
			// // if (artistId > 0 && countryId > 0 && filmId > 0)
			// // {
			// // System.out.println(node.getId() + "; " +
			// // node.getProperty("__URI__"));
			// // System.out.println(
			// // artistId + "; " +
			// // knowledgeGraph.getNodeById(artistId).getProperty("__URI__"));
			// //
			// // System.out.println(countryId + "; "
			// // +
			// // knowledgeGraph.getNodeById(countryId).getProperty("__URI__"));
			// //
			// // System.out.println(
			// // filmId + "; " +
			// // knowledgeGraph.getNodeById(filmId).getProperty("__URI__"));
			// //
			// // System.out.println();
			// // }
			//
			// if (albumId > 0 && filmId > 0) {
			// System.out.println(node.getId() + "; " +
			// node.getProperty("__URI__"));
			// System.out.println(
			// albumId + "; " +
			// knowledgeGraph.getNodeById(albumId).getProperty("__URI__"));
			// System.out.println(
			// filmId + "; " +
			// knowledgeGraph.getNodeById(filmId).getProperty("__URI__"));
			// System.out.println();
			//
			// }
			// }
			//
			// }
			// }
		}

		// System.out.println("props:");
		// for (Node node :
		// GlobalGraphOperations.at(knowledgeGraph).getAllNodes()) {
		// Map<String, Object> props = node.getAllProperties();
		// for (String propKey : props.keySet()) {
		// if
		// (props.get(propKey).toString().toLowerCase().contains("jennifer"))
		// {
		// System.out.println(node.getId() + ", " + props.get(propKey));
		// }
		// }
		// }

		// Node node = knowledgeGraph.getNodeById(400);
		// for (Label lbl : node.getLabels()) {
		// System.out.println(lbl);
		// }
		// Map<String, Object> props = node.getAllProperties();
		// for (String key : props.keySet()) {
		// System.out.println(key + ";" + props.get(key));
		// }

		///////////////////////////////////////////////////////////////////////////////////
		///// check how many label are the same
		//////////////////////////////////////////////////////////////////////////////
		// TreeMap<String, Integer> sameLabels = new TreeMap<String,
		// Integer>();
		// int noLabelNodes = 0;
		// int numberOfLabels = 0;
		// int numberOfNodes = 0;
		// int differentLabel = 0;
		// int minLabelLength = Integer.MAX_VALUE;
		// int maxLabelLength = Integer.MIN_VALUE;
		//
		// for (Node node :
		// GlobalGraphOperations.at(knowledgeGraph).getAllNodes()) {
		// numberOfNodes++;
		// boolean noLabel = true;
		// for (Label label : node.getLabels()) {
		// numberOfLabels++;
		// if (label.toString().length() < minLabelLength) {
		// minLabelLength = label.toString().length();
		// }
		// if (label.toString().length() > maxLabelLength) {
		// maxLabelLength = label.toString().length();
		// }
		// noLabel = false;
		// if (sameLabels.containsKey(label.toString())) {
		// sameLabels.put(label.toString(), sameLabels.get(label.toString())
		// + 1);
		// } else {
		// sameLabels.put(label.toString(), 1);
		// differentLabel++;
		// }
		// }
		// if (noLabel) {
		// noLabelNodes++;
		// }
		// }
		//
		// System.out.println("no label nodes: " + noLabelNodes);
		// System.out.println("number of labels :" + numberOfLabels);
		// System.out.println("number of nodes :" + numberOfNodes);
		// System.out.println("number of different labels :" +
		// differentLabel);
		// System.out.println("min label length :" + minLabelLength);
		// System.out.println("max label length :" + maxLabelLength);
		// for (String key : sameLabels.keySet()) {
		// System.out.println(key + " ; " + sameLabels.get(key));
		// }

		// System.out.println(knowledgeGraph.getNodeById(12).getDegree());

		////////////////////////////////////////////////////////////////////////////////////////
		///// check nolabels what is their common attributes? why they don't
		//////////////////////////////////////////////////////////////////////////////////////// have
		//////////////////////////////////////////////////////////////////////////////////////// labels?
		//////////////////////////////////////////////////////////////////////////////////////// /////
		////////////////////////////////////////////////////////////////////////////////////////
		// TreeMap<String, Integer> sameProperties = new TreeMap<String,
		// Integer>();
		// for (Node node :
		// GlobalGraphOperations.at(knowledgeGraph).getAllNodes()) {
		// Iterable<Label> iterator = node.getLabels();
		// if (!iterator.iterator().hasNext()) {
		// for (String propertyKey : node.getPropertyKeys()) {
		// if (sameProperties.containsKey(propertyKey.toString())) {
		// sameProperties.put(propertyKey.toString(),
		// sameProperties.get(propertyKey.toString()) + 1);
		// } else {
		// sameProperties.put(propertyKey.toString(), 1);
		// }
		// }
		// }
		//
		// }
		//
		// for (String key : sameProperties.keySet()) {
		// System.out.println(key + " ; " + sameProperties.get(key));
		// }

		// property values.
		// ArrayList<String> props = new ArrayList<String>();
		// // TreeMap<String, Integer> samePropertyValues = new
		// TreeMap<String,
		// // Integer>();
		// for (Node node :
		// GlobalGraphOperations.at(knowledgeGraph).getAllNodes()) {
		// Iterable<Label> iterator = node.getLabels();
		// if (iterator.iterator().hasNext()) {
		// // for (String key : node.getPropertyKeys()) {
		// props.add(node.getProperty("__URI__").toString());
		// // props.insertSorted();
		// // if
		// //
		// (samePropertyValues.containsKey(node.getProperty(key).toString()))
		// // {
		// // samePropertyValues.put(node.getProperty(key).toString(),
		// // samePropertyValues.get(node.getProperty(key).toString())
		// // + 1);
		// // } else {
		// // samePropertyValues.put(node.getProperty(key).toString(),
		// // 1);
		// // }
		// // }
		// }
		//
		// }
		// Collections.sort(props);
		// for (String str : props) {
		// System.out.println(str);
		// }

		// for (String propertyValue : samePropertyValues.keySet()) {
		// System.out.println(propertyValue + " ; " +
		// samePropertyValues.get(propertyValue));
		// }

		// for (Node node1 :
		// GlobalGraphOperations.at(knowledgeGraph).getAllNodes()) {
		// for (Node node2 :
		// GlobalGraphOperations.at(knowledgeGraph).getAllNodes()) {
		// if (node1.getId() != node2.getId()) {
		// levenshtein.HowMuchTwoNodesAreSimilar(node1, node2);
		// }
		// }
		// }

		// nodeAndTheirDegrees();
		// tx1.success();
		// }catch(
		//
		// Exception exc)
		// {
		// System.out.println("queryGraph Transaction failed");
		// exc.printStackTrace();
		// }finally
		// {
		knowledgeGraph.shutdown();
		// }

	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
}
