package wsu.eecs.mlkd.KGQuery.algo;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.neo4j.cypher.internal.compiler.v2_2.helpers.IteratorSupport;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.graphdb.index.Index;
import java.util.Map;

public class GraphNeo4j {

	// private static final String DB_PATH =
	// "/Users/qsong/Downloads/dbpedia_infobox_properties_en.db";
	// private static final String DB_PATH =
	// "/Users/qsong/Downloads/social_network_factor_1.db";
	// private static final String DB_PATH =
	// "/Users/qsong/Downloads/YagoCores_graph_small_sample1.db";
	// private static final String DB_PATH =
	// "/Users/qsong/Downloads/watdiv-graph.db";
	// private static final String DB_PATH = "/Users/qsong/Downloads/movie.db";
	private static final String DB_PATH = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.2.5/data/dbpedia_infobox_properties_en.db";
	private GraphDatabaseService graphDb;
	private static final String NAME_KEY = "name";

	public void setUp(String PATH) throws IOException {
		// FileUtils.deleteRecursively( new File( DB_PATH ) );
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(PATH);
		registerShutdownHook(graphDb);
	}

	public void shutdown() {
		graphDb.shutdown();
	}

	public static void main(String[] args) throws IOException {
		GraphNeo4j graphneo4j = new GraphNeo4j();
		graphneo4j.setUp(DB_PATH);
		int nodeNumber = 0;
		int relationshipNumber = 0;
		int deleteNumber = 0;
		long nodeId = 0;
		int zeroDegreeNodeNumber = 0;
		int nullLabelsNumber = 0;
		long wholeProperties = 0;
		long wholeLabels = 0;
		Relationship relationshipdelete = null;
		int multipleEdges = 0;
		int number = 0;

		// ---------------------------------------------------------
		// this part deletes independent nodes
		// ---------------------------------------------------------
		/*
		 * try ( Transaction tx = graphneo4j.graphDb.beginTx() ) {
		 * GlobalGraphOperations globalOperation =
		 * GlobalGraphOperations.at(graphneo4j.graphDb); Iterable<Node> neoNodes
		 * = globalOperation.getAllNodes(); for ( Node item : neoNodes ) {
		 * nodeNumber ++; if (nodeNumber >= 5000000) break; nodeId=item.getId();
		 * if (item.getDegree() == 0){ //System.out.println("node Id: " +
		 * nodeId); Node nodedelete = graphneo4j.graphDb.getNodeById(nodeId);
		 * //System.out.println("Node?: " + nodedelete.toString() + " " +
		 * nodedelete.getId()); nodedelete.delete(); deleteNumber++; if
		 * (deleteNumber%100000 == 0) System.out.println("delete "+deleteNumber+
		 * " node"); } } for ( Node item : neoNodes) { if (item.getDegree()==0)
		 * zeroDegreeNodeNumber ++; } System.out.println(deleteNumber);
		 * System.out.println("deal with" + nodeNumber); System.out.println(
		 * "all independent nodes: " + zeroDegreeNodeNumber); tx.success(); }
		 */

		// ---------------------------------------------------------
		// delete nodes with small degree and related relationships
		// ---------------------------------------------------------
		/*
		 * try ( Transaction tx = graphneo4j.graphDb.beginTx() ) {
		 * GlobalGraphOperations globalOperation =
		 * GlobalGraphOperations.at(graphneo4j.graphDb); Iterable<Node> neoNodes
		 * = globalOperation.getAllNodes(); for (Node item : neoNodes ) {
		 * 
		 * if (number >= 80000) break; nodeId=item.getId(); if
		 * ((item.getDegree() < 10) && (nodeId > 4393388)){ number ++; Node
		 * nodedelete = graphneo4j.graphDb.getNodeById(nodeId);
		 * nodedelete.delete(); Iterable<Relationship> itemRelationships =
		 * item.getRelationships(); for (Relationship ritem : itemRelationships)
		 * { long relationshipId = ritem.getId(); try { relationshipdelete =
		 * graphneo4j.graphDb.getRelationshipById(relationshipId);
		 * relationshipdelete.delete(); } catch (NotFoundException e){
		 * 
		 * }
		 * 
		 * } deleteNumber++; if (deleteNumber%10000 == 0) System.out.println(
		 * "delete "+deleteNumber+ " node"); } } System.out.println(
		 * "current node id: " + nodeId); for ( Node item : neoNodes) { if
		 * (item.getDegree() < 10) zeroDegreeNodeNumber ++; }
		 * System.out.println(deleteNumber); System.out.println("deal with" +
		 * number); System.out.println("still need to delete : " +
		 * zeroDegreeNodeNumber); tx.success(); }
		 */

		// ---------------------------------------------------------
		// This part calculates the number of nodes with different degrees
		// ---------------------------------------------------------
		/// *
//		Map<Integer, Integer> distributionOfDegreesMap = new HashMap<Integer, Integer>();
////		for (int i = 0; i < 700; i++) {
////			distributionOfDegreesMap.put(i, 0);
////		}
//		int sumDegrees=0;
//		int cnt=0;
//		try (Transaction tx = graphneo4j.graphDb.beginTx()) {
//			GlobalGraphOperations globalOperation = GlobalGraphOperations.at(graphneo4j.graphDb);
//			Iterable<Node> neoNodes = globalOperation.getAllNodes();
//			// ResourceIterable<String> allPropertyKeys =
//			// globalOperation.getAllPropertyKeys();
//			for (Node item : neoNodes) {
//					sumDegrees += item.getDegree();
//					cnt++;
////				if (degree < 700)
////					distributionOfDegreesMap.put(degree, distributionOfDegreesMap.get(degree) + 1);
//			}
//			tx.success();
////			for (int i = 0; i < distributionOfDegreesMap.size(); i++) {
////				System.out.println(i + ", " + distributionOfDegreesMap.get(i));
////			}
//			System.out.println(sumDegrees);
//			System.out.println(cnt);
//			System.out.println((float)sumDegrees/(float)cnt);
//			
//		}
		/// *

		// ---------------------------------------------------------
		// This part calculates the Average properties per Node
		// ---------------------------------------------------------
		/*
		 * try ( Transaction tx = graphneo4j.graphDb.beginTx() ) {
		 * GlobalGraphOperations globalOperation =
		 * GlobalGraphOperations.at(graphneo4j.graphDb); int properties; for
		 * (Node n : globalOperation.getAllNodes()) { properties = 0; nodeNumber
		 * ++; Iterable<String> keys = n.getPropertyKeys(); for(String key :
		 * keys) properties ++; wholeProperties += properties;
		 * if(nodeNumber%100000 == 0) System.out.println("Processed: " +
		 * nodeNumber); } System.out.println("total properties = " +
		 * wholeProperties); System.out.println(
		 * "average number of properties per node = " +
		 * (double)wholeProperties/nodeNumber); tx.success(); }
		 */

		// ---------------------------------------------------------
		// This part calculates the Average labels per Node
		// ---------------------------------------------------------
		/*
		 * try ( Transaction tx = graphneo4j.graphDb.beginTx() ) {
		 * GlobalGraphOperations globalOperation =
		 * GlobalGraphOperations.at(graphneo4j.graphDb); int nodeLabelNumber;
		 * for (Node n : globalOperation.getAllNodes()) { nodeLabelNumber = 0;
		 * nodeNumber ++; Iterable<Label> labels = n.getLabels(); for(Label
		 * label : labels) nodeLabelNumber ++; wholeLabels += nodeLabelNumber;
		 * if(nodeNumber%100000 == 0) System.out.println("Processed: " +
		 * nodeNumber); } System.out.println("total labels = " + wholeLabels);
		 * System.out.println("average number of labels per node = " +
		 * (double)wholeLabels/nodeNumber); tx.success(); }
		 */

		// ---------------------------------------------------------
		// This part calculates the no label Node
		// ---------------------------------------------------------
		/*
		 * try ( Transaction tx = graphneo4j.graphDb.beginTx() ) {
		 * GlobalGraphOperations globalOperation =
		 * GlobalGraphOperations.at(graphneo4j.graphDb); int nodeLabelNumber;
		 * for (Node n : globalOperation.getAllNodes()) { nodeLabelNumber = 0;
		 * nodeNumber ++; Iterable<Label> labels = n.getLabels(); for(Label
		 * label : labels) nodeLabelNumber ++; if(nodeLabelNumber == 0)
		 * wholeLabels++; } System.out.println(wholeLabels); tx.success(); }
		 */
		// ---------------------------------------------------------
		// This part calculates the number of nodes and relationships.
		// ---------------------------------------------------------

		// try ( Transaction tx = graphneo4j.graphDb.beginTx() )
		// {
		// // Database operations go here
		// System.out.println("...running");
		// GlobalGraphOperations globalOperation =
		// GlobalGraphOperations.at(graphneo4j.graphDb);
		// ResourceIterable<Node> neoNodes = globalOperation.getAllNodes();
		// for ( Node item : neoNodes )
		// {
		// nodeNumber++;
		// // System.out.println(item.getId());
		// }
		// System.out.println("node number: " + nodeNumber);
		// Iterable<Relationship> neoRelationships =
		// globalOperation.getAllRelationships();
		// for (Relationship item:neoRelationships)
		// {
		// relationshipNumber++;
		// }
		//
		// System.out.println("Relationship number: " + relationshipNumber);
		// tx.success();
		// }

		// delete nodes to create a new graph (from some sample nodes)
		/*
		 * try ( Transaction tx = graphneo4j.graphDb.beginTx() ) { HashSet<Node>
		 * nodeSet = new HashSet<Node>(); HashSet<Node > nodeSet_1hop = new
		 * HashSet<Node>(); GlobalGraphOperations globalOperation =
		 * GlobalGraphOperations.at(graphneo4j.graphDb); for(Node n :
		 * globalOperation.getAllNodes()) { for(Label l : n.getLabels()){
		 * if(l.name().equals("wikicat_Women")){ nodeSet.add(n); } } } for(Node
		 * n : nodeSet){ nodeSet_1hop.add(n); for(Relationship r :
		 * n.getRelationships(Direction.INCOMING))
		 * nodeSet_1hop.add(r.getStartNode()); for(Relationship r :
		 * n.getRelationships(Direction.OUTGOING))
		 * nodeSet_1hop.add(r.getEndNode()); } System.out.println(
		 * "starting to delete"); int delCnt=0; for(Relationship r :
		 * globalOperation.getAllRelationships()) {
		 * if(nodeSet_1hop.contains(r.getStartNode()) &&
		 * nodeSet_1hop.contains(r.getEndNode())) continue; else{ r.delete();
		 * delCnt++; if(delCnt%1000==0) System.out.println(delCnt); } }
		 * 
		 * for(Node n : globalOperation.getAllNodes()) {
		 * if(!nodeSet_1hop.contains(n)) n.delete(); }
		 * 
		 * tx.success(); }
		 */

		/*
		 * try ( Transaction tx = graphneo4j.graphDb.beginTx() ) { Node n =
		 * graphneo4j.graphDb.getNodeById(165249); for(Label l : n.getLabels())
		 * System.out.println(l); for(String s : n.getPropertyKeys())
		 * System.out.println(s + "-------" + n.getProperty(s)); tx.success(); }
		 */

		/*
		 * 
		 * try ( Transaction tx = graphneo4j.graphDb.beginTx() ) { // Database
		 * operations go here System.out.println("...running");
		 * GlobalGraphOperations globalOperation =
		 * GlobalGraphOperations.at(graphneo4j.graphDb); Iterable<Relationship>
		 * neoRelationships = globalOperation.getAllRelationships(); for (
		 * Relationship item1 : neoRelationships ){ for ( Relationship item2 :
		 * neoRelationships){ if(item1.getId() != item2.getId()){
		 * if(((item1.getStartNode().getId()==item2.getStartNode().getId())
		 * &&(item1.getEndNode().getId()==item2.getEndNode().getId()))
		 * ||((item1.getStartNode().getId()==item2.getEndNode().getId())
		 * &&(item1.getEndNode().getId()==item2.getStartNode().getId()))){
		 * System.out.println("multiple label exists"); multipleEdges ++;
		 * //System.out.println("Relationship1: " + item1.getId() +
		 * " Relationship2: " +item2.getId()); //System.out.println(
		 * "Relationship1 start node: " + item1.getStartNode().getId() +
		 * "End node" + item1.getEndNode().getId()); //System.out.println(
		 * "Relationship2 start node: " + item2.getStartNode().getId() +
		 * "End node" + item2.getEndNode().getId()); } } } } System.out.println(
		 * "Number of multiple edges = " + multipleEdges); tx.success(); }
		 */

		graphneo4j.shutdown();
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

}
