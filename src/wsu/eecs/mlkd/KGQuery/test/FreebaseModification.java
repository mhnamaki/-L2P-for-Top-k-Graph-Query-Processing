package wsu.eecs.mlkd.KGQuery.test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class FreebaseModification {
	public static String dbPath = "/Users/mnamaki/Documents/Education/PhD/Fall2015/BigData/Neo4j/neo4j-community-2.3.1/data/freebase.db";
	public static ArrayList<String> prefixes;
	private static BatchInserter db;
	private static BatchInserterIndex index;
	private static final String URI_PROPERTY = "__URI__";

	public static void main(String[] args) {

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-dbPath")) {
				dbPath = args[++i];
			}
		}
		GraphDatabaseService knowledgeGraph = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		registerShutdownHook(knowledgeGraph);

		Transaction tx1 = knowledgeGraph.beginTx();
		int counter = 0;
		HashMap<Long, String> uriProps = new HashMap<Long, String>();
		// HashMap<Long, ArrayList<Label>> prevLabels = new HashMap<Long,
		// ArrayList<Label>>();

		// HashSet<Long> problematicNodes = new HashSet<Long>();
		// problematicNodes.add(33522l);
		// problematicNodes.add(34000l);
		// problematicNodes.add(34802l);
		// problematicNodes.add(38264l);
		// problematicNodes.add(38673l);
		// problematicNodes.add(43883l);

		for (Node node : GlobalGraphOperations.at(knowledgeGraph).getAllNodes()) {
			if (!node.getLabels().iterator().hasNext()) {
				uriProps.put(node.getId(), node.getProperty("__URI__").toString().toLowerCase());
			}

			// ArrayList<Label> lbls = new ArrayList<Label>();
			// try {
			// if (!problematicNodes.contains(node.getId())) {
			// for (Label label : node.getLabels()) {
			// lbls.add(DynamicLabel.label("dbp_lbl_" + label));
			// }
			// } else {
			// lbls.add(DynamicLabel.label("dbp_lbl_commmon_label"));
			// }
			// } catch (Exception exc) {
			// System.out.println("not created prevLabel: " + node.getId());
			// }
			// prevLabels.put(node.getId(), lbls);
		}
		System.out.println("uriProps size: " + uriProps.size());
		tx1.success();
		tx1.close();
		knowledgeGraph.shutdown();

		Map<String, String> config = new HashMap<String, String>();

		// config.put("dbms.pagecache.pagesize", "32g");
		// config.put("neostore.nodestore.db.mapped_memory", "50000M");
		// config.put("node_auto_indexing", "true");
		db = BatchInserters.inserter(dbPath/* , config */);
		BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db);
		index = indexProvider.nodeIndex("freeBaseIndex", MapUtil.stringMap("type", "exact"));
		index.setCacheCapacity(URI_PROPERTY, 500000001);
		System.out.println("After filling the uriProps map ");
		prefixes = new ArrayList<String>();
		fillArrayList(prefixes);
		for (Long nodeId : uriProps.keySet()) {

			String originalURI = uriProps.get(nodeId);
			String modifiedURI = getModifiedURI(originalURI);
			// make sure that you don't insert empty string as a label
			if (!modifiedURI.equals("")) {
				// System.out.println("nodeId: " + nodeId);
				modifiedURI = "frb_uri_" + modifiedURI;
				System.out.println(originalURI + ", " + modifiedURI);
				db.setNodeLabels(nodeId, new Label[] { DynamicLabel.label(modifiedURI) });
			} else {
				System.err.println("originalURI was: " + originalURI + " -> empty modified!");
			}
			counter++;
			if ((counter % 500000) == 0) {
				index.flush();
				System.out.println(counter + " progress.");
				// tx1.success();
				// tx1.close();
				// tx1 = knowledgeGraph.beginTx();
			}
		}

		// tx1.success();
		// tx1.close();

		index.flush();

		uriProps = null;
		System.gc();
		System.runFinalization();
		
		///System.out.println("uriProps size: " + uriProps.size());
		System.out.println("counter: " + counter);
		System.out.println("indexProvider shutting down");
		indexProvider.shutdown();

		System.out.println("db shutting down");
		db.shutdown();

		System.out.println("knowledgeGraph shutting down");
		knowledgeGraph.shutdown();

		System.out.println("completed");

	}

	private static String getModifiedURI(String uri) {
		if (uri.startsWith("http:") && !uri.startsWith("http://")) {
			uri = uri.replace("http:/", "");
			uri = uri.replace("http:", "");
		}

		for (String prefix : prefixes) {
			if (uri.startsWith(prefix) && uri.length() != prefix.length()) {
				return uri.replace(prefix, "");
			}
		}
		URL aURL;
		try {
			aURL = new URL(uri);
			String[] strArrays = aURL.getHost().replace("www.", "").split("\\/");
			if (strArrays.length == 0) {
				return aURL.getHost().replace("www.", "");
			}
			return strArrays[0];
		} catch (MalformedURLException e) {
			return uri;
		}

	}

	private static void fillArrayList(ArrayList<String> prefixes) {
		prefixes.add("http:////");
		prefixes.add("http:www.");
		prefixes.add("http:///www.");
		prefixes.add("http://www./");
		prefixes.add("http://www.?");
		prefixes.add("http://www:");
		prefixes.add("mms://");
		prefixes.add("http://rdf.freebase.com/ns/");
		prefixes.add("http://dbpedia.org/resource/");
		prefixes.add("http://disneychannel.disney.com/");
		prefixes.add("http://disneyjunior.com/");
		prefixes.add("http://disneyxd.disney.com/");
		prefixes.add("http://distribution.dhxmedia.com/catalogue/animation/");
		prefixes.add("http://distribution.dhxmedia.com/catalogue/preschool/");
		prefixes.add("http://distribution.dhxmedia.com/catalogue/preschool/");
		prefixes.add("http://dnr.wi.gov/topic/parks/name/");
		prefixes.add("http://dnr2.maryland.gov/publiclands/Pages/central/");
		prefixes.add("http://dsc.discovery.com/fansites/bike/");
		prefixes.add("http://dsc.discovery.com/tv/");
		prefixes.add("http://dsc.discovery.com/tv-shows/");
		prefixes.add("http://dtvamerica.com/stations/");
		prefixes.add("http://dubois.fas.harvard.edu/");
		prefixes.add("http://edu.kde.org/");
		prefixes.add("http://edu.kde.org/");
		prefixes.add("http://en.wikipedia.org/wiki/");
		prefixes.add("http://tv.sbs.co.kr/");
		prefixes.add("http://facebook.com/");
		prefixes.add("http://tvcity.tvb.com/special/");
		prefixes.add("http://tvg.globo.com/novelas/");
		prefixes.add("http://tvn.lifestyler.co.kr/drama/");
		prefixes.add("http://tvnz.co.nz/");
		prefixes.add("http://twitter.com/#!/");
		prefixes.add("http://uk.linkedin.com/pub/");
		prefixes.add("http://uk.linkedin.com/in/");
		prefixes.add("http://upload.wikimedia.org/wikipedia/");
		prefixes.add("http://v3.player.abacast.com/");
		prefixes.add("http://v4.player.abacast.com/");
		prefixes.add("http://v5.player.abacast.com/");
		prefixes.add("http://v6.player.abacast.com/");
		prefixes.add("http://wayback.archive.org/*/http://www.");
		prefixes.add("http://web.archive.org/*/http://www.");
		prefixes.add("http://web.archive.org/web/*/http://www.");
		prefixes.add("http://web.archive.org/web/*/http://");
		prefixes.add("http://web.archive.org/web/");
		prefixes.add("http://web.telecom.cz/");
		prefixes.add("http://www.myspace.com/");
		prefixes.add("https://twitter.com/");
		prefixes.add("https://web.archive.org/web/*/http://www.");
		prefixes.add("https://wiki.gnome.org/Apps/");
		prefixes.add("https://wiki.mozilla.org/");
		prefixes.add("https://www.edline.net/pages/");
		prefixes.add("https://www.facebook.com/pages/");
		prefixes.add("https://www.facebook.com/");
		prefixes.add("https://www.youtube.com/user/");
		prefixes.add("https://www.youtube.com/");
		prefixes.add("http://juusho.com");
		prefixes.add("http://stateofillinois.us");
		prefixes.add("http://stateofnevada.info");
		prefixes.add("http://upload.wikimedia.org/wikipedia/en/");
		prefixes.add("http://upload.wikimedia.org/wikipedia/");
		prefixes.add("http://upload.wikimedia.org/wikipedia/commons/");
		prefixes.add("http://upload.wikimedia.org/wikipedia/en/");
		prefixes.add("http://upload.wikimedia.org/wikipedia/commons/");
		prefixes.add("http://covers.openlibrary.org/b/olid");
		prefixes.add("http://images.thetvdb.com/banners/episodes/");
		prefixes.add("http://www.whosdatedwho.com/celebrities/people/");
		prefixes.add("http://s3.amazonaws.com/twitter_production/profile_images/");
		prefixes.add("http://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=");
		prefixes.add("http://www.thetvdb.com/banners/episodes/");
		prefixes.add("http://cdn.static.ovimg.com/");
		prefixes.add("http://ec.snagfilms.com/");
		prefixes.add("http://ecx.images-amazon.com/");
		prefixes.add("http://ia.media-imdb.com/images/");
		prefixes.add("http://images.thetvdb.com/banners/episodes/");
		prefixes.add("http://images.tvrage.com/screencaps/");
		prefixes.add("http://plus.google.com/");
		prefixes.add("http://stateofillinois.us/");
		prefixes.add("http://sw.opencyc.org/concept/");
		prefixes.add("http://vimeo.com/");
		prefixes.add("http://www.facebook.com/");
		prefixes.add("http://www.hulu.com/watch/");
		prefixes.add("http://www.last.fm/music/");
		prefixes.add("http://www.linkedin.com/in/");
		prefixes.add("http://www.myspace.com/");
		prefixes.add("http://www.thetvdb.com/banners/episodes/");
		prefixes.add("http://www.twitter.com/");
		prefixes.add("http://www.whosdatedwho.com/celebrities/people/dating/");
		prefixes.add("http://www.youtube.com/watch?v=");
		prefixes.add("https://myspace.com/");
		prefixes.add("https://plus.google.com/+");
		prefixes.add("https://plus.google.com/u/0/b/");
		prefixes.add("https://twitter.com/");
		prefixes.add("https://www.facebook.com/");
		prefixes.add("https://www.facebook.com/pages/");
		prefixes.add("https://www.linkedin.com/company/");
		prefixes.add("https://www.pinterest.com/");
		prefixes.add("https://www.youtube.com/channel/");
		prefixes.add("https://www.youtube.com/watch?v=");

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
