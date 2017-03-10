package wsu.eecs.mlkd.KGQuery.DBLPImporter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class Neo4jCreator {
	HashMap<String, Long> distinctAuthors;
	HashMap<String, Long> distinctEditors;
	HashMap<String, Entity> entityMap;
	HashMap<String, Long> entityKeyNodeMap;
	private BatchInserter db;
	private BatchInserterIndex index;
	private static final String KEY_PROPERTY = "key";
	private int totalEntity = 0;
	HashSet<Long> nodeIdCreated = new HashSet<Long>();
	BatchInserterIndexProvider indexProvider;

	private static enum RelTypes implements RelationshipType {
		EDITED_BY, WRITTEN_BY, CITE_TO, IS_CROSSREF_WITH, PUBLISHED_IN
	}

	public Neo4jCreator(HashMap<String, Entity> entityMap) {
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "50000M");
		config.put("dbms.pagecache.pagesize", "8g");
		config.put("node_auto_indexing", "true");
		db = BatchInserters.inserter("dblp2.db", config);
		indexProvider = new LuceneBatchInserterIndexProvider(db);
		index = indexProvider.nodeIndex("dblpIndex", MapUtil.stringMap("type", "exact"));
		index.setCacheCapacity(KEY_PROPERTY, 500000001);
		distinctAuthors = new HashMap<String, Long>();
		distinctEditors = new HashMap<String, Long>();
		entityKeyNodeMap = new HashMap<String, Long>();
		this.entityMap = entityMap;
		for (String key : entityMap.keySet()) {
			Entity entity = entityMap.get(key);
			for (String author : entity.authors) {
				if (distinctAuthors.get(author) == null) {
					distinctAuthors.put(author, 0L);
				}
			}
			for (String editor : entity.editors) {
				if (distinctEditors.get(editor) == null) {
					distinctEditors.put(editor, 0L);
				}
			}
		}
		System.gc();
		System.out.println("create the graph db is called!");
		createTheGraphDb();
	}

	private void createTheGraphDb() {
		System.out.println("There are this number of distinct authors: " + distinctAuthors.keySet().size());
		System.out.println("There are this number of distinct editors: " + distinctEditors.keySet().size());
		System.out.println("There are this number of entities: " + entityMap.keySet().size());
		// tx = db.beginTx();
		for (String author : distinctAuthors.keySet()) {
			totalEntity++;

			Label label = DynamicLabel.label(author);
			long nodeId = db.createNode(null, label);

			distinctAuthors.put(author, nodeId);
			nodeIdCreated.add(nodeId);
			if ((totalEntity % 50000) == 0) {
				index.flush();
				System.out.println("authors: " + totalEntity);
			}
		}

		System.out.println("distinctAuthors map nodes created!");

		for (String editor : distinctEditors.keySet()) {
			totalEntity++;
			Label label = DynamicLabel.label(editor);
			Long nodeId = db.createNode(null, label);

			nodeIdCreated.add(nodeId);
			distinctEditors.put(editor, nodeId);
			if ((totalEntity % 50000) == 0) {
				index.flush();
				System.out.println("editors: " + totalEntity);
			}
		}

		System.out.println("distinctEditors map nodes created!");

		for (String key : entityMap.keySet()) {
			totalEntity++;

			// System.out.println(totalEntity);

			if ((totalEntity % 1000000) == 0) {
				System.out.println("current number of entities: " + totalEntity);
			}
			Entity entity = entityMap.get(key);
			Map<String, Object> props = new HashMap<String, Object>();

			if (entity.address != null) {
				props.put("address", entity.address);
			}
			if (entity.booktitle != null) {
				props.put("booktitle", entity.booktitle);
			}
			if (entity.cdRom != null) {
				props.put("cdRom", entity.cdRom);
			}
			if (entity.chapter != null) {
				props.put("chapter", entity.chapter);
			}
			if (entity.ee != null) {
				props.put("ee", entity.ee);
			}
			if (entity.crossref != null) {
				props.put("crossref", entity.crossref);
			}
			if (entity.isbn != null) {
				props.put("isbn", entity.isbn);
			}
			if (entity.journal != null) {
				props.put("journal", entity.journal);
			}
			if (entity.key != null) {
				props.put("key", entity.key);
			}
			if (entity.mdate != null) {
				props.put("mdate", entity.mdate);
			}
			if (entity.month != null) {
				props.put("month", entity.month);
			}
			if (entity.note != null) {
				props.put("note", entity.note);
			}
			if (entity.number != null) {
				props.put("number", entity.number);
			}
			if (entity.pages != null) {
				props.put("pages", entity.pages);
			}
			if (entity.publisher != null) {
				props.put("publisher", entity.publisher);
			}
			if (entity.series != null) {
				props.put("series", entity.series);
			}
			if (entity.school != null) {
				props.put("school", entity.school);
			}
			if (entity.type != null) {
				props.put("type", entity.type);
			}
			if (entity.url != null) {
				props.put("url", entity.url);
			}
			if (entity.volume != null) {
				props.put("volume", entity.volume);
			}
			if (entity.year != null) {
				props.put("year", entity.year);
			}

			ArrayList<Label> labels = new ArrayList<Label>();
			if (entity.type != null)
				labels.add(DynamicLabel.label(entity.type));
			if (entity.title != null)
				labels.add(DynamicLabel.label(entity.title));

			if (entity.journal != null && entity.journal.length() > 0)
				labels.add(DynamicLabel.label(entity.journal));

			if (entity.note != null && entity.note.length() > 0)
				labels.add(DynamicLabel.label(entity.note));

			if (entity.booktitle != null && entity.booktitle.length() > 0)
				labels.add(DynamicLabel.label(entity.booktitle));

			if (entity.publisher != null && entity.publisher.length() > 0)
				labels.add(DynamicLabel.label(entity.publisher));

			// if (labels.size() == 0) {
			// System.out.print("no label!");
			// }
			// if (props.keySet().size() == 0) {
			// System.out.print("no prop!");
			// }
			// if (totalEntity == 1669890) {
			// int jkjk = 0;
			// jkjk++;
			// }
			if (labels.size() > 0) {
				Long nodeId = db.createNode(props, labels.toArray(new Label[labels.size()]));
				entityKeyNodeMap.put(key, nodeId);
				nodeIdCreated.add(nodeId);
			}

			if ((totalEntity % 50000) == 0) {
				System.out.println("entities: " + totalEntity);
				index.flush();
			}
		}
		System.out.println("all nodes added!");

		// insertIndex = 0;
		for (String key : entityMap.keySet()) {
			totalEntity++;
			Entity entity = entityMap.get(key);
			for (String author : entity.authors) {
				db.createRelationship(entityKeyNodeMap.get(key), distinctAuthors.get(author), RelTypes.WRITTEN_BY,
						null);
			}
			for (String editor : entity.editors) {
				db.createRelationship(entityKeyNodeMap.get(key), distinctAuthors.get(editor), RelTypes.EDITED_BY, null);
			}
			for (String cite : entity.cites) {
				if (nodeIdCreated.contains(entityKeyNodeMap.get(cite)))
					db.createRelationship(entityKeyNodeMap.get(key), entityKeyNodeMap.get(cite), RelTypes.CITE_TO,
							null);
			}

			if (entityKeyNodeMap.get(entity.crossref) != null
					&& nodeIdCreated.contains(entityKeyNodeMap.get(entity.crossref))) {
				db.createRelationship(entityKeyNodeMap.get(key), entityKeyNodeMap.get(entity.crossref),
						RelTypes.IS_CROSSREF_WITH, null);
			}
			if (entityKeyNodeMap.get(entity.publisher) != null
					&& nodeIdCreated.contains(entityKeyNodeMap.get(entity.publisher))) {
				db.createRelationship(entityKeyNodeMap.get(key), entityKeyNodeMap.get(entity.publisher),
						RelTypes.PUBLISHED_IN, null);
			}
			if ((totalEntity % 50000) == 0) {
				index.flush();
				System.out.println("relationship: " + totalEntity);
			}
		}

		System.out.println("all relationship added!");

		System.out.println("indexProvider shutting down");
		indexProvider.shutdown();

		System.out.println("db shutting down");
		db.shutdown();

		System.out.println("program is finished!");

	}
}
