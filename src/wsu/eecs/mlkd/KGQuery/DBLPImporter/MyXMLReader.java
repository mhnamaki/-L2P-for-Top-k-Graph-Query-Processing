package wsu.eecs.mlkd.KGQuery.DBLPImporter;

import javax.swing.ProgressMonitorInputStream;
import javax.xml.parsers.*;
import org.xml.sax.*;
import org.xml.sax.helpers.*;

import java.util.*;
import java.io.*;

public class MyXMLReader {

	public Set<String> author = new HashSet<String>();

	public static void main(String[] args) throws Exception, SAXException {
		
		System.setProperty("jdk.xml.entityExpansionLimit", "0");
		// XMLSecurityManager securityManager = new XMLSecurityManager(false);
		// factory.setProperty("http://apache.org/xml/properties/security-manager",
		// securityManager);
		try {

			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();

			DefaultHandler handler = new DefaultHandler() {
				HashMap<String, Entity> entityMap = new HashMap<String, Entity>();
				public int level = 0;
				String prevItemLevel2Key = "";
				boolean seenAuthorStartElem = false;
				boolean seenEditorStartElem = false;
				boolean seenTitleStartElem = false;
				boolean seenCiteStartElem = false;
				boolean seenPagesStartElem = false;
				boolean seenYearStartElem = false;
				boolean seenVolumeStartElem = false;
				boolean seenJournalStartElem = false;
				boolean seenNumberStartElem = false;
				boolean seenUrlStartElem = false;
				boolean seenEeStartElem = false;
				boolean seenNoteStartElem = false;
				boolean seenCdRomStartElem = false;
				boolean seenCrossrefStartElem = false;
				boolean seenIsbnStartElem = false;
				boolean seenBooktitleStartElem = false;
				boolean seenSeriesStartElem = false;
				boolean seenPublisherStartElem = false;
				boolean seenMonthStartElem = false;
				boolean seenSchoolStartElem = false;
				boolean seenChapterStartElem = false;
				boolean seenAddressStartElem = false;

				int elementCnt = 0;

				@Override
				public void endDocument() throws SAXException {

					new Neo4jCreator(entityMap);

					super.endDocument();
				}

				public void startElement(String uri, String localName, String qName, Attributes attributes)
						throws SAXException {
					// attributes.getValue("name")
					level++;
					elementCnt++;
					if ((elementCnt % 10000000) == 0) {
						System.out.println(elementCnt);
					}
					if (level == 2) {
						try {
							// TODO: check if this key already exists
							String key = attributes.getValue("key");
							if (entityMap.get(key) != null) {
								System.err.println("duplicate key! " + key);
							}
							Entity entity = new Entity();
							entity.key = key;
							prevItemLevel2Key = key;
							entity.type = qName;
							try {
								entity.mdate = attributes.getValue("mdate");
							} catch (Exception exc) {
							}
							entityMap.put(key, entity);
						} catch (Exception exc) {
							System.err.println("no key for item in level 2 is found!");
						}
					} else if (level == 3) {
						if (qName == "author") {
							seenAuthorStartElem = true;
						} else if (qName == "editor") {
							seenEditorStartElem = true;
						} else if (qName == "title") {
							seenTitleStartElem = true;
						} else if (qName == "cite") {
							seenCiteStartElem = true;
						} else if (qName == "pages") {
							seenPagesStartElem = true;
						} else if (qName == "year") {
							seenYearStartElem = true;
						} else if (qName == "volume") {
							seenVolumeStartElem = true;
						} else if (qName == "journal") {
							seenJournalStartElem = true;
						} else if (qName == "number") {
							seenNumberStartElem = true;
						} else if (qName == "url") {
							seenUrlStartElem = true;
						} else if (qName == "ee") {
							seenEeStartElem = true;
						} else if (qName == "note") {
							seenNoteStartElem = true;
						} else if (qName == "cdRom") {
							seenCdRomStartElem = true;
						} else if (qName == "crossref") {
							seenCrossrefStartElem = true;
						} else if (qName == "isbn") {
							seenIsbnStartElem = true;
						} else if (qName == "booktitle") {
							seenBooktitleStartElem = true;
						} else if (qName == "series") {
							seenSeriesStartElem = true;
						} else if (qName == "publisher") {
							seenPublisherStartElem = true;
						} else if (qName == "month") {
							seenMonthStartElem = true;
						} else if (qName == "school") {
							seenSchoolStartElem = true;
						} else if (qName == "chapter") {
							seenChapterStartElem = true;
						} else if (qName == "address") {
							seenAddressStartElem = true;
						}
					}

				}

				public void endElement(String uri, String localName, String qName) throws SAXException {
					level--;
				}

				public void characters(char ch[], int start, int length) throws SAXException {
					if (seenAuthorStartElem) {
						entityMap.get(prevItemLevel2Key).authors.add(new String(ch, start, length));
						seenAuthorStartElem = false;
					} else if (seenEditorStartElem) {
						entityMap.get(prevItemLevel2Key).editors.add(new String(ch, start, length));
						seenEditorStartElem = false;
					} else if (seenCiteStartElem) {
						entityMap.get(prevItemLevel2Key).cites.add(new String(ch, start, length));
						seenCiteStartElem = false;
					} else if (seenTitleStartElem) {
						entityMap.get(prevItemLevel2Key).title = new String(ch, start, length);
						seenTitleStartElem = false;
					} else if (seenCrossrefStartElem) {
						entityMap.get(prevItemLevel2Key).crossref = new String(ch, start, length);
						seenCrossrefStartElem = false;
					} else if (seenPagesStartElem) {
						entityMap.get(prevItemLevel2Key).pages = new String(ch, start, length);
						seenPagesStartElem = false;
					} else if (seenYearStartElem) {
						entityMap.get(prevItemLevel2Key).year = new String(ch, start, length);
						seenYearStartElem = false;
					} else if (seenAddressStartElem) {
						entityMap.get(prevItemLevel2Key).address = new String(ch, start, length);
						seenAddressStartElem = false;
					} else if (seenBooktitleStartElem) {
						entityMap.get(prevItemLevel2Key).booktitle = new String(ch, start, length);
						seenBooktitleStartElem = false;
					} else if (seenCdRomStartElem) {
						entityMap.get(prevItemLevel2Key).cdRom = new String(ch, start, length);
						seenCdRomStartElem = false;
					} else if (seenChapterStartElem) {
						entityMap.get(prevItemLevel2Key).chapter = new String(ch, start, length);
						seenChapterStartElem = false;
					} else if (seenEeStartElem) {
						entityMap.get(prevItemLevel2Key).ee = new String(ch, start, length);
						seenEeStartElem = false;
					} else if (seenMonthStartElem) {
						entityMap.get(prevItemLevel2Key).month = new String(ch, start, length);
						seenMonthStartElem = false;
					} else if (seenNumberStartElem) {
						entityMap.get(prevItemLevel2Key).number = new String(ch, start, length);
						seenNumberStartElem = false;
					} else if (seenNoteStartElem) {
						entityMap.get(prevItemLevel2Key).note = new String(ch, start, length);
						seenNoteStartElem = false;
					} else if (seenJournalStartElem) {
						entityMap.get(prevItemLevel2Key).journal = new String(ch, start, length);
						seenJournalStartElem = false;
					} else if (seenVolumeStartElem) {
						entityMap.get(prevItemLevel2Key).volume = new String(ch, start, length);
						seenVolumeStartElem = false;
					} else if (seenPublisherStartElem) {
						entityMap.get(prevItemLevel2Key).publisher = new String(ch, start, length);
						seenPublisherStartElem = false;
					} else if (seenIsbnStartElem) {
						entityMap.get(prevItemLevel2Key).isbn = new String(ch, start, length);
						seenIsbnStartElem = false;
					} else if (seenSeriesStartElem) {
						entityMap.get(prevItemLevel2Key).series = new String(ch, start, length);
						seenSeriesStartElem = false;
					} else if (seenUrlStartElem) {
						entityMap.get(prevItemLevel2Key).url = new String(ch, start, length);
						seenUrlStartElem = false;
					} else if (seenSchoolStartElem) {
						entityMap.get(prevItemLevel2Key).school = new String(ch, start, length);
						seenSchoolStartElem = false;
					}

					// System.out.println(new String(ch, start, length));

				}

			};

			double sTime, eTime, duration;
			sTime = System.nanoTime();
			saxParser.parse("dblp.xml", handler);
			eTime = System.nanoTime();
			duration = (eTime - sTime) / 1e6;
			System.out.println("duration: " + duration);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
