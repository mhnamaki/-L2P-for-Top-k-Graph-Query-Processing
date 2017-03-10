package wsu.eecs.mlkd.KGQuery.DBLPImporter;

import java.util.ArrayList;

//author;
//editor;

public class Entity {
	String type;
	// article;
	// proceedings;
	// inproceedings;
	// incollection;
	// book;
	// phdthesis;
	// mastersthesis;
	// www;
	String mdate;
	String key;
	ArrayList<String> authors = new ArrayList<String>();
	ArrayList<String> editors = new ArrayList<String>();
	ArrayList<String> cites = new ArrayList<String>();
	String title;
	String pages;
	String year;
	String volume;
	String journal;
	String number;
	String url;
	String ee;
	String note;
	String cdRom;
	String crossref;
	String isbn;
	String booktitle;
	String series;
	String publisher;
	String month;
	String school;
	String chapter;
	String address;
}
