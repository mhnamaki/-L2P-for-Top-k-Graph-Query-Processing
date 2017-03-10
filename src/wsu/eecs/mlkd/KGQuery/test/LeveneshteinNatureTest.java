package wsu.eecs.mlkd.KGQuery.test;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import wsu.eecs.mlkd.KGQuery.TopKQuery.Levenshtein;

public class LeveneshteinNatureTest {

//	public static String getDomainName(String url) throws URISyntaxException, Exception {
//		 URL aURL = new URL(url);
//		 String[] strArrays = aURL.getHost().replace("www.","").split("\\/");
//		 if(strArrays.length==0){
//			 System.err.println(aURL);
//			 return "";
//		 }
//		 return strArrays[0];
//	}

	public static void main(String[] args) throws Exception {
//		System.out.println(getDomainName("http://wiki.wxwidgets.org/Development:_Supported_Classes"));
//		System.out.println(getDomainName("http://wikimapia.org/4766034/Chanda-Bazar-Panchita "));
//		System.out.println(getDomainName("https://www.hu-berlin.de/?set_language=en&cl=en"));
//		System.out.println(getDomainName("https://www.law.upenn.edu/journals/jlasc/"));
//		System.out.println(getDomainName("https://www.linkedin.com/in/nielsboon"));
//		System.out.println(getDomainName("https://www.madison.k12.al.us/schools/mchs/default.aspx"));

		 System.out.println(Levenshtein.normalizedDistance("a", "a"));
		 System.out.println(Levenshtein.normalizedDistance("aa", "aa"));
		 System.out.println(Levenshtein.normalizedDistance("aaa", "aaa"));
		 System.out.println(Levenshtein.normalizedDistance("ahmad", "ahlar"));

		// System.out.println(Levenshtein.normalizedDistance("TelevisionShow",
		// "TelevisionShow"));
		// System.out.println(Levenshtein.normalizedDistance("http://channel.nationalgeographic.com/channel/drugs-inc/",
		// "http://zzzzz"));

	}

}
