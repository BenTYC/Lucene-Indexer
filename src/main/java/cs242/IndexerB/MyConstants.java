package cs242.IndexerB;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.CharArraySet;

public class MyConstants {

	public static final String FILES_TO_INDEX_DIRECTORY = "filesToIndex";
	public static final String INDEX_DIRECTORY = "Index";
	
	public static final String SIMILARITY_TFIDF = "tfidf";
	public static final String SIMILARITY_BM25 = "bm25";

	public static final String FIELD_PATH = "path";
	public static final String FIELD_URL = "url";
	public static final String FIELD_TITLE = "title";
	public static final String FIELD_CONTENT = "content";
	
	public static final int MAX_SEARCH = 1000;
	
	public static final CharArraySet ENGLISH_STOP_WORDS_SET;
	
	static {
	    final List<String> stopWords = Arrays.asList(
	      "a", "an", "and", "are", "as", "at", "be", "but", "by",
	      "for", "if", "in", "into", "is", "it",
	      "no", "not", "of", "on", "or", "such",
	      "that", "the", "their", "then", "there", "these",
	      "they", "this", "to", "was", "will", "with"
	    );
	    final CharArraySet stopSet = new CharArraySet(stopWords, false);
	    ENGLISH_STOP_WORDS_SET = CharArraySet.unmodifiableSet(stopSet); 
	  }

}
