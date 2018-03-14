package cs242.IndexerB;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

public class App {
	// default setting
	static String fileToIndexPath = MyConstants.FILES_TO_INDEX_DIRECTORY;
	static String indexSimilarity = MyConstants.SIMILARITY_BM25;
	static String indexDirectory = MyConstants.INDEX_DIRECTORY;

	public static void main(String[] args) throws IOException, ParseException {

		// parse settings
		
		for (int i = 0; i < args.length; i++) {
			switch (args[i].toLowerCase()) {
			case "-p":
				fileToIndexPath = args[i + 1];
				break;
			case "-d":
				indexDirectory = args[i + 1];
				break;
			case "-s":
				indexSimilarity = args[i + 1];
				break;
			}
			i++;
		}
		
		
		// Add stop words
		// Analyzer analyzer = new StandardAnalyzer(addStopWords());
		Analyzer analyzer = new StandardAnalyzer();

		// To store an index on disk:
		Directory directory = FSDirectory.open(Paths.get(indexDirectory));

		// Indexing
		createIndex(directory, analyzer, indexSimilarity, fileToIndexPath);

		directory.close();
	}

	public static void createIndex(Directory directory, Analyzer analyzer, String similarity, String fileToIndexPath)
			throws IOException {		
		
		//Setup index config
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		LogDocMergePolicy logByteSizeMergePolicy = new LogDocMergePolicy();
		logByteSizeMergePolicy.setMergeFactor(3);
		config.setMergePolicy(logByteSizeMergePolicy);

		config.setOpenMode(OpenMode.CREATE);
		switch (similarity.toLowerCase()) {
		case MyConstants.SIMILARITY_BM25:
			config.setSimilarity(new BM25Similarity());
			break;
		case MyConstants.SIMILARITY_TFIDF:
			config.setSimilarity(new ClassicSimilarity());
			break;
		}
		
        config.setUseCompoundFile(true);
		IndexWriter iwriter = new IndexWriter(directory, config);
		
		//Create file to record runtime
		BufferedWriter bwriter = new BufferedWriter(new FileWriter("time.txt"));
		Date startTime = new Date();
		
		// Open file to index
		File inputFile = new File(fileToIndexPath);
		Object[] lines = Files.lines(Paths.get(inputFile.toString()), StandardCharsets.UTF_8).toArray();

		int startLine = findNextHttp(lines, 0);
		int endLine = 0, count_doc = 0;
		while(endLine != lines.length - 1){
			endLine = findNextHttp(lines, startLine + 1) - 1;
			endLine = (endLine > 0)? endLine:lines.length - 1;	
			
			//Write doc to index
			writeToIndex(iwriter, lines, startLine, endLine);
			
			//Record time per 1000 docs
			if(count_doc++ % 1000 == 0)
				bwriter.write(Long.toString(new Date().getTime() - startTime.getTime()) + "\n");
			    
			startLine = endLine + 1;
		}
		bwriter.close();
		
		Date endTime = new Date();
		System.out.println("Number of doc: " + iwriter.numDocs());
		System.out.println("Indexing time: " + (endTime.getTime() - startTime.getTime()) + " total milliseconds\n");

		iwriter.close();
	}

	static void addStopWords() {
		CharArraySet stopSet = CharArraySet.copy(StandardAnalyzer.STOP_WORDS_SET);
		stopSet.addAll(MyConstants.ENGLISH_STOP_WORDS_SET);
	}
	
	static int findNextHttp(Object[] lines, int row) {
		while(row < lines.length) {
			String line = (String) lines[row];
			if(line.startsWith("https://www.reddit.com")) 
				return row;
			
			row++;
		}
		return -2;
	}
	
	static void writeToIndex(IndexWriter iwriter, Object[] lines, int start, int end) throws IOException {
		//Build document
		Document document = new Document();
		System.out.println(lines[start]);
		//System.out.println("Title: " + lines[start + 1]);
		
		// URL
		String url = (String) lines[start];
		Field urlField = new StringField(MyConstants.FIELD_URL, url, Field.Store.YES);
		document.add(urlField);
		
		// title
		String title = (String) lines[start + 1];
		Field titleField = new TextField(MyConstants.FIELD_TITLE, title, Field.Store.YES);
		document.add(titleField);
		
		// content
		StringBuilder contents = new StringBuilder(); 
	    for(int i = start + 1; i <= end; i++) 
	    	    contents.append(lines[i]);
	    
		Field contentField = new TextField(MyConstants.FIELD_CONTENT, contents.toString(), Field.Store.YES);
		document.add(contentField);
		
		//Doc to index
		iwriter.addDocument(document);
		
	}
}
