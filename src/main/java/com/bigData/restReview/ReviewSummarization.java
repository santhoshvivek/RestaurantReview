package com.bigData.restReview;

import java.io.File;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.StringLogger;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;

class MyComparator implements java.util.Comparator<String> {
	public MyComparator() {
		super();
	}

	public int compare(String s1, String s2) {
		int dist1 = Math.abs(s1.length());
		int dist2 = Math.abs(s2.length());

		return dist2 - dist1;
	}
}

public class ReviewSummarization {

	public GraphDatabaseService db;
	static MaxentTagger tagger;

	public void setup(GraphDatabaseService db) {
		this.db = db;
		ClassLoader classLoader = getClass().getClassLoader();
		File taggerModelFile = new File(classLoader.getResource(
				"english-left3words-distsim.tagger").getFile());
		tagger = new MaxentTagger(taggerModelFile.getAbsolutePath());
	}

	public void deleteFile(File file) {
		if (file.isDirectory()) {
			for (File f : file.listFiles())
				deleteFile(f);
		}
		file.delete();
	}

	public Boolean isSignificant(String sentence) {
		boolean flagNoun = false;
		boolean flagVerb = false;
		String taggedSentence = tagger.tagString(sentence).trim();
		if (taggedSentence.endsWith("CD") || taggedSentence.endsWith("VBZ")
				|| taggedSentence.endsWith("very_RB")
				|| taggedSentence.endsWith("not_RB")
				|| taggedSentence.endsWith("RP")
				|| taggedSentence.endsWith("IN")
				|| taggedSentence.endsWith("WP")
				|| taggedSentence.endsWith("WDT")
				|| taggedSentence.endsWith("WP$")
				|| taggedSentence.endsWith("WRB")) {
			return false;
		}
		String[] wordSplit = taggedSentence.split("\\s+");
		HashSet<String> tagSet = new HashSet<String>();
		int i = 0;
		for (String word : wordSplit) {

			String[] wordTag = word.split("_");
			if (i == 0) {
				if (wordTag[1].equals("IN") || wordTag[1].equals("WP")
						|| wordTag[1].equals("WDT") || wordTag[1].equals("WP$")
						|| wordTag[1].equals("WRB")) {
					return false;
				}
			}
			tagSet.add(wordTag[1]);

		}
		if (tagSet.contains("JJ") || tagSet.contains("JJR")
				|| tagSet.contains("JJS")) {
			if (tagSet.contains("NN") || tagSet.contains("NNS")
					|| tagSet.contains("NNP") || tagSet.contains("NNPS")) {
				return true;
			}
		} else if (tagSet.contains("RB") || tagSet.contains("RBR")
				|| tagSet.contains("RBS")) {
			if (tagSet.contains("NN") || tagSet.contains("NNS")
					|| tagSet.contains("NNP") || tagSet.contains("NNPS")) {
				flagNoun = true;
			}
			if (tagSet.contains("VB") || tagSet.contains("VBD")
					|| tagSet.contains("VBG") || tagSet.contains("VBN")
					|| tagSet.contains("VBP") || tagSet.contains("VBZ")) {
				flagVerb = true;
			}
			if (flagNoun && flagVerb)
				return true;

		}

		return false;
	}

	public List<String> getReviews(Result result) {
		List<String> reviews = new LinkedList<String>();
		ResourceIterator<Object> resultIterator = result.columnAs("review");

		while (resultIterator.hasNext()) {
			Node resultNode = (Node) resultIterator.next();
			reviews.add((String) resultNode.getProperty("value"));
		}

		return reviews;
	}

	public void getFoodReviewSummarization(String restaurantId, String tmpDbPath) {
		List<String> reviews = new LinkedList<String>();
		Transaction ignored = db.beginTx();
		Result result = db.execute("MATCH (res:Restaurant {id:\""
				+ restaurantId + "\"})-[:R_F]->(review) return review");
		reviews = getReviews(result);
		if (reviews.size() > 0) {
			getSummary(reviews, tmpDbPath);
		}
	}

	public void getAmbienceReviewSummarization(String restaurantId,
			String tmpDbPath) {
		List<String> reviews = new LinkedList<String>();
		Transaction ignored = db.beginTx();
		Result result = db.execute("MATCH (res:Restaurant {id:\""
				+ restaurantId + "\"})-[:R_A]->(review) return review");
		reviews = getReviews(result);

		if (reviews.size() > 0) {
			getSummary(reviews, tmpDbPath);
		}
	}

	public void getServiceReviewSummarization(String restaurantId,
			String tmpDbPath) {
		List<String> reviews = new LinkedList<String>();
		Transaction ignored = db.beginTx();
		Result result = db.execute("MATCH (res:Restaurant {id:\""
				+ restaurantId + "\"})-[:R_S]->(review) return review");
		reviews = getReviews(result);

		if (reviews.size() > 0) {
			getSummary(reviews, tmpDbPath);
		}
	}

	public void getPriceReviewSummarization(String restaurantId,
			String tmpDbPath) {
		List<String> reviews = new LinkedList<String>();
		Transaction ignored = db.beginTx();
		Result result = db.execute("MATCH (res:Restaurant {id:\""
				+ restaurantId + "\"})-[:R_P]->(review) return review");
		if (reviews.size() > 0) {
			getSummary(reviews, tmpDbPath);
		}
	}

	public HashSet<String> getUniquePhrases(List<String> phrases) {
		MyComparator comp = new MyComparator();
		java.util.Collections.sort(phrases, comp);
		HashSet<String> uniqueSet = new HashSet<String>(phrases);
		int j = 0;
		while (j <= phrases.size()) {
			for (int i = j + 1; i < phrases.size(); i++) {
				String p1 = phrases.get(j);
				String p2 = phrases.get(i);
				if (p1.contains(p2)) {
					uniqueSet.remove(p2);
				}
			}
			j++;

		}
		return uniqueSet;
	}

	public HashSet<String> getSummary(List<String> reviews, String tempDBPath) {
		List<String> summary = new LinkedList<String>();
		GraphDatabaseService dbTmp = new GraphDatabaseFactory()
				.newEmbeddedDatabase(tempDBPath);
		StringLogger s = StringLogger.wrap(new StringBuffer(""));
		ExecutionEngine execEngine = new ExecutionEngine(dbTmp, s);
		int j = 0;
		for (String review : reviews) {
			j++;
			if (j == 100) {
				break;
			}
			review = ReviewNormalizer.normalizeReview2(review);
			String[] words = review.split("\\s+");
			for (int i = 0; i < words.length - 1; i++) {
				String word1 = words[i].replaceAll("[^A-Za-z0-9]", "");
				String word2 = words[i + 1].replaceAll("[^A-Za-z0-9]", "");
				execEngine
						.execute("MERGE (w1:Word {name:\""
								+ word1
								+ "\"})"
								+ " MERGE (w2:Word {name:\""
								+ word2
								+ "\"})"
								+ " MERGE (w1)-[r:NEXT]->(w2)"
								+ " ON CREATE SET r.count = 1 ON MATCH SET r.count = r.count +1");
			}
		}

		int i = 0;
		Transaction ignored = dbTmp.beginTx();
		Result result = dbTmp
				.execute("MATCH path = (w:Word)-[:NEXT*..5]->()"
						+ " WHERE ALL (r IN rels(path) WHERE r.count > 1)"
						+ " RETURN [w IN nodes(path)| w.name] AS phrase, reduce(sum=0,r IN rels(path)| sum + r.count) as score"
						+ " ORDER BY score DESC" + " LIMIT 100");

		while (result.hasNext()) {
			Map<String, Object> row = result.next();
			for (Entry<String, Object> column : row.entrySet()) {
				if (column.getKey().equals("phrase")) {
					String value = column.getValue().toString();
					value = value.substring(1, value.length() - 1);
					value = value.replaceAll(",", "");
					if (isSignificant(value)) {
						i++;
						summary.add(value);
					}
					if (i == 10) {
						break;
					}
				}
			}

		}

		dbTmp.shutdown();
		deleteFile(new File(tempDBPath));

		HashSet<String> uniquePhrases = getUniquePhrases(summary);
		for (String str : uniquePhrases) {
			System.out.println(str);
		}
		return uniquePhrases;
	}

}
