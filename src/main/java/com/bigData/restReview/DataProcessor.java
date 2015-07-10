package com.bigData.restReview;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.text.BreakIterator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.util.InvalidFormatException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.cypher.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.StringLogger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DataProcessor implements Serializable{
	/**
	 * 
	 */
	
	private static final String DB_PATH = "/Users/kumaran_rajendiran/Desktop/temp1.db";
	private static final long serialVersionUID = -7233451631928465633L;
	private final static String BUSINESS_ID = "business_id";
	private final static String USER_ID = "user_id";
	private final static String CATEGORIES = "categories";
	private final static String NAME = "name";
	private final static String TEXT = "text";
	private final static String STARS = "stars";
	private static final String FOOD = "food";
	private static final String AMBIENCE = "ambience";
	private static final String SERVICE = "service";
	private static final String PRICE = "price";
	
	public static JavaSparkContext sc;
	public static ExecutionEngine execEngine;
	public static Map<String,String> restaurants;
	public static DoccatModel docCategorizationModel; 
	
	public void setup() throws InvalidFormatException, IOException{
		sc = new JavaSparkContext(
				"local",
				"App",
				"/path/to/sparkhome",
				new String[] { "target/com.bigData.restReview-0.0.1-SNAPSHOT.jar" });
		restaurants = new HashMap<String, String>();
		DataClassifier dataClassifier = new DataClassifier();
		docCategorizationModel = dataClassifier.getClassificationModel();
		GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		StringLogger s = StringLogger.wrap(new StringBuffer("")) ;
        execEngine = new ExecutionEngine(db, s);
	}
	
	public List<String> tokenizeText(String text) {
		BreakIterator iterator = BreakIterator.getSentenceInstance(Locale.US);
		List<String> sentences = new LinkedList<String>();
		iterator.setText(text);
		int start = iterator.first();
		for (int end = iterator.next(); end != BreakIterator.DONE; start = end, end = iterator
				.next()) {
			sentences.add(text.substring(start, end));
		}
		return sentences;
	}
	
	public void populateDatabase(String userId, String restaurantId, String review, String stars) throws InvalidFormatException, IOException{
		review = ReviewNormalizer.normalizeReview(review);
		List<String> statements = tokenizeText(review);
		ExecutionResult execResult = execEngine.execute("MERGE (user:User {name:\""+userId+"\"})"
				+" MERGE (restaurant:Restaurant {id:\""+restaurantId+"\",name:\""+restaurants.get(restaurantId)+"\"})"
				+" MERGE (user)-[r:RATED]->(restaurant)"
				+" ON CREATE SET r.count = "+stars.trim());
		
		for(String statement: statements){
			String category = DataClassifier.classifyText(docCategorizationModel, statement);
			String classValue= "";
			if (category.equals(FOOD)) {
				classValue = "F";
			} else if (category.equals(AMBIENCE)) {
				classValue = "A";
			} else if (category.equals(SERVICE)) {
				classValue ="S";
			} else if (category.equals(PRICE)) {
				classValue ="P";
			}
			if(!classValue.isEmpty()){
				execResult = execEngine.execute("MATCH (res:Restaurant) WHERE res.id=\""+restaurantId+"\" "
						+ " MERGE (review:Review {value:\""+statement+"\"})"
						+ " MERGE (res)-[r:R_"+classValue+"]->(review)");
			}
			
		}
		
	}
	
	public void popualteListOfRestaurants(String id, String name){	
		restaurants.put(id,name);
	}
	
	public void popualteListOfRestaurantsWrapper() {		
		ClassLoader classLoader = getClass().getClassLoader();
		File businessInfofile = new File(classLoader.getResource("business.json").getFile());	
		JavaRDD<String> businessInfoData = sc.textFile(businessInfofile.getAbsolutePath())
				.cache();
		businessInfoData.filter(new Function<String, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -2939415444640896882L;

			public Boolean call(String s) {
				JsonElement jelement = new JsonParser().parse(s);
				JsonObject jobject = jelement.getAsJsonObject();
				JsonElement businessIdElement = jobject.get(BUSINESS_ID);
				JsonElement nameElement = jobject.get(NAME);
				JsonArray categoriesArray = jobject.get(CATEGORIES)
						.getAsJsonArray();
				for (int iterator = 0; iterator < categoriesArray.size(); iterator++) {
					if (categoriesArray.get(iterator).getAsString()
							.toLowerCase().contains("restaurants")
							|| categoriesArray.get(iterator).getAsString()
									.toLowerCase().contains("nightlife")) {
						popualteListOfRestaurants(businessIdElement.getAsString(),nameElement.getAsString());	
						return true;
					}
				}
				return false;
			}
		}).count();
	}

	public void processReviews() {
		ClassLoader classLoader = getClass().getClassLoader();
		File reviewfile = new File(classLoader.getResource("review.json").getFile());
		JavaRDD<String> reviewData = sc.textFile(reviewfile.getAbsolutePath())
				.cache();
		reviewData.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -2293343791697648250L;

			public Boolean call(String s) throws InvalidFormatException, IOException {
				JsonElement jelement = new JsonParser().parse(s);
				JsonObject jobject = jelement.getAsJsonObject();
				JsonElement businessIdElement = jobject.get(BUSINESS_ID);
				if (restaurants.containsKey(businessIdElement.getAsString())) {
					JsonElement starsElement = jobject.get(STARS);
					JsonElement reviewElement = jobject.get(TEXT);
					JsonElement userIdElement = jobject.get(USER_ID);
					populateDatabase(userIdElement.getAsString(),
							businessIdElement.getAsString(), 
							reviewElement.getAsString(), 
							starsElement.getAsString());
					return true;
				} 
				return false;
			}
		}).count();
	}


}
