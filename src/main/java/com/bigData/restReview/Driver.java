package com.bigData.restReview;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Driver {

	public static void main(String args[]) throws IOException{
		if(args.length!=1){
			System.out.println("Please provide the neo4j database folder path");
			System.exit(0);
		}
		String dbPath = args[0];
		GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		ReviewSummarization testNeo4j = new ReviewSummarization();
    	testNeo4j.setup(db);
    	CollaborativeFiltering colFilter = new CollaborativeFiltering();
	    colFilter.setup(db);
	    
	    BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(System.in));
	    
	    //n5eQnMnVVt3FfrFENYoU0g
	    while (true) {
			System.out.println("\nEnter the restaurant id...");
			String input = bufferedReader.readLine();
			if (input != null && !input.isEmpty()) {
				
				System.out.println("\n\nSummarizing the reviews for this restaurant...");
				input = input.trim();
				testNeo4j.getPriceReviewSummarization(input,dbPath+File.separator+"t1.db");
		    	testNeo4j.getFoodReviewSummarization(input,dbPath+File.separator+"t2.db");
		    	testNeo4j.getAmbienceReviewSummarization(input,dbPath+File.separator+"t3.db");
		    	testNeo4j.getServiceReviewSummarization(input,dbPath+File.separator+"t4.db");
		    	
		    	System.out.println("\n\nFinding similar restaurants...");
			    List<String> similarRestaurants = colFilter.findSimilarRestaurant(input);
			    for(String similarRestaurant: similarRestaurants){
			    	System.out.println(similarRestaurant);
			    }
			}
			System.out.println("\nEnter '1' to continue...");
			input = bufferedReader.readLine();
			if (!input.trim().equals("1")) {
				break;
			}
		}

    	



	}
}
