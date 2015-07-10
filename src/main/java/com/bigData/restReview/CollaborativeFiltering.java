package com.bigData.restReview;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class CollaborativeFiltering {
	public static HashMap<String, HashMap<String, Float>> restaurantUserHashMap;
	public static HashMap<String, String> restaurantIdMap;
	public static HashMap<String, Float> avgRating;
	public GraphDatabaseService db;

	public void setup(GraphDatabaseService db) {
		this.db = db;
		restaurantUserHashMap = new HashMap<String, HashMap<String, Float>>();
		restaurantIdMap = new HashMap<String, String>();
		avgRating = new HashMap<String, Float>();
		pouplateMap();
	}

	public void pouplateMap() {

		Transaction ignored = db.beginTx();
		Result result = db.execute("MATCH (res:Restaurant) return res");

		ResourceIterator<Object> resultIterator = result.columnAs("res");

		while (resultIterator.hasNext()) {
			Node resultNode = (Node) resultIterator.next();
			restaurantIdMap.put(resultNode.getProperty("id").toString(),
					resultNode.getProperty("name").toString());
		}

		Set<String> idSet = restaurantIdMap.keySet();
		for (String id : idSet) {
			int sum = 0;
			int cnt = 0;

			ignored = db.beginTx();
			result = db
					.execute("MATCH (u:User)-[r:RATED]->(res:Restaurant {id:\""
							+ id + "\"}) return u.name, r.count");

			while (result.hasNext()) {
				cnt++;
				Map<String, Object> row = result.next();
				String userId = row.get("u.name").toString();
				sum += Integer.parseInt(row.get("r.count").toString().trim());
				float rating = Float.parseFloat(row.get("r.count").toString()
						.trim());
				if (restaurantUserHashMap.containsKey(id)) {
					restaurantUserHashMap.get(id).put(userId, rating);
				} else {
					HashMap<String, Float> userRatingMap = new HashMap<String, Float>();
					userRatingMap.put(userId, rating);
					restaurantUserHashMap.put(id, userRatingMap);
				}
			}

			avgRating.put(id, (float) sum / (float) cnt);
		}
	}

	public List<String> findSimilarRestaurant(String rest1) {

		Set<String> restIdSet = restaurantUserHashMap.keySet();
		List<String> pq = new LinkedList<String>();

		boolean flag = false;
		for (String rest2 : restIdSet) {
			if (!rest1.equals(rest2)) {

				HashMap<String, Float> user1HashMap = restaurantUserHashMap
						.get(rest1);
				HashMap<String, Float> user2HashMap = restaurantUserHashMap
						.get(rest2);
				float average1 = avgRating.get(rest1);
				float average2 = avgRating.get(rest2);

				Set<String> userIdSet = user1HashMap.keySet();

				float weight = 0;
				float x = 0, y = 0, z = 0;
				for (String userId : userIdSet) {
					if (user2HashMap.containsKey(userId)) {
						// (Va,j - ~Va)
						float difference1 = 0;
						// (Vi,j - ~Vi)
						float difference2 = 0;
						float ratting1 = user1HashMap.get(userId);
						float ratting2 = user2HashMap.get(userId);
						difference1 = ratting1 - average1;
						difference2 = ratting2 - average2;
						x += (difference1 * difference2);
						y += (difference1 * difference1);
						z += (difference2 * difference2);
					}
				}
				if (x != 0 && y != 0 && z != 0) {
					weight = x / (float) Math.sqrt((y * z));
				}
				if (weight > 0) {

					if (weight > 0.7F) {
						pq.add(restaurantIdMap.get(rest2));
						if (pq.size() == 5) {
							flag = true;
							break;
						}
					}
				}
			}
			if (flag) {
				flag = false;
				break;
			}
		}
		return pq;
	}
}
