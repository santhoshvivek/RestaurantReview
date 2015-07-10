package com.bigData.restReview;

public class ReviewNormalizer {

	
	public static String normalizeReview(String review){
		review =  review.toLowerCase();
		review = review.replaceAll("[-]", "");
		review = review.replaceAll("won't", "would not");
		review = review.replaceAll("can't", "cannot");
		review = review.replaceAll("wont", "would not");
		review = review.replaceAll("cant", "cannot");
		review = review.replaceAll("n't", " not");
		review = review.replaceAll("'d", " would");
		review = review.replaceAll("'s", " is");
		review = review.replaceAll("'re", " are");
		review = review.replaceAll("'ll", " will");
		review = review.replaceAll("[^A-Za-z0-9-\\.]", " ");
		review = review.replace("\\s+", " ");
		return review;
	}
	
	public static String normalizeReview2(String review){
		review = review.trim();
		review = review.replaceAll(" of ", " ");
		review = review.replaceAll(" at ", " ");
		review = review.replaceAll(" to ", " ");
		review = review.replaceAll(" does ", " ");
		review = review.replaceAll(" do ", " ");
		review = review.replaceAll(" did ", " ");
		review = review.replaceAll(" us ", " ");
		review = review.replaceAll(" i ", " ");
		review = review.replaceAll(" you ", " ");
		review = review.replaceAll(" with ", " ");
		review = review.replaceAll(" ve ", " have ");
		review = review.replaceAll(" have ", " ");
		review = review.replaceAll(" had ", " ");
		review = review.replaceAll(" will ", " ");
		review = review.replaceAll(" been ", " ");
		review = review.replaceAll(" in ", " ");
		review = review.replaceAll(" be ", " ");
		review = review.replaceAll(" am ", " ");
		review = review.replaceAll(" my ", " ");
		review = review.replaceAll(" this ", " ");
		review = review.replaceAll(" that ", " ");
		review = review.replaceAll(" they ", " ");
		review = review.replaceAll(" was ", " ");
		review = review.replaceAll(" were ", " ");
		review = review.replaceAll(" as ", " ");
		review = review.replaceAll(" is ", " ");
		review = review.replaceAll(" are ", " ");
		review = review.replaceAll(" and ", " ");
		review = review.replaceAll(" other ", " ");
		review = review.replaceAll(" but ", " ");
		review = review.replaceAll(" it ", " ");
		review = review.replaceAll(" a ", " ");
		review = review.replaceAll(" an ", " ");
		review = review.replaceAll(" the ", " ");
		review = review.replaceAll(" ca ", " can ");
		review = review.replaceAll(" wo ", " would ");
		review = review.replaceAll(" can ", " ");
		review = review.replaceAll(" would ", " ");
		review = review.replaceAll(" bit ", " ");
		review = review.replaceAll(" so ", " ");
		if(review.startsWith("the") && review.length()>4){
			review = review.substring(4);
		}else if(review.startsWith("i") &&  review.length()>2){
			review = review.substring(2);
		}else if(review.startsWith("a") && review.length()>2){
			review = review.substring(2);
		}else if(review.startsWith("an") && review.length()>3){
			review = review.substring(3);
		}
		review = review.trim();
		return review;
	}
}
