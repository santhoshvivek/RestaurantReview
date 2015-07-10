package com.bigData.restReview;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
import opennlp.tools.util.InvalidFormatException;

public class DataClassifier {
	
	/**
	 * Method to get the classification model
	 * @return
	 * @throws InvalidFormatException
	 * @throws IOException
	 */
	public DoccatModel getClassificationModel() throws InvalidFormatException, IOException{
		ClassLoader classLoader = getClass().getClassLoader();
		File classificationModelFile = new File(classLoader.getResource("model").getFile());
		InputStream is = new FileInputStream(classificationModelFile.getAbsoluteFile());
		DoccatModel classificationModel = new DoccatModel(is);
		return classificationModel;
	}
	/**
	 * Method used to associate the text with one of the available classes
	 * @param text
	 * @return
	 * @throws InvalidFormatException
	 * @throws IOException
	 */
	public static String classifyText(DoccatModel classificationModel, String text)
			throws InvalidFormatException, IOException {
		DocumentCategorizerME classificationME = new DocumentCategorizerME(
				classificationModel);
		String documentContent = text;
		double[] classDistribution = classificationME
				.categorize(documentContent);
		String predictedCategory = classificationME
				.getBestCategory(classDistribution);
		return predictedCategory;
	}
}
