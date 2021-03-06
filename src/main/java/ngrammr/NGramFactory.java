package ngrammr;

import java.util.ArrayList;
import java.util.Arrays;

public class NGramFactory {

	public NGramFactory() {

	}

	public static String[] cleanText(String input) {
		String[] words = input.replaceAll("[^a-zA-Z ]", "").toLowerCase()
				.split("\\s+");

		return words;
	}

	public static ArrayList<String> grams(String input, int g, int minG) {
		String[] words = cleanText(input);
		ArrayList<String> gramList = new ArrayList<String>();

		/*
		 * for each i in GRAM_N, for each gram of length i in words, join the
		 * array subset of length i starting at index j and add to gramList
		 */
		for (int i = g; i >= minG; i--) {
			for (int j = 0; j + i <= words.length; j++) {
				gramList.add(String.join(" ",
						Arrays.copyOfRange(words, j, j + i)));
			}
		}
		return gramList;
	}
}
