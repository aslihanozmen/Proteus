package main;

import test.EnrichingTest;

public class Driver {

	public static void main(String[] args) throws Exception {
		int[] colsFrom = new int[] { 0,1 };
		String file = "benchmark/functional/prettycleaned_airport_codes_internalGT2.csv";
		EnrichingTest test = new EnrichingTest();
		test.testEnrichment(file, colsFrom);
	}
}
