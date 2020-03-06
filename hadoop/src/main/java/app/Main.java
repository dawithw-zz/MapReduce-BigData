package app;

public class Main {
	
	public static void main(String[] args) throws Exception {
		
		/* 
		 * Comment/Uncomment the following lines 
		 * to run the required MapReduce program
		 * 
		 */
		
		//wordCount.WordCount.run(args);				// WordCount Algorithm
		//average.AverageValue.run(args);				// Average Algorithm
		//averageInMapper.AverageValue.run(args);		// Average In-Mapper Algorithm
		//frequencyPair.FrequencyPair.run(args);		// Pair Approach relative frequency
		//frequencyStripe.FrequencyStripes.run(args);	// Stripe Approach relative frequency
		frequencyPairStripe.PairsStripes.run(args);		// Hybrid Approach relative frequency
	}
}
