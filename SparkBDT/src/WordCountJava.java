import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCountJava {

	public static void main(String[] args) {	    
	    	    
	    if(args.length != 3){
	      System.out.println("Usage:  input output word_frequency_threshold ") ;     
	    }
	    
	    final int threshold  = Integer.parseInt(args[2]);
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

		// Load our input data
		JavaRDD<String> lines = sc.textFile(args[0]);

		// Calculate word count
		JavaPairRDD<String, Integer> words = lines.flatMap(line -> Arrays.asList(line.split(" ")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1)).reduceByKey((x, y) -> x + y)
				.filter(p -> p._2 > threshold);
		
		JavaPairRDD<String, Integer> counts = words.flatMap( x -> {
			List<String> list = new ArrayList<>();
			for(char c: x._1.toCharArray()){
				list.add(String.valueOf(c));
			}
			return list;
		})
			      .mapToPair( x -> new Tuple2<String, Integer>(x, 1))
			      .reduceByKey((x, y) -> x + y);

		// Save the word count back out to a text file, causing evaluation
		counts.saveAsTextFile(args[1]);

		sc.close();
	}

}
