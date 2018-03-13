import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class LogAnalysisJava {

	public static void main(String[] args) throws FileNotFoundException {	    
	    
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("LogAnalysis").setMaster("local"));

		// Load our input data
		JavaRDD<String> file = sc.textFile(args[0]);
		
		JavaRDD<String[]> lines = file.map(line -> line.split(" "));
		lines.persist(StorageLevel.MEMORY_ONLY() );
		
		PrintWriter p = new PrintWriter(new File(args[1]));  

		JavaRDD<String[]>  range = lines.filter(x -> timeRange(x[3]) ).filter(x -> Integer.parseInt(x[8])  == 200) ;
        
	    int cnt = range.map(x-> 1).reduce((x, y) -> x + y)  ;
	    
	    System.out.println(cnt);
	    p.println(cnt+"");
	    
	    System.out.println("------------------------------------------------");
	    JavaPairRDD<String, Integer> ip_cnt  = lines.mapToPair((String[] x) -> new Tuple2<String, Integer>(x[0], 1))
	    		.reduceByKey((x, y) -> x + y);	   	   
	    
	    JavaRDD<Tuple2<String, Integer>> sorted = ip_cnt.map(x -> x).sortBy(x->x._2, false, 1);
	    
	    List<Tuple2<String, Integer>> col = sorted.collect();
	    
	    col.forEach( x->{System.out.println(x.toString()); p.println(x.toString()); });
	    
	    p.close();
		sc.close();
	}
	
	 static boolean timeRange( String x) {
		 return x.compareTo("[07/Mar/2004:16:00") > 0   && x.compareTo("[07/Mar/2004:17:00") < 0 ;
	 }

}
