import java.io.PrintWriter
import java.io.File;

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object LogAnalysisScala {

  def main(args: Array[String]) : Unit = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("LogAnalysis")
      .setMaster("local")
    val sc = new SparkContext(conf)

    var file = sc.textFile(args(0))

    var lines = file.map(line => line.split(" "))
    
    lines.persist();
    
    val p = new PrintWriter(new File(args(1)));  
    
    val range = lines.filter(x => timeRange(x(3)) ).filter(x => x(8).toInt == 200)    
    var cnt = range.map(x=> 1).reduce(_ + _)  
    
    println(cnt)
    p.println(cnt+"");
    
    println("------------------------------------------------")
    val ip_cnt  = lines.map(x => (x(0), 1)).reduceByKey(_ + _).sortBy( p => p._2, false, 1)
    
    val col = ip_cnt.collect();
    
    col.foreach( x => {println(x); p.println(x.toString()); })    
    
    p.close
    sc.stop    
    
   
 
  }

  
  def timeRange(x : String ): Boolean ={
    return x.compareTo("[07/Mar/2004:16:00") > 0   &&  x.compareTo("[07/Mar/2004:17:00") < 0
  }
}