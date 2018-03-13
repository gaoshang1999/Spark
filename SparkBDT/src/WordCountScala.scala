import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.PrintWriter
import java.io.File

object WordCountScala {

  def main(args: Array[String]) : Unit = {
    var threshold = 0
    
    if(args.length != 3){
      println("Usage:  input output word_frequency_threshold ")      
    }else{
      threshold = args(2).toInt
    }

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    //Read some example file to a test RDD
    val lines = sc.textFile(args(0))

    lines.flatMap (line => line.split(" ") )
      .map ( word => (word, 1) )
      .reduceByKey(_ + _)
      .filter( x => x._2 > threshold )
      .flatMap( x => x._1.toCharArray())
      .map( x => (x, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(args(1))  

    sc.stop
  }

}