

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions



object IPCountOfLog extends App{
  override def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("IPCountOfLog").setMaster("local")
    val sc = new SparkContext(conf)

    var file = sc.textFile("file:///home/cloudera/access_log")

    var h = file.map( line => (getIp(line), 1) ).reduceByKey(_ + _).sortBy(_._2, false)
    
    var f = new String("/home/cloudera/ip_count_of_log_output")    
    FileUtils.deleteDirectory(new File(f))

    
    h.saveAsTextFile("file://" + f)
    h.foreach(println)
    
  }

  def getIp(line: String): String = {
    var array = line.split(" ")
    return array(0)
  }
}