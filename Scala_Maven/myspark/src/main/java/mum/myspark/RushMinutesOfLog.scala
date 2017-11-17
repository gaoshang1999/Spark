package mum.myspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.io.File
import org.apache.commons.io.FileUtils



object RushMinutesOfLog extends App{
  override def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RushMinutesOfLog").setMaster("local")
    val sc = new SparkContext(conf)

    var file = sc.textFile("file:///home/cloudera/access_log_input")

    var h = file.map( line => (getMinute(line), 1) ).reduceByKey(_ + _).sortBy(_._2, false)
    
    var f = new String("/home/cloudera/rush_minute_of_log_output")    
    FileUtils.deleteDirectory(new File(f))

    
    h.saveAsTextFile("file://" + f)
    h.foreach(println)
    
  }

  def getMinute(line: String): String = {
    var p = Pattern.compile("\\d+/\\w+/\\d+:(\\d+:\\d)");
    var m = p.matcher(line);
    if (m.find()) {
      return m.group(1)+"0:00";
    } else {
      return "";
    }

  }
}