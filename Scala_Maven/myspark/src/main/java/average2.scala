import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.PrintWriter
import java.io.File

object average2 extends App {
  override def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("average").setMaster("local")
    val sc = new SparkContext(conf)

    var file = sc.textFile("file:///home/cloudera/access_log")

    var lines = file.map(line => line.split(" ")).filter(x => x.length == 10).filter(x => x(9).matches("\\d+"))  
    var sum_cnt = lines.map(x => (x(0), (Integer.parseInt(x(9)).toLong , 1))).
                      reduceByKey( (x, y) => (x._1  + y._1 , x._2+ y._2)) 
     
    var avg = sum_cnt.mapValues(x =>  x._1.toFloat / x._2).sortByKey() 

    val writer = new PrintWriter(new File("output2.txt"))
    val res = avg.collect()
    for (n <- res) writer.println(n.toString())

    writer.close()

  }
} 