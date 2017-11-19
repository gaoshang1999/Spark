



import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.collection.Seq



object TfIdf extends App{
  
  override def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("IPCountOfLog").setMaster("local")
    val sc = new SparkContext(conf)
    
	
    var fs = Array("file:///home/cloudera/Wikicorpus/1.txt", "file:///home/cloudera/Wikicorpus/2.txt",
                    "file:///home/cloudera/Wikicorpus/3.txt")    
    var D = fs.length;    
    var unique_words_in_corpus = sc.parallelize(Seq(""))
    
    for( f <- fs ){   
      var file = sc.textFile(f)      
      var words = file.flatMap(line => line.split(" ")).map(w=>w.trim().toLowerCase()).filter(w=> w.matches("\\w+"))      
      val pairs = words.map(word => (word, 1)).reduceByKey(_ + _).sortByKey()      
      var total = pairs.values.reduce(_ + _)      
      var tf = pairs.map(p => (p._1, p._2*1.0/total ) )      
      var tf_file = f+"-tf" 
      new java.io.File(tf_file).deleteOnExit()
      tf.saveAsTextFile( tf_file)
      
      tf.foreach(println)      
      unique_words_in_corpus = unique_words_in_corpus.union(words.distinct());
    
    }
    
    var df = unique_words_in_corpus.map(word => (word, 1)).reduceByKey(_ + _).sortByKey()
    var df_file = "file:///home/cloudera/Wikicorpus/df"
    new java.io.File(df_file).deleteOnExit()
    df.saveAsTextFile( df_file)
    
    var idf = df.map( p => (p._1, Math.log( (D + 1 ) /p._2 + 1.0 ))  )	
    
    for( f <- fs ){   
        var file = sc.textFile(f+"-tf")
        var tf = file.map(line => {val l = line.substring(1, line.length()-1).split(",");  (l(0), l(1)); })
        
        var tfidf =  tf.join(idf).map( p  => (p._1, p._2._1.toDouble * p._2._2)).sortByKey()
		
    		var tfidf_file = f+"-tfidf"
    		new java.io.File(tfidf_file).deleteOnExit()
    		tfidf.saveAsTextFile( tfidf_file)
        
     }

 
    
  }

  
 
}