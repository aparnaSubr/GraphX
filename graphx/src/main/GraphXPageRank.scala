import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphXPageRank {

def main(args: Array[String]) {
        if (args.length < 1) {
            System.err.println("Usage: GraphXPageRank <file> <iter>")
            System.exit(0)
        }
        
        val spark = SparkSession
        .builder
        .master("spark://10.254.0.83:7077")
        .appName("GraphXPageRank")
        .config("spark.driver.memory", "5g")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///home/ubuntu/logs/spark")
        .config("spark.executor.memory", "15g")
        .config("spark.executor.cores", "4")
        .config("spark.task.cpus", "1")
        .config("spark.default.parallelism", "20")
        .getOrCreate()
        
        val iterations = if (args.length > 1) args(1).toInt else 20

        val lines = spark.read.textFile(args(0)).rdd

        val edgeRDD = lines.map{ s.toLong => 
                                val parts = s.split("\\s+")
                                (parts(0).toLong, parts(1).toLong) 
                                }.distinct().groupByKey().cache()
        
        var vertexRDD = edgeRDD.mapValues(v => 1.0)
        
        //for (i <- 1 to iters) {
        //    val contribs = links.join(ranks).values.flatMap{ case(urls, rank) => 
        //    val size = urls.size 
        //    urls.map(url => (url, rank / size)) }
        //    ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
        //}
       
        val output = edgeRDD.take(5)
        
          
        spark.stop()
        output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    }
}
