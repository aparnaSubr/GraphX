import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object GraphXQuery1 {

def main(args: Array[String]) {
        if (args.length < 1) {
            System.err.println("Usage: GraphXQuery1 <src-folder>")
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
        
        val fileRDD = spark.sparkContext.wholeTextFiles("hdfs:///user/ubuntu/words")
        val vertices = fileRDD.map{x =>
                                   val key = x._1.split("/words/")(1).split(".txt")(0)
                                   (key, x._2.split("\n"))}

        val wordsRDD = vertices.flatMapValues(x => x).distinct()

        val words = wordsRDD.map{x =>
                                 (x._2, x._1)}.distinct().groupByKey()

        val pairs  = words.map{x =>
                         val arr = x._2.toSeq
                         val combos = for {
                                            a <- arr
                                            b <- arr
                                            if(a != b)
                                           } yield((a,b))
                         (x._1, combos)}.flatMapValues(x => x).distinct()

        val edges = pairs.map{x =>
                              ((x._2._1, x._2._2), x._1)}.groupByKey()

        val defaultVertex = ("default_vertex")
        val v = vertices.map{x =>
                             val id = x._1.split("words_")(1).toLong
                             (id, x._2.size.toLong)}.collect().toSeq
        val vertexRDD: RDD[(VertexId, Any)] = spark.sparkContext.parallelize(v)
        val e = edges.map{x =>
                          Edge(x._1._1.split("words_")(1).toLong, x._1._2.split("words_")(1).toLong, x._2)}.collect().toSeq
        val edgeRDD: RDD[Edge[Iterable[String]]] = spark.sparkContext.parallelize(e)

        val graph = Graph(vertexRDD, edgeRDD, defaultVertex)

        val edgeCount = graph.triplets.filter{x => x.srcAttr.asInstanceOf[Long] > x.dstAttr.asInstanceOf[Long]}.count()


        spark.stop()

    }
}
