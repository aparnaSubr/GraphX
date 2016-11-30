import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.GraphLoader
import scala.reflect.ClassTag
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.{Vector, Vectors}

object GraphXPageRank {

def runPageRank[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15): Graph[Double, Double] =
    {
        val src: VertexId = (-1L)

        // Initialize the PageRank graph with each edge attribute having
        // weight 1/outDegree and each vertex with attribute resetProb.
        // When running personalized pagerank, only the source vertex
        // has an attribute resetProb. All others are set to 0.
        var rankGraph: Graph[Double, Double] = graph
          // Associate the degree with each vertex
          .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
          // Set the weight on the edges based on the degree
          .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
          // Set the vertex attributes to the initial pagerank values
          .mapVertices { (id, attr) =>
            if (!(id != src)) resetProb else 0.0}
    
        def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }
        
        var iteration = 0
        var prevRankGraph: Graph[Double, Double] = null
        while (iteration < numIter) {
          rankGraph.cache()
        
          // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
          // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
          val rankUpdates = rankGraph.aggregateMessages[Double](
            ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)
        
          // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
          // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
          // edge partitions.
          prevRankGraph = rankGraph
          val rPrb = (src: VertexId, id: VertexId) => resetProb
        
          rankGraph = rankGraph.joinVertices(rankUpdates) {
            (id, oldRank, msgSum) => rPrb(src, id) + (1.0 - resetProb) * msgSum
          }.cache()
        
          rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
          prevRankGraph.vertices.unpersist(false)
          prevRankGraph.edges.unpersist(false)
        
          iteration += 1
        }
    
    rankGraph
}

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

        val graph = GraphLoader.edgeListFile(spark.sparkContext, args(0))
        val ranks = runPageRank(graph, iterations, 0.15).vertices

        var ranksByNode = ranks.collect()
        spark.stop()

        ranksByNode.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    }
}
