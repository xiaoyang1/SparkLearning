package learn.graphx

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession

/**
  *  这个主要是用来学习 Pregel 操作来表达计算， 比如说 单点 源最短路径问题。
  */

object SSSPExample {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder()
    		.master("local[4]")
    		.appName(s"${this.getClass.getSimpleName}")
    		.getOrCreate()

		val sc = spark.sparkContext

		// 获得随机图, 原来的边是int， 要转换为double
		val graph: Graph[Long, Double] = GraphGenerators.logNormalGraph(sc, numVertices = 100)
    		.mapEdges(e => e.attr.toDouble)

		val sourceId: VertexId = 42  // 选出最终的源 source
		// 初始化图，使除root根之外的所有顶点都具有距离无穷大。

		val initialGraph = graph.mapVertices((id, _) => if(id == sourceId) 0.0 else Double.PositiveInfinity)
		// 最短路径
		val sssp = initialGraph.pregel(Double.PositiveInfinity)(
			// 每次reduce玩都要执行一次message之后，都会graph.joinVertex(message), 还会调用一次该节点函数
			(id, distance, newDist) => math.min(distance, newDist), // vertex program
			// map 函数， 如果有更近的路径，就发送消息
			triplet => {
				if(triplet.srcAttr + triplet.attr < triplet.dstAttr){
					Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
				} else {
					Iterator.empty
				}
			},
			(a, b) => math.min(a, b) // reduce  merge 函数， 找出最短的路径
		)

		println(sssp.vertices.collect().mkString("\n"))
		spark.stop()
	}
}
