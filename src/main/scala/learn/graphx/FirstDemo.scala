package learn.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object FirstDemo {

	def mapUDF(id: VertexId, attr: (String, String)) = ???

	def main(args: Array[String]): Unit = {
		val userGraph: Graph[(String, String), String] = null

		val sparkConf = new SparkConf().setMaster("local[3]")setAppName("Graph First Demo")
		val sc = new SparkContext(sparkConf)

		val users: RDD[(VertexId, (String, String))] = sc.parallelize(
			Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
				(5L, ("franklin", "prof")), (2L, ("istoica", "prof")))
		)

		val relationships: RDD[Edge[String]] = sc.parallelize(
			Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"), Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"))
		)
		// 定义默认用户，以防与缺少用户有关系
		val defaultUser = ("John Doe", "missing")
		val graph = Graph(users, relationships, defaultUser)

		graph.edges.filter(x => x.srcId > x.dstId).count()
		graph.vertices.filter{case (id, (name, pos)) => pos == "postdoc"}


		// 关系三元组 edge triplet view
		val triplet = graph.triplets
		val facts: RDD[String] = triplet.map(triplet1 => triplet1.srcAttr._1 + " is the " + triplet1.attr + " of " + triplet1.dstAttr._1)
		facts.collect().foreach(println(_))

		val indegree: VertexRDD[Int] = graph.inDegrees

		// 以下两个操作是等价的，但第一个不保留结构索引，也不会受益于GraphX系统优化：
//		val newVertices = graph.vertices.map{case (id, attr) => (id, mapUDF(id, attr))}
//		val newGraph = Graph(newVertices, graph.edges)
//
//		val newGraph1 = graph.mapVertices((id, attr) => (id, mapUDF(id, attr)))

		// 删除缺少的顶点以及连接到它们的边
		val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "missing")
		validGraph.vertices.collect().foreach(println)

		validGraph.triplets.map(
			triplet1 => triplet1.srcAttr._1 + " is the " + triplet1.attr + " of " + triplet1.dstAttr._1
		).collect.foreach(println(_))

		// 以下两者是等价的
		graph.mapVertices((id, b) => b._1 + b._2)
//		graph.mapVertices{case (id, (attr1, attr2)) => attr1 + attr2}
		sc.stop()


	}
}
