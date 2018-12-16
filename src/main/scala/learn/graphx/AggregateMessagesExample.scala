package learn.graphx

import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession


/**
  *  这个例子主要是用来学习 aggregateMessages 方法， 统计所有顶点的近邻，大于自己年龄的平均年龄
  */
object AggregateMessagesExample {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder()
			.appName(s"${this.getClass.getSimpleName}")
			.master("local[4]")
			.getOrCreate()

		val sc = spark.sparkContext

		// 创建一个图，顶点属性为age， 为了简便，使用随机图, 100 个顶点
		val graph: Graph[Double, Int] = GraphGenerators.logNormalGraph(sc, numVertices = 100)
			.mapVertices((id, _)=> id.toDouble)

		// 计算较老的粉丝数量及其总年龄
		val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
			triplet => { // Map 函数,
				if(triplet.srcAttr > triplet.dstAttr){
					// 将消息发送到包含计数器和年龄的目标顶点
					// 相当于context.write(key, value)只不过，key已经是dst节点，value就是msg
					triplet.sendToDst((1, triplet.srcAttr))
				}
			},
			(a, b) => (a._1 + b._1, a._2 + b._2) // reduce函数， 统计人数和age。
		)

		val avgAgeOfOlderFollowers: VertexRDD[Double] = olderFollowers.mapValues(
			(id, value) => value match {
				case (count, totalAge) => totalAge/count
			}
		)
		avgAgeOfOlderFollowers.collect().foreach(println)
		sc.stop()
	}
}
