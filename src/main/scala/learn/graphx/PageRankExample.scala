package learn.graphx

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

/**
  *  这个是用来学习 PageRank 的例子
  */

object PageRankExample {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder()
			.master("local[4]")
			.appName(s"${this.getClass.getSimpleName}")
			.getOrCreate()
		val sc = spark.sparkContext

		val graph = GraphLoader.edgeListFile(sc, "file:///E:\\idea_code\\SCALA\\SparkLearning\\src\\main\\resources\\followers.txt")
		// 得到 PageRank 向量值
		val ranks = graph.pageRank(0.0001).vertices
		// join the rank with username
		val users = sc.textFile("file:///E:\\idea_code\\SCALA\\SparkLearning\\src\\main\\resources\\users.txt").map{ line =>
			val fields = line.split(",")
			(fields(0).toLong, fields(1))
		}

		val ranksByUsername = users.join(ranks).map{
			case (id, (username, rank)) => (username, rank)
		}

		// Print the result
		println(ranksByUsername.collect().mkString("\n"))
		// $example off$
		spark.stop()
	}
}
