package learn.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {
	def main(args: Array[String]): Unit = {
		if (args.length < 2) {
			System.err.println("Usage: NetworkWordCount <hostname> <port>")
			System.exit(1)
		}

		val sparkConf = new SparkConf().setAppName("NetworkWordCount")
		val ssc = new StreamingContext(sparkConf, Seconds(10))
		//使用updateStateByKey前需要设置checkpoint
		ssc.checkpoint("hdfs://master:9000/spark/checkpoint")

		val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
			//通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
			val currentCount = currValues.sum
			// 已累加的值
			val previousCount = prevValueState.getOrElse(0)
			// 返回累加后的结果，是一个Option[Int]类型
			Some(currentCount + previousCount)
		}

		//val currWordCounts = pairs.reduceByKey(_ + _)
		//currWordCounts.print()

		val lines = ssc.socketTextStream(args(0), args(1).toInt)

		val words = lines.flatMap(_.split(" "))
		val pairs = words.map(word => (word, 1))

		val totalWordCounts = pairs.updateStateByKey[Int](addFunc)
		totalWordCounts.print()

		ssc.start()
		ssc.awaitTermination()


	}
}
