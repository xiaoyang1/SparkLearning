package learn.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
	def main(args: Array[String]): Unit = {
		val logFile = "file:///E:\\idea_code\\SCALA\\SparkLearning\\src\\main\\resources\\data.txt"
		val conf = new SparkConf().setMaster("local[2]").setAppName("Simple Application")
		val sc = new SparkContext(conf)
		val logData = sc.textFile(logFile, 2).cache()

		val count = logData.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
		count.foreach(println)
		println("---------------------------------------")
		count.collect().sortBy(word => word).foreach(println)

		sc.stop()
	}
}
