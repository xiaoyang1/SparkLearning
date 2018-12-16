package learn.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object QueueStream {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(20))
//		val rddQueue = new ConcurrentLinkedQueue[RDD[Int]]()
		val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()
		val queueStream = ssc.queueStream(rddQueue)

		val mappedStream = queueStream.map(r => (r % 10, 1))

		val reduceStream = mappedStream.reduceByKey(_ + _)
		reduceStream.print()

		ssc.start()
		for (i <- 1 to 10){
			rddQueue += ssc.sparkContext.makeRDD(1 to 100,2)
			// 两者都是等价
//			rddQueue += ssc.sparkContext.parallelize(1 to 100, 2)
			Thread.sleep(1000)
		}
		ssc.stop()
	}
}
