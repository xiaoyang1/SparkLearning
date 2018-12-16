package learn.sparkstream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object SqlNetworkWordCount {
	def main(args: Array[String]): Unit = {
		if (args.length < 2) {
			System.err.println("Usage: NetworkWordCount <hostname> <port>")
			System.exit(1)
		}

		Logger.getRootLogger.setLevel(Level.WARN)
		val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount")
		val ssc = new StreamingContext(sparkConf, Seconds(2))

		val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
		val words = lines.flatMap(x => x.trim.split(" "))

		words.foreachRDD{(rdd: RDD[String], time: Time) => {
			// 注意，千万不要用spark， 就和网络的connection一样，会序列化对象发到worker，会出问题的。
			val spark = SparkSessionSingleton.getInsatnce(rdd.sparkContext.getConf)
			import spark.implicits._

			// Convert RDD[String] to RDD[case class] to DataFrame
			val wordsDataFrame = rdd.map(w => Record(w)).toDF()
			wordsDataFrame.createOrReplaceTempView("words")


			val wordCountDataFrame = spark.sql("select word, count(*) from words group by word")
			println(s"========= $time =========")
			wordCountDataFrame.show()
		}}
		// 由于上述的foreach循环不需要存储，所以没碰到那个0 peer 的WARN

		ssc.start()
		ssc.awaitTermination()
	}

	case class Record(word: String)

	object SparkSessionSingleton {

		@transient private var instance: SparkSession = _

		def getInsatnce(sparkConf: SparkConf) : SparkSession = {
			if(instance == null){
				instance = SparkSession.builder()
    					.config(sparkConf)
    					.getOrCreate()
			}
			instance
		}
	}
}
