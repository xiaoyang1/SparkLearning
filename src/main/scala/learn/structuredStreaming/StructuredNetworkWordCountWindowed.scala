package learn.structuredStreaming

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object StructuredNetworkWordCountWindowed {
	def main(args: Array[String]): Unit = {
		if (args.length < 3) {
			System.err.println("Usage: StructuredNetworkWordCountWindowed <hostname> <port>" +
				" <window duration in seconds> [<slide duration in seconds>]")
			System.exit(1)
		}
		Logger.getRootLogger.setLevel(Level.WARN)
		val host = args(0)
		val port = args(1).toInt
		val windowSize = args(2).toInt
		val slideSize = if(args.length == 3) windowSize else args(3).toInt

		if (slideSize > windowSize) {
			System.err.println("<slide duration> must be less than or equal to <window duration>")
			System.exit(0)
		}

		val windowDuration = s"$windowSize seconds"
		val slideDuration = s"$slideSize seconds"

		val spark = SparkSession.builder().appName("StructuredNetworkWordCountWindowed").getOrCreate()
		import spark.implicits._

		val lines = spark.readStream.format("socket")
    		.option("host", host)
    		.option("port", port)
    		.option("includeTimeStamp", true)
    		.load()


		val words = lines.as[(String, Timestamp)]
			.flatMap(line => line._1.split(" ")
			.map(word => (word, line._2)))
    		.toDF("word", "timestamp")
//		words.withColumn("temp", new Column())
		// 必须加 $ 符号，，这个会自定义解析为列column，
		val windowedCounts = words.groupBy(
			window($"timestamp", windowDuration, slideDuration), $"word"
		).count().orderBy("window")

		// 指定watermarking， event time 在十分钟之后不再接受延迟数据late data
		val windowedCountsWaterMarking = words
			.withWatermark("timestamp", "10 minutes")
    		.groupBy(window($"timestamp", windowDuration, slideDuration), $"word")
    		.count().orderBy("window")



		val query = windowedCounts.writeStream
    		.outputMode("complete")
    		.format("console")
    		.option("truncate", "false")
    		.start()

		
		query.awaitTermination()
	}
}
