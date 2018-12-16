package learn.kafka

import org.apache.spark.sql.SparkSession

object kafkaStructuredStream {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("kafkaStructuredStream").getOrCreate()

		import spark.implicits._
		val lines = spark.readStream
    		.format("kafka")
    		.option("kafka.bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
    		.option("subscribe", "topicA,topicB")
    		.load().selectExpr("CAST(value AS STRING)")
    		.as[String]

		val checkpoint = "/spark/checkpoint/11-27/"

		val wordCount = lines.flatMap(_.split(" ")).groupBy("value").count()

		val query = wordCount.writeStream
    		.outputMode("complete")
    		.format("console")
    		.option("checkpointLocation", checkpoint)
    		.start()

		query.awaitTermination()
	}
}
