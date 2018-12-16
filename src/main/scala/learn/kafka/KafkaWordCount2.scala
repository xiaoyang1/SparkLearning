package learn.kafka

import learn.sparkstream.RecoverableNetworkWordCount
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object KafkaWordCount2 {
	def main(args: Array[String]): Unit = {
		val kafkaParams = Map[String, Object] (
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "master:9092,slave1:9092,slave2:9092",
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG  -> classOf[StringDeserializer],
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
			ConsumerConfig.GROUP_ID_CONFIG -> "demo1",
			ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
			ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> "30000"
		)

		val topic  = Array("topicA", "topicB")

		val sparkConf = new SparkConf().setAppName("KafkaDemo");
		val ssc = new StreamingContext(sparkConf, Seconds(3))
		val checkpointPath = "/spark/checkpoint"
		ssc.checkpoint(checkpointPath)
		RecoverableNetworkWordCount.deleteCheckpointDirectory(checkpointPath, ssc.sparkContext)
		val messageStream = KafkaUtils.createDirectStream[String, String](
			ssc,
			LocationStrategies.PreferBrokers,
			ConsumerStrategies.Subscribe[String, String](topic, kafkaParams)
		)

		val addfunc =  (values: Seq[Int], state: Option[Int]) => {
			val currentCount = values.sum
			val previous = state.getOrElse(0)
			Some(currentCount + previous)
		}

		val line = messageStream.map(record => record.value())
		val words = line.flatMap(value => value.trim.split(" "))
		val wordCount = words.map(x => (x, 1)).updateStateByKey(addfunc)

		wordCount.print()
		ssc.start()
		ssc.awaitTermination()
	}
}
