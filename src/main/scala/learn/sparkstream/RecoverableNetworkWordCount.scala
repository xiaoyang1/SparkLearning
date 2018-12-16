package learn.sparkstream

import java.io.File
import java.net.URI
import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator


/**
  *  用来学习 checkpoint, 顺便学习广播对象， 和累计器
  *  注意：广播变量和累加器是不能通过checkpoint来恢复的。
  */

object WordBlacklist {
	@volatile private var instance : Broadcast[Seq[String]] = null
	// 使用单例模式配置广播变量
	// 广播变量保证一台机器只会有一份副本，而不是随着task的数目增加
	// 不能将RDD广播出去，因为RDD不存储数据， 但是可以将RDD结果广播出去
	// 广播变量只能在Driver端定义，不能在Executor端定义。
	// 在Driver端可以修改广播变量的值，在Executor端无法修改广播变量的值。
	// 如果executor端用到了Driver的变量，如果不使用广播变量在Executor有多少task就有多少Driver端的变量副本。
	// 如果Executor端用到了Driver的变量，如果使用广播变量在每个Executor中只有一份Driver端的变量副本。
	def getInstance(sc : SparkContext): Broadcast[Seq[String]] = {
		if(instance == null){
			synchronized {
				if(instance == null){
					val wordBlacklist = Seq("val", "var", "def")
					instance = sc.broadcast(wordBlacklist)
				}
			}
		}
		instance
	}
}

// 单例的累计器
object DroppedWordCounter {
	@volatile private var instance: LongAccumulator = null

	def getInstance(sc: SparkContext) : LongAccumulator = {
		if(instance == null) {
			synchronized {
				if (instance == null) {
					instance = sc.longAccumulator("WordsInBlacklistCounter")
				}
			}
		}
		instance
	}
}

object RecoverableNetworkWordCount {

	def deleteCheckpointDirectory(checkpointDirectory: String, sparkContext: SparkContext) = {
		val checkpointPath = new Path(checkpointDirectory)
		val hdfs = checkpointPath.getFileSystem(sparkContext.hadoopConfiguration)
		if(hdfs.exists(checkpointPath)){
			hdfs.delete(checkpointPath, true)
			println(s"$checkpointPath has been deleted ")
		}
	}

	def createContext(ip: String, port:Int, outputPath: String, checkpointDirectory: String)
			: StreamingContext = {
		// If you do not see this printed, that means the StreamingContext has been loaded
		// from the new checkpoint
		println("Creating new context")
		val outputFile = new File(outputPath)
		if (outputFile.exists()) outputFile.delete()

		val sparkConf = new SparkConf().setAppName("RecoverableNetworkWordCount")
		val ssc = new StreamingContext(sparkConf, Seconds(1))
		deleteCheckpointDirectory(checkpointDirectory, ssc.sparkContext)
		ssc.checkpoint(checkpointDirectory)

		val lines = ssc.socketTextStream(ip, port)
		val words = lines.flatMap(x => x.trim.split(" "))
		val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
		// 过于频繁地checkpoint， 会大大减少吞吐量
		wordCounts.checkpoint(Seconds(10))
		wordCounts.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
			// 获得或者登记一个广播对象，
			val blacklist = WordBlacklist.getInstance(rdd.sparkContext)

			// 获得或注册一个累计器
			val droppedWordCounter = DroppedWordCounter.getInstance(rdd.sparkContext)

			// Use blacklist to drop words and use droppedWordsCounter to count them
			val counts = rdd.filter { case (word, count) => {
				if (blacklist.value.contains(word)) {
					droppedWordCounter.add(count)
					false
				} else {
					true
				}
			}
			}.collect().mkString("[", ", ", "]")

			val output = s"Counts at time $time $counts"
			println(output)
			println(s"Dropped ${droppedWordCounter.value} word(s) totally")
			println(s"Appending to ${outputFile.getAbsolutePath}")

			Files.append(output + "\n", outputFile, Charset.defaultCharset())
		})
		ssc
	}

	def main(args: Array[String]): Unit = {
		if (args.length != 4) {
			System.err.println(s"Your arguments were ${args.mkString("[", ", ", "]")}")
			System.err.println(
				"""
				  |Usage: RecoverableNetworkWordCount <hostname> <port> <checkpoint-directory>
				  |     <output-file>. <hostname> and <port> describe the TCP server that Spark
				  |     Streaming would connect to receive data. <checkpoint-directory> directory to
				  |     HDFS-compatible file system which checkpoint data <output-file> file to which the
				  |     word counts will be appended
				  |
				  |In local mode, <master> should be 'local[n]' with n > 1
				  |Both <checkpoint-directory> and <output-file> must be absolute paths
				""".stripMargin
			)
			System.exit(1)
		}

		Logger.getRootLogger.setLevel(Level.WARN)

		val Array(ip, port, checkpointDirectory, outputPath) = args
		val ssc = StreamingContext.getOrCreate(checkpointDirectory,
			() => createContext(ip, Integer.parseInt(port), outputPath, checkpointDirectory))
		ssc.start()
		ssc.awaitTermination()
	}
}
