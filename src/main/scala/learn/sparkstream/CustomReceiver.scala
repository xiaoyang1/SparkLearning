package learn.sparkstream

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.{Logger, LoggerFactory}

class CustomReceiver(host: String, port: Int)
	extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
	protected final val logger : org.slf4j.Logger= LoggerFactory.getLogger(this.getClass())

	override def onStart(): Unit = {
		// 开启一个线程在连接上接收数据
		new Thread(){
			override def run(): Unit = {
				receive()
			}
		}.start()
	}

	override def onStop(): Unit = {
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself isStopped() returns false
	}

	private def receive(): Unit = {
		var socket: Socket = null
		var userInput: String = null

		try{
			logger.warn("Connecting to " + host + ":" + port)
			socket = new Socket(host, port)
			logger.warn("Connected to " + host + ":" + port)
			val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
			userInput = reader.readLine()
			while (!isStopped() && userInput != null){
				store(userInput)
				userInput = reader.readLine()
			}
			reader.close()
			socket.close()
			logger.warn("Stopped receiving")
			restart("Trying to connect again")
		} catch {
			case e: java.net.ConnectException => restart("Error connecting to " + host + ":" + port, e)
			case throwable: Throwable =>  restart("Error receiving data", throwable)
		}
	}
}


object CustomReceiver{
	def main(args: Array[String]): Unit = {
		if (args.length < 2) {
			System.err.println("Usage: CustomReceiver <hostname> <port>")
			System.exit(1)
		}
//		org.apache.log4j.Logger.getRootLogger.setLevel(Level.WARN)

		// 创建一个context ，batch 间隔为1s
		val sparkConf = new SparkConf().setAppName("CustomReceiver")
		val ssc = new StreamingContext(sparkConf, Seconds(1))

		// 用自定义的流接收数据
		val lines = ssc.receiverStream(new CustomReceiver(args(0), args(1).toInt))
		val words = lines.flatMap(_.trim.split(" "))
		val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)

		wordCount.print()

		try {
			ssc.start()
			ssc.awaitTermination() // 等待计算被终止
		} catch {
			case _ : Throwable => {}
		}
	}
}