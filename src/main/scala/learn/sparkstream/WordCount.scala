package learn.sparkstream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *  这个类是用来学习spark 流处理的，用于统计网络流ip传来的单词数目
  */
object WordCount {
	def main(args: Array[String]): Unit = {
//		if(args.length < 2){
//			System.err.println("Usage: NetworkWordCount <hostname> <port>")
//			System.exit(1)
//		}
//		Logger.getRootLogger.setLevel(Level.WARN)

		// 获得spark 配置
		val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
		// 获得 流上下文, 这是流功能的主要入口点。
		val ssc = new StreamingContext(conf, Seconds(1))
		// 监听本地端口9999
		// lines 是一个 DStream 类型，这个流戴白了从端口接收的流数据，一行是一个record。
		val lines = ssc.socketTextStream("localhost", 9999)
//		val lines = ssc.socketTextStream(args(0), Integer.parseInt(args(1)))


		// 变换transformation ， flatMap是一对多DStream操作，
		// 它通过从源DStream中的每个记录生成多个新记录来创建新的DStream。
		val words = lines.flatMap(_.trim().split(" "))

		val pairs = words.map(x => (x,1))
		val wordCount = pairs.reduceByKey(_ + _)
		wordCount.countByValue().print()
		// 将此DStream中生成的每个RDD的前十个元素打印到控制台
		wordCount.print()

		// 到目前为止， spark streaming 只是设置启动时将执行的计算， 并没有开始真正的处理。
		// 要在完成所有转换后开始处理， 需要调用
		try {
			ssc.start()
			ssc.awaitTermination() // 等待计算被终止
		} catch {
			case _ : Throwable => {}
		}
	}
}
