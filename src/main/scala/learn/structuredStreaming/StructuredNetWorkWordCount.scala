package learn.structuredStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode


/**
  *  这个是用来学习 structured Stream 的
  */
object StructuredNetWorkWordCount {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder()
			.appName("StructuredNetWorkWordCount")
    		.getOrCreate()

		import spark.implicits._
		// 创建 DataFrame 表示从连接到localhost的输入行流：9999
		// 这个lines DataFrame 表示包含流文本数据的无界表。 这个表包含一列: "value"
		// 流数据的每一行就是table的每一row。  注意，这个也是懒加载的，没有start 就不是运行。
		val lines = spark.readStream
    		.format("socket")
    		.option("host", "localhost")
    		.option("port", 9999)
    		.load()
		// 然后，转换DataFrame 到DataSet，使用 .as[String] ，这样就可以使用 flatMap
		val words = lines.as[String].flatMap(_.split(" "))
		// 最后就可以定义 wordCounts Dataframe 通过数据集中的唯一值进行分组并对其进行计数。
		// 请注意，这是一个流式DataFrame，它表示流的运行字数。
		val wordCounts = words.groupBy("value").count()

		val query = wordCounts.writeStream
    		.outputMode(OutputMode.Complete())
    		.format("console")
    		.start()

		query.awaitTermination()
	}

	/**
	  *  从这个例子中，可以看到，lines 是 input table，一个无界表。
	  *  而 lines DataFrame 生成 wordCount 的过程都是和静态的DataFrame一样， DAG是一致的
	  *  当query查询开始时，spark 会持续从socket中检查新数据，
	  *  如果有新数据，spark会执行一个“增量”查询，它将先前的运行计数与新数据相结合，以计算更新的计数
	  */
}
