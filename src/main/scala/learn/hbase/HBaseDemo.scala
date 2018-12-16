package learn.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  这个是根据HBase 的官网实现的
  */
object HBaseDemo {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("HbaseDemo").setMaster("local[4]")
		val sc = new SparkContext(sparkConf)

		val config = HBaseConfiguration.create()
		val hbaseContext = ???

	}
}
