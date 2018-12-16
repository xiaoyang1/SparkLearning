package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HBaseTestDemo {
	def main(args: Array[String]): Unit = {
		// 方式一
		val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Hbase DataSource")
		val spark = SparkSession.builder().config(sparkConf).getOrCreate()

		spark.hbaseTableAsDataFrame("hbase_book", Some("192.168.1.81:2181")).show(false)
		// 方式二
		spark.read.format("org.apache.spark.sql.execution.datasources.hbase").
			options(Map(
				"spark.table.schema" -> "id:String,name:String,age:int,sex:String",
				"hbase.table.schema" -> ":rowkey,info:name,info:age,info:sex",
				"hbase.zookeeper.quorum" -> "192.168.1.81:2181",
				"hbase.table.name" -> "hbase_book"
			)).load.show(false)

	}
}
