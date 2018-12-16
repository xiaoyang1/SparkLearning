package learn.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DataSourceDemo {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("DataSourceDemo")
		val spark = SparkSession.builder().config(sparkConf).getOrCreate()



		import spark.implicits._
		// 创建一个简单的DataFrame， 存储进一个分区目录中
		val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i*i)).toDF("value", "square")
		squaresDF.write.parquet("data/test_table/key=1")
		// 创建另一个DataFrame 入另一个分区目录，
		// 添加新列，并drop 一个已经存在的列
		val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i*i*i)).toDF("value", "cube")
		cubesDF.write.parquet("data/test_table/key=2")

		// 读取分区表
		val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
		
		mergedDF.printSchema()
	}
}
