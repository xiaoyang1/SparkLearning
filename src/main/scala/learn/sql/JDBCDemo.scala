package learn.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object JDBCDemo {

	case class Book(name: String, price: Double)

	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("JDBC Demo")
		val spark = SparkSession.builder().config(sparkConf).getOrCreate()

		val url = "jdbc:mysql://192.168.1.82:3306/db_library?createDatabaseIfNotExist=true&&useSSL=false"

		val jdbcDF = spark.read.format("jdbc")
    		.option("url", url)
    		.option("dbtable", "book")
    		.option("user", "root")
    		.option("password", "xiaoyang123")
    		.load()



		import spark.implicits._
		val data = (1 to 4).map(i => (s"book_$i", i*i)).toDF()

		// 创建schema 对应起来
		val dataSchema = StructType(List(
			StructField("id", DataTypes.IntegerType, true),
			StructField("name", DataTypes.StringType, true),
			StructField("price", DataTypes.DoubleType, true)
		))
		val data2 = (5 to 8).map(i => Row(i ,s"book_$i", i*i.toDouble))
		val data1 = spark.createDataFrame(spark.sparkContext.parallelize(data2), dataSchema)

		val connectProperties: Properties = new Properties()
		connectProperties.put("username", "root")
		connectProperties.put("password", "xiaoyang123")
		val jdbcDF2 = spark.read.jdbc(url, "book", connectProperties)

		data1.write.jdbc(url, "book", connectProperties)
		// data 会报错， 因为列名对不上, 但是加上以后也不行。
		data.write.format("jdbc")
    		.mode(SaveMode.Append)
			.option("url", url)
			.option("dbtable", "book")
			.option("user", "root")
			.option("password", "xiaoyang123")
			.save()

	}
}
