package learn.structuredStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}

object SourceExample {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("SourceExample").getOrCreate()

		import spark.implicits._

		val socketDF = spark.readStream.format("socket")
    		.option("host", "localhost")
    		.option("port", 9999)
    		.load()

		println("socketDF is streaming : " + socketDF.isStreaming)
		socketDF.printSchema()


		// 读取所有的自动写入文件夹的csv文件
		val userSchema = new StructType().add("name", DataTypes.StringType)
    		.add("age", DataTypes.IntegerType)

		val csvDF = spark.readStream
			.option("sep", ";")
			.schema(userSchema)
			.csv("path/to/diractory")


	}
}
