package learn.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class Person(name: String, age: Int)

object SqlDemo {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("Sql Demo")
		val spark = SparkSession.builder().config(sparkConf).getOrCreate()

		import spark.implicits._
		val peopleDF = spark.sparkContext
				.textFile("file:///home/hadoop/code_for_spark/data/people.txt")
    		    .map(_.split(","))
    		    .map(attribute => Person(attribute(0), attribute(1).trim.toInt))
    		    .toDF()

		peopleDF.createOrReplaceTempView("people")

		val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
		// 下面两个执行语句是效果一样的
		teenagersDF.map(teenager => "Name: " + teenager(0)).show()
		teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

		implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
		teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()

		val peopleRDD = spark.sparkContext.textFile("file:///home/hadoop/code_for_spark/data/people.txt")
		// 模式以字符串形式编码
		val schemaString = "name age"
		// 生成基于字符串模式的模式
		val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
		val schema = StructType(fields)
		// 将RDD(people) 转换成行
		val rowRDD = peopleRDD.map(_.split(","))
			.map(attribute => Row(attribute(0), attribute(1).trim))
		// 将模式应用于RDD
		val peopleDF2 = spark.createDataFrame(rowRDD, schema)

		// 创建临时视图
		peopleDF2.createOrReplaceTempView("people")
		val results = spark.sql("SELECT name FROM people")
		results.map(attributes => "Name: " + attributes(0)).show()
	}
}
