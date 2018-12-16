package learn.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator


/**
  *  用来  自定义 一个 类型安全的聚合函数
  */

case class Employee(name: String, salary: Long)
case class Average(var sum: Double, var count: Long)

object MyAverage extends Aggregator[Employee, Average, Double]{
	// 零值 初始化
	override def zero: Average = Average(0L, 0L)
	// 结合两个值产生一个新的值，
	override def reduce(buffer: Average, employee: Employee): Average = {
		buffer.sum += employee.salary
		buffer.count += 1
		buffer
	}
	// 合并两个中间值
	override def merge(b1: Average, b2: Average): Average = {
		b1.sum += b2.sum
		b1.count += b2.count
		b1
	}
	// 结束
	override def finish(reduction: Average): Double = {
		reduction.sum / reduction.count
	}
	// 缓冲区编码器
	override def bufferEncoder: Encoder[Average] = Encoders.product
	// 输出编码器
	override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object MyAverageTest {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("MyAverageTest")
		val spark = SparkSession.builder().config(sparkConf).getOrCreate()

		import spark.implicits._
		val ds = spark.read.json("file:///home/hadoop/code_for_spark/data/employees.json").as[Employee]
		ds.show()
//		val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
//		peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")
		// 将函数转换为`TypedColumn`并为其命名
		val averageSalary = MyAverage.toColumn.name("average_salary")
		val result = ds.select(averageSalary)
		result.show()
		// +--------------+
		// |average_salary|
		// +--------------+
		// |        3750.0|
		// +--------------+
	}
}




