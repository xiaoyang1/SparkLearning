package learn.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  *  用户自定义 无类型的聚合函数
  */
object MyAvg extends UserDefinedAggregateFunction{
	// 此聚合函数的输入参数数据类型
	override def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
	// 聚合缓冲区中值的数据类型
	override def bufferSchema: StructType = {
		StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
	}
	// 返回值的数据类型
	override def dataType: DataType = DoubleType
	// 此函数是否始终在相同输入上返回相同的输出
	override def deterministic: Boolean = true
	// 初始化给定的聚合缓冲区。 缓冲区本身是一个“行”，
	// 除此之外还提供标准方法，如在索引处检索值（例如，get（），getBoolean（））
	// 更新其值的机会。注意，缓冲区内的array 和map还是不可变的。
	override def initialize(buffer: MutableAggregationBuffer): Unit = {
		buffer(0) = 0L
		buffer(1) = 0L
	}

	// 更新 buffer
	override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
		if(!input.isNullAt(0)) {
			buffer(0) = buffer.getLong(0) + input.getLong(0)
			buffer(1) = buffer.getLong(1) + 1
		}
	}
	// 合并两个聚合缓冲区并将更新的缓冲区值存储回“buffer1”
	override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
		buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
		buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
	}

	override def evaluate(buffer: Row): Double = {
		buffer.getLong(0).toDouble / buffer.getLong(1)
	}
}

object Test {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("MyAvg")
		val spark = SparkSession.builder().config(sparkConf).getOrCreate();

		spark.udf.register("myAverage", MyAvg)
		val df = spark.read.json("file:///home/hadoop/code_for_spark/data/employees.json")
		df.createOrReplaceTempView("employees")
		df.show()

		val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
		result.show()
	}
}
