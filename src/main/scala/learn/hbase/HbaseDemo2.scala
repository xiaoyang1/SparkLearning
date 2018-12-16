package learn.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
/**
  *  这个是看林子雨的方式给的方法，主要是看hbase官网的包没有找到，
  */
object HbaseDemo2 {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("HbaseDemo").setMaster("local[4]")
		val sc = new SparkContext(sparkConf)

		val conf = HBaseConfiguration.create()
		// 配置输入表
		val tableName = "student"
		conf.set(TableInputFormat.INPUT_TABLE, tableName)
		// 看源码， 第一个是InputFormat， 第二个是Key， 最后是Value
		val stuRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
			classOf[ImmutableBytesWritable], classOf[Result])

		val count = stuRDD.count()
		println("Students RDD Count:" + count)

		stuRDD.cache()

		// 遍历输出
		stuRDD.foreachPartition(iter => {
			iter.foreach{
				case (_, result) => {
					val key = Bytes.toString(result.getRow)
					val name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
					val age = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))
					val sex = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex")))
					println("Row key:" + key + " Name:" + name + " Gender:" + sex + " Age:" + age)
				}
			}
		})
	}
}
