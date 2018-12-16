package learn.hbase

import java.util

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext, SerializableWritable}

/**
  *  这个是结合林子雨的读方式，将得到的数据插入hbase-book中， 但是有conf 序列化问题
  */
object HBaseWriteDemo2 {
	class MyConn() extends Serializable{
		val conf = HBaseConfiguration.create()
		lazy val conn = ConnectionFactory.createConnection(conf)
		def getTable(tableName: String): Table = {
			conn.getTable(TableName.valueOf(tableName))
		}

		def setInputTable(tableName: String) = {
			conf.set(TableInputFormat.INPUT_TABLE, tableName)
		}

	}

	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("HbaseDemo").setMaster("local[3]")
		val sc = new SparkContext(sparkConf)

		val tableName = "student"
		val myConn = new MyConn()
		myConn.setInputTable(tableName)

		val stuRDD = sc.newAPIHadoopRDD(myConn.conf, classOf[TableInputFormat],
			classOf[ImmutableBytesWritable], classOf[Result])

		val count = stuRDD.count()
		println("Students RDD Count:" + count)

		val broadcastedConf = sc.broadcast(new SerializableWritable(HBaseConfiguration.create()))

		val func1 = (iter: Iterator[(ImmutableBytesWritable, Result)]) => {
//			val conn = ConnectionFactory.createConnection(HBaseConfiguration.create())
			val conn = ConnectionFactory.createConnection(broadcastedConf.value.value)
			val table = conn.getTable(TableName.valueOf("hbase_book"))
			val puts = new util.ArrayList[Put]()

			iter.foreach{
				case (_, result) =>
					val put = new Put(result.getRow)
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))
					put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex")))
					puts.add(put)
			}
			table.put(puts)
			table.close()
			conn.close()
		}
		// 遍历输出
		stuRDD.foreachPartition(func1)
	}
}

