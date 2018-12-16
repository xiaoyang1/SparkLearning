package learn.hbase

import org.apache.hadoop.fs.{Path}
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Mutation, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  这个是学习林子雨的用 save的方式存储hbase
  */
object WriteExample {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("HbaseDemo").setMaster("local[3]")
		val sc = new SparkContext(sparkConf)
		// 注意序列化
		val conf = HBaseConfiguration.create()
		// 配置输入表
		val inputTableName = "student"
		val outputTableName = "hbase_book"
		conf.set(TableInputFormat.INPUT_TABLE, inputTableName)
		// 看源码， 第一个是InputFormat， 第二个是Key， 最后是Value
		val stuRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
			classOf[ImmutableBytesWritable], classOf[Result])

		val count = stuRDD.count()
		println("Students RDD Count:" + count)

		stuRDD.cache()


//		val result = stuRDD.map{ case (_, result: Result) =>
//			val put = new Put(result.getRow)
//			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
//			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))
//			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex")))
//			(NullWritable.get(), put)
//		}
//		// 由于查看源码知道， TableOutputFormat 源码中Key的类型是没有用到的。
//		conf.set(TableOutputFormat.OUTPUT_TABLE, outputTableName)
//		val job = Job.getInstance(conf)
//		job.setOutputKeyClass(classOf[NullWritable])
//		job.setOutputFormatClass(classOf[TableOutputFormat[NullWritable]])
//		result.saveAsNewAPIHadoopDataset(job.getConfiguration)

		// 或者采取如下方式： 先写HFile 文件，然后再存入表中

		conf.set(TableOutputFormat.OUTPUT_TABLE, outputTableName)
		val job1 = Job.getInstance(conf)
		job1.setOutputKeyClass(classOf[ImmutableBytesWritable])
		// 必须是使用Hbase的键值对类型
		job1.setOutputValueClass(classOf[Cell])
		// 配置HFileOutputFormat
		val conn = ConnectionFactory.createConnection(conf)
		val table = conn.getTable(TableName.valueOf(outputTableName))
		// 看HFileOutputFormat2源码可知，输出key必须是 ImmutableBytesWritable, 输出必须是Cell类型
		HFileOutputFormat2.configureIncrementalLoad(job1, table, conn.getRegionLocator(table.getName))
		val kvResult = stuRDD.flatMap{
			case (rowKey, result) =>
				for(cell <- result.rawCells()) yield ((rowKey, cell))
		}

		stuRDD.map{
			case (rowKey, result) =>
				for(cell <- result.rawCells()) yield ((rowKey, cell))
		}
		// 保存文件到HFile上，再BulkLoad
		kvResult.saveAsNewAPIHadoopFile("/tmp/data1", classOf[ImmutableBytesWritable], classOf[Cell],
			classOf[HFileOutputFormat2], job1.getConfiguration)

		val bulkLoader = new LoadIncrementalHFiles(job1.getConfiguration)
		bulkLoader.doBulkLoad(new Path("/tmp/data1"), conn.getAdmin, table, conn.getRegionLocator(table.getName))
	}
}
