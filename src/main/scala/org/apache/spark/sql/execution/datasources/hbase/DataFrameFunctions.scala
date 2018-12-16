package org.apache.spark.sql.execution.datasources.hbase


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.DefaultFormats
import org.apache.spark.util.SerializableConfiguration

import scala.collection.immutable.HashMap

 class DataFrameFunctions(data: DataFrame) extends Serializable with Logging{
	def saveToHBase(tableName: String, zkUrl: Option[String] = None,
	                options: Map[String, String] = new HashMap[String, String]): Unit = {
		val wrappedConf = {
			implicit val formats = DefaultFormats
			val hc = HBaseConfiguration.create()
			hc.set("hbase.zookeeper.quorum", zkUrl.getOrElse("localhost:2181"))
			hc.set("hbase.client.keyvalue.maxsize", "104857600")
			options.foreach(p => hc.set(p._1, p._2))
			data.sparkSession.sparkContext.getConf.getAll
				.filter(_._1.toLowerCase.contains("hbase"))
    			.foreach(p => hc.set(p._1, p._2))
			new SerializableConfiguration(hc)
		}

		// 解析对应的字段
		val hbaseConf = wrappedConf.value
		val rowkey = options.getOrElse("hbase.table.rowkey.field", data.schema.head.name)
		require(data.schema.fields.map(_.name).contains(rowkey),"No field name named " + rowkey)
		log.warn("The rowkey field name is " + rowkey)
		if(!options.contains("hbase.table.rowkey.field")) log.warn("The hbase.table.rowkey.field is not set, and the first field is rowkey as the default `" + data.schema.head.name + "`")
		val family = options.getOrElse("hbase.table.family", "info")
		if(!options.contains("hbase.table.family")) log.warn("The hbase.table.family is not set, use the default family is `info`")
		val numReg = options.getOrElse("hbase.table.numReg", -1).toString.toInt
		val rowkeyPrefix = options.getOrElse("hbase.table.rowkey.prefix", null)
		val regionSplits = options.getOrElse("hbase.table.region.splits", null)
		val startKey = options.getOrElse("hbase.table.startKey", options.getOrElse("hbase.table.startkey", null))
		val endKey = options.getOrElse("hbase.table.endKey", options.getOrElse("hbase.table.endkey", null))

		val rdd = data.rdd
		val f = family

		val tName = TableName.valueOf(tableName)
		val connection = ConnectionFactory.createConnection(hbaseConf)
		val admin = connection.getAdmin
		if(options.getOrElse("hbase.check_table","false").toBoolean){
			if(!admin.isTableAvailable(tName)){
				HBaseUtils.createTable(connection, tName, family, startKey, endKey, numReg,rowkeyPrefix,regionSplits)
			}
		}
		if (hbaseConf.get("mapreduce.output.fileoutputformat.outputdir") == null) {
			hbaseConf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")
		}

		val conf = new Configuration(hbaseConf)
		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
		val job = Job.getInstance(conf)
		job.setOutputKeyClass(classOf[ImmutableBytesWritable])
		job.setOutputValueClass(classOf[Result])
		job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

		val fields = data.schema.toArray
		val rowkeyField = fields.zipWithIndex.filter(f => f._1.name == rowkey).head
		val otherFields = fields.zipWithIndex.filter(f => f._1.name != rowkey )

		lazy val rowkeySetter = HBaseUtils.makeRowkeySetter(rowkeyField)
		lazy val setters = otherFields.map(r => HBaseUtils.makeHBaseSetter(r))
		lazy val setters_bulkload = otherFields.map(r => HBaseUtils.makeHbaseSetter_bulkload(r))

		options.getOrElse("bulkload.enable", "false") match {
			case "true" =>
				val tmpPath = s"/tmp/bulkload/${tableName}" + System.currentTimeMillis()
				def convertToPut_bulkload(row: Row) ={
					val rk = rowkeySetter.apply(row)
					setters_bulkload.map(_.apply(rk, row, f))
				}
				rdd.flatMap(convertToPut_bulkload).sortBy(_._1)
    				.saveAsNewAPIHadoopFile(tmpPath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)

				val bulkLoader: LoadIncrementalHFiles = new LoadIncrementalHFiles(hbaseConf)
				bulkLoader.doBulkLoad(new Path(tmpPath), admin, connection.getTable(TableName.valueOf(tableName)), connection.getRegionLocator(TableName.valueOf(tableName)))
			case "false" =>
				def convertToPut(row: Row) = {
					val put = new Put(rowkeySetter.apply(row))
					setters.foreach(_.apply(put, row, f))
					(new ImmutableBytesWritable(), put)
				}
				rdd.map(convertToPut).saveAsNewAPIHadoopDataset(job.getConfiguration)
		}

		admin.close()
		connection.close()
	}
}
