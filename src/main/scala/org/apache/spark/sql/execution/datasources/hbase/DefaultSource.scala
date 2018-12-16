package org.apache.spark.sql.execution.datasources.hbase

import java.io.IOException
import java.math.BigInteger
import java.sql.Timestamp
import java.text.DecimalFormat

import org.joda.time.DateTime
import org.apache.spark.sql.types._
import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{BinaryType, DateType, StructField, StructType}
import java.util
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm



class DefaultSource extends CreatableRelationProvider with RelationProvider with DataSourceRegister with Logging{
	/**
	  * 保存DataFrame 到目的源中，这个mode指定了当数据已经存在时的操作，也就是write操作
	  */
	override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String],
	                            data: DataFrame): BaseRelation = {
		val relation = InsertHBaseRelation(data, parameters)(sqlContext)
		relation.insert(data, false)
		relation
	}

	/**
	  *  通过给定的参数返回一个 Relation， 这个是read操作
	  */
	override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
		HBaseRelation(parameters, None)(sqlContext)
	}
	// 这个是一个别名，就比如JDBCRelation用jdbc一样。
	override def shortName(): String = "hbase"
}


private[sql] case class InsertHBaseRelation(dataFrame: DataFrame, parameters: Map[String, String])(@transient val sqlContext: SQLContext)
			extends BaseRelation with InsertableRelation {
	override def schema: StructType = dataFrame.schema

	override def insert(data: DataFrame, overwrite: Boolean): Unit = {
		def getZKURL: String = parameters.getOrElse("hbase.zookeeper.quorum", "localhost:2181")
		def getOutPutTableName: String = parameters.getOrElse("hbase.table.name", sys.error("You must specify parameter hbase.table.name"))

		data.saveToHBase(getOutPutTableName, Some(getZKURL), parameters)
	}
}

private[sql] case class HBaseRelation(parameters: Map[String, String],
                                 userSpecifiedschema: Option[StructType])(@transient val sqlContext: SQLContext)
			extends BaseRelation with TableScan {
	def getZKURL: String = parameters.getOrElse("hbase.zookeeper.quorum", "localhost:2181")
	def getInputTableName: String = parameters.getOrElse("hbase.table.name", sys.error("You must specify parameter hbase.table.name"))

	// 获取 HBase 表的 schema
	override def schema: StructType = {
		sqlContext.sparkSession.hbaseTableAsDataFrame(getInputTableName, Some(getZKURL), parameters).schema
	}

	// 读取HBase 表的数据，得到 RDD[Row]
	override def buildScan(): RDD[Row] = {
		if (parameters.contains("spark.rowkey.view.name")) {
			val df = sqlContext.sparkSession.sql("select * from " + parameters("spark.rowkey.view.name"))
			sqlContext.sparkSession.rowkey(df).hbaseTableAsDataFrame(getInputTableName, Some(getZKURL), parameters).rdd
		}  else {
			sqlContext.sparkSession.hbaseTableAsDataFrame(getInputTableName, Some(getZKURL), parameters).rdd
		}
	}
}

object HBaseUtils extends Logging{

	case class HBaseTableSchema(cm: String, cel: String)

	case class SparkTableSchema(fieldName: String, fieldType: DataType)

	// numReg 分区数
	def createTable(connection: Connection, tableName: TableName, family: String, startKey: String, endKey: String, numReg: Int, rowkeyPrefix: String, regionSplits: String): Unit = {
		// 如果表不存在，不可用，那么初始化表
		if(null != regionSplits){
			HBaseUtils.createTable(connection, tableName, getSplitsByStrs(regionSplits), family)
		} else if(null != rowkeyPrefix){
			HBaseUtils.createTable(connection, tableName, getSplitKeys(startKey, endKey, numReg, rowkeyPrefix), family)
		} else if( numReg != -1){
			HBaseUtils.createTable(connection, tableName, getHexSplits(startKey, endKey, numReg), family)
		} else {
			var admin: Admin = null
			try {
				admin = connection.getAdmin
				if(admin.isTableAvailable(tableName)){
					val tableDescBuilder = TableDescriptorBuilder.newBuilder(tableName)
					val cfDescBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family))
					tableDescBuilder.setColumnFamily(cfDescBuilder.build())
					admin.createTable(tableDescBuilder.build())
				}
			} finally {
				if(admin != null) admin.close()
			}
		}
	}

	@throws[IOException]
	def createTable(connection: Connection, tableName: TableName, hexSplits:  Array[Array[Byte]], columnFamilies: String*): Unit = {
		var admin: Admin = null
		try{
			admin = connection.getAdmin
			if(admin.tableExists(tableName)) log.warn("table:{} exists!", tableName.getName)
			else {
				val tabledescBuilder = TableDescriptorBuilder.newBuilder(tableName)
				for(columnFamily <- columnFamilies){
					val columnFamilyBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily))
					columnFamilyBuilder.setCompressionType(Algorithm.SNAPPY)
					val ttl = connection.getConfiguration.get("hbase.cf.ttl")
					if(ttl != null){
						columnFamilyBuilder.setTimeToLive(Integer.valueOf(ttl))
					}
					tabledescBuilder.setColumnFamily(columnFamilyBuilder.build())
				}
				admin.createTable(tabledescBuilder.build(), hexSplits)
			}
		} finally {
			if(admin != null) admin.close()
		}
	}

	/**
	  * 获取HBase表Schema
	  */
	def registerHbaseTableSchema(sparkTableSchema: String): Array[HBaseTableSchema] = {
		sparkTableSchema.split(",").map(field =>
			field.split(":") match {
				case Array(cf, cel) =>
					HBaseTableSchema(cf, cel)
			}
		)
	}

	/**
	  * 获取SparkSQL表Schema
	  */
	def registerSparkTableSchema(sparkTableSchema: String): StructType = {
		StructType(
			sparkTableSchema.split(",").map(field =>
				field.split(":") match {
					case Array(fieldName, type_) =>
						val fieldType = type_.toLowerCase match {
							case "long" => LongType
							case "float" => FloatType
							case "double" => DoubleType
							case "integer" => IntegerType
							case "int" => IntegerType
							case "boolean" => BooleanType
							case "binary" => BinaryType
							case "timestamp" => TimestampType
							case "datetype" => DateType
							case _ => StringType
						}
						StructField(fieldName, fieldType)
				}
			)
		)
	}

	type HBaseValueGetter = (Result) => Any
	def makeHbaseGetter(dataType: (SparkTableSchema, HBaseTableSchema)): HBaseValueGetter = {
		val (sparkSchema, hbaseSchema) = dataType
		sparkSchema.fieldType match {
			case LongType =>
				(result: Result) =>
					try {
						Bytes.toLong(result.getValue(hbaseSchema.cm.getBytes, hbaseSchema.cel.getBytes()))
					} catch {
						case _: Exception => 0L
					}
			case FloatType =>
				(result: Result) =>
					Bytes.toFloat(result.getValue(hbaseSchema.cm.getBytes, hbaseSchema.cel.getBytes))
			case DoubleType =>
				(result: Result) =>
					Bytes.toDouble(result.getValue(hbaseSchema.cm.getBytes, hbaseSchema.cel.getBytes))
			case IntegerType =>
				(result: Result) => // 因为存的时候就是string， 如果不改的话，读出来会出问题
					Integer.parseInt(Bytes.toString(result.getValue(hbaseSchema.cm.getBytes, hbaseSchema.cel.getBytes)))
			case BooleanType =>
				(result: Result) =>
					Bytes.toBoolean(result.getValue(hbaseSchema.cm.getBytes, hbaseSchema.cel.getBytes))
			case BinaryType =>
				(result: Result) =>
					result.getValue(hbaseSchema.cm.getBytes, hbaseSchema.cel.getBytes)
			case TimestampType =>
				(result: Result) =>
					new Timestamp(Bytes.toLong(result.getValue(hbaseSchema.cm.getBytes, hbaseSchema.cel.getBytes)))
			case DateType =>
				(result: Result) =>
					new java.sql.Date(Bytes.toLong(result.getValue(hbaseSchema.cm.getBytes, hbaseSchema.cel.getBytes)))
			case _ =>
				(result: Result) =>
					if (hbaseSchema.cm.equals("")) {
						Bytes.toString(result.getRow)
					} else {
						Bytes.toString(result.getValue(hbaseSchema.cm.getBytes, hbaseSchema.cel.getBytes))
					}
		}
	}


	type HBaseRowkeySetter = (Row) => Array[Byte]
	def makeRowkeySetter(dataType: (StructField, Int)): HBaseRowkeySetter = {
		val (structField, index) = dataType
		structField.dataType match {
			case StringType =>
				(row: Row) => Bytes.toBytes(row.getString(index))
			case LongType =>
				(row: Row) => Bytes.toBytes(row.getLong(index))
			case FloatType =>
				(row: Row) => Bytes.toBytes(row.getFloat(index))
			case DoubleType =>
				(row: Row) => Bytes.toBytes(row.getDouble(index))
			case IntegerType =>
				(row: Row) => Bytes.toBytes(row.getInt(index))
			case BooleanType =>
				(row: Row) => Bytes.toBytes(row.getBoolean(index))
			case DateType =>
				(row: Row) => Bytes.toBytes(new DateTime(row.getDate(index)).getMillis)
			case TimestampType =>
				(row: Row) => Bytes.toBytes(new DateTime(row.getTimestamp(index)).getMillis)
			case BinaryType =>
				(row: Row) => row.getAs[Array[Byte]](index)
			case _ =>
				(row: Row) => Bytes.toBytes(row.getString(index))
		}
	}

	type HBaseValueSetter = (Put, Row, String) => Unit
	def makeHBaseSetter(dataType: (StructField, Int)): HBaseValueSetter = {
		val (structField, index) = dataType
		structField.dataType match {
			case StringType =>
				(put: Put, row: Row, cm: String) =>
					if (row.getString(index) == null) put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(""))
					else put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getString(index)))
			case LongType =>
				(put: Put, row: Row, cm: String) =>
					put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getLong(index)))
			case FloatType =>
				(put: Put, row: Row, cm: String) =>
					put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getFloat(index)))
			case DoubleType =>
				(put: Put, row: Row, cm: String) =>
					put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getDouble(index)))
			case IntegerType =>
				(put: Put, row: Row, cm: String) =>
					put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getInt(index)))
			case BooleanType =>
				(put: Put, row: Row, cm: String) =>
					put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getBoolean(index)))
			case DateType =>
				(put: Put, row: Row, cm: String) =>
					put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(new DateTime(row.getDate(index)).getMillis))
			case TimestampType =>
				(put: Put, row: Row, cm: String) =>
					put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(new DateTime(row.getTimestamp(index)).getMillis))
			case BinaryType =>
				(put: Put, row: Row, cm: String) =>
					put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), row.getAs[Array[Byte]](index))
			case _ =>
				(put: Put, row: Row, cm: String) =>
					put.addColumn(Bytes.toBytes(cm), Bytes.toBytes(structField.name), Bytes.toBytes(row.getString(index)))
		}
	}

	type HBaseValueSetter_bulkload = (Array[Byte], Row, String) => (ImmutableBytesWritable, KeyValue)
	def makeHbaseSetter_bulkload(dataType: (StructField, Int)): HBaseValueSetter_bulkload = {
		val (structField, index) = dataType
		structField.dataType match {
			case StringType => (rk: Array[Byte], row: Row, cm: String) =>
				if (row.getString(index) != null) (new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes(), structField.name.getBytes(), Bytes.toBytes(row.getString(index))))
				else (new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes(), structField.name.getBytes()))
			case LongType =>
				(rk: Array[Byte], row: Row, cm: String) =>
					(new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes(), structField.name.getBytes(), Bytes.toBytes(row.getLong(index))))
			case FloatType =>
				(rk: Array[Byte], row: Row, cm: String) =>
					(new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes(), structField.name.getBytes(), Bytes.toBytes(row.getFloat(index))))
			case DoubleType =>
				(rk: Array[Byte], row: Row, cm: String) =>
					(new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes(), structField.name.getBytes(), Bytes.toBytes(row.getDouble(index))))
			case IntegerType =>
				(rk: Array[Byte], row: Row, cm: String) =>
					(new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes(), structField.name.getBytes(), Bytes.toBytes(row.getInt(index))))
			case BooleanType =>
				(rk: Array[Byte], row: Row, cm: String) =>
					(new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes(), structField.name.getBytes(), Bytes.toBytes(row.getBoolean(index))))
			case DateType =>
				(rk: Array[Byte], row: Row, cm: String) =>
					(new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes(), structField.name.getBytes(), Bytes.toBytes(new DateTime(row.getDate(index)).getMillis)))
			case TimestampType =>
				(rk: Array[Byte], row: Row, cm: String) =>
					(new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes(), structField.name.getBytes(), Bytes.toBytes(new DateTime(row.getTimestamp(index)).getMillis)))
			case BinaryType =>
				(rk: Array[Byte], row: Row, cm: String) =>
					(new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes(), structField.name.getBytes(), row.getAs[Array[Byte]](index)))
			case _ =>
				(rk: Array[Byte], row: Row, cm: String) =>
					(new ImmutableBytesWritable(rk), new KeyValue(rk, cm.getBytes(), structField.name.getBytes(), Bytes.toBytes(row.getString(index))))
		}
	}

	// 返回分区region的字节数组
	def getSplitsByStrs(regionSplits: String): Array[Array[Byte]] = {
		val strs = JSON.parseArray(regionSplits)
		val splitsArray = new Array[Array[Byte]](strs.size() - 1)
		var i = 0
		while (i < strs.size() - 1){
			splitsArray(i) = strs.get(i).toString.getBytes()
			i += 1
		}
		splitsArray
	}

	def getHexSplits(start: String, end: String, numRegions: Int): Array[Array[Byte]] ={
		val startKey = if(null == start) "00000000000000000000000000000000" else start
		val endKey = if (null == end) "ffffffffffffffffffffffffffffffff" else end
		val splits = new Array[Array[Byte]](numRegions - 1)
		var lowestKey = new BigInteger(startKey, 16)
		val highestKey = new BigInteger(endKey, 16)
		val range = highestKey.subtract(lowestKey)
		val regionIncrement = range.divide(BigInteger.valueOf(numRegions))
		lowestKey = lowestKey.add(regionIncrement)
		var i = 0
		while(i < numRegions - 1){
			val key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)))
			val b = String.format("%032x", key).getBytes()
			splits(i) = b
			i += 1
		}
		splits
	}

	/**
	  * 预分区段（根据10进制均分）
	  */
	def getSplitKeys(startKey: String, endKey: String, regions: Int, rowkeyPrefix: String): Array[Array[Byte]] = {
		val lowestKey = if (null == startKey) 0 else startKey.toInt
		val highestKey = if (null == endKey) Math.pow(10, rowkeyPrefix.trim.length) - 1 else Math.min(Math.pow(10, rowkeyPrefix.trim.length), endKey.toInt)
		require(highestKey.toInt > lowestKey, "hbase.table.startKey must be smaller than hbase.table.endKey!")

		val range = highestKey - lowestKey + 1
		val numRegins: Int = Math.min(regions, Math.min(Math.pow(10, rowkeyPrefix.trim.length), range)).asInstanceOf[Int]
		val regionIncrement = Math.ceil(range / numRegins)
		var numFloor = regionIncrement * numRegins - range
		val splitKeys = new Array[Array[Byte]](numRegins - 1)
		//这里默认不会超过两位数的分区，如果超过，需要变更设计，如果需要灵活操作，也需要变更设计
		val keys = new Array[String](numRegins - 1)
		val df = new DecimalFormat(rowkeyPrefix)
		var tempKey = lowestKey - 1
		for (i <- 0 until numRegins - 1) {
			if (numFloor > 0) {
				tempKey = tempKey + (regionIncrement.toInt - 1)
				keys(i) = df.format(tempKey) + "|"
				numFloor -= 1
			} else {
				tempKey = tempKey + regionIncrement.toInt
				keys(i) = df.format(tempKey) + "|"
			}
		}
		log.info("Split Keys is " + keys.mkString(" "))
		val rows = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)
		for (i <- keys.indices) {
			rows.add(Bytes.toBytes(keys(i)))
		}
		val rowKeyIter = rows.iterator()
		var n: Int = 0
		while (rowKeyIter.hasNext) {
			val trmpRow = rowKeyIter.next()
			rowKeyIter.remove()
			splitKeys(n) = trmpRow
			n += 1
		}
		splitKeys
	}
}
