package learn.structuredStreaming

import java.util.Date

import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *  这个主要是学习structured stream的一些DataFrame 和dataSet 的操作，
  *  一些无类型的DF操作和有类型的DS操作，以及一些windows操作，
  */
object SourceOperation {

	case class DeviceData(device: String, deviceType: String, signal: Double, time: Date)

	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("SourceOperation").getOrCreate()

		val df:DataFrame = ???  // streaming DataFrame with IOT device data with schema
		// { device: string, deviceType: string, signal: double, time: string }
		import spark.implicits._
		val ds:Dataset[DeviceData] = df.as[DeviceData]

		// Select the devices which have signal more than 10
		df.select("device").where("signal > 10")      // using untyped APIs
		ds.filter(_.signal > 10).map(_.device)         // using typed APIs

		df.groupBy("deviceType").count() // using untyped APIs

		ds.groupByKey(_.deviceType).agg(typed.avg(_.signal))
	}
}
