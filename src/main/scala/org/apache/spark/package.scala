package org.apache

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.{DataFrameFunctions, SparkSqlContextFunctions}

package object spark {
	implicit def toSparkSqlContextFunctions(spark: SparkSession): SparkSqlContextFunctions = {
		new SparkSqlContextFunctions(spark)
	}

	implicit def toDataFrameFunctions(data: DataFrame): DataFrameFunctions = {
		new DataFrameFunctions(data)
	}
}
