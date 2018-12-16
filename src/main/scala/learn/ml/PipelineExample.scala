package learn.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.Vector

object PipelineExample {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("PipelineExample").getOrCreate()

		val training = spark.createDataFrame(Seq(
			(0L, "a b c d e spark", 1.0),
			(1L, "b d", 0.0),
			(2L, "spark f g h", 1.0),
			(3L, "hadoop mapreduce", 0.0)
		)).toDF("id", "text", "label")

		// 配置一个ML pipeline 管道， 这包含三个阶段： tokenizer， hashingTF， 和LR
		val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("word")
		val hashingTF = new HashingTF().setNumFeatures(1000)
			.setInputCol(tokenizer.getOutputCol)
    		.setOutputCol("features")

		val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)

		val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

		// 训练模型
		val model = pipeline.fit(training)
		// 保存模型
		model.write.overwrite().save("/spark/ml/tmp/spark-logistic-regression-model")
		// 也可以保存没有开始拟合的模型
		pipeline.write.overwrite().save("/spark/ml/tmp/unfit-lr-model")

		val sameModel = PipelineModel.load("/spark/ml/tmp/spark-logistic-regression-model")

		// 准备测试文档
		val test = spark.createDataFrame(Seq(
			(4L, "spark i j k"),
			(5L, "l m n"),
			(6L, "spark hadoop spark"),
			(7L, "apache hadoop")
		)).toDF("id", "text")

		// 预测文档结果
		model.transform(test)
    		.select("id", "text", "probability", "prediction")
    		.collect()
    		.foreach{
			    case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
				    println(s"($id, $text) --> prob=$prob, prediction=$prediction")
		    }

		spark.stop()
	}
}