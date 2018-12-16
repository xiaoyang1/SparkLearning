package learn.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap

object EstimatorTransformerParamExample {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("EstimatorTransformerParamExample ").getOrCreate()

		// 准备训练数据 从一个 (label, features)元组列表中获得
		val training = spark.createDataFrame(Seq(
			(1.0, Vectors.dense(0.0, 1.1, 0.1)),
			(0.0, Vectors.dense(2.0, 1.0, -1.0)),
			(0.0, Vectors.dense(2.0, 1.3, 1.0)),
			(1.0, Vectors.dense(0.0, 1.2, -(0.5)))
		)).toDF("label", "features")
		// 创建逻辑回归模型实例， 这个实例是一个Estimator
		val lr = new LogisticRegression()
		// 打印 参数，文档，和任何默认值
		println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")
		// 迭代次数和正则
		lr.setMaxIter(10).setRegParam(0.01)
		// 学习LR 模型， 使用的是存储在lr中的参数
		val model1 = lr.fit(training)

		// 因为这只是个模型， 有Estimator 评估器得到的 Transformer 变换器
		// 我们可以查新拟合过程中的参数，
		println(s"Model 1 was fit using parameters: ${model1.parent.extractParamMap}")

		// 我们可以使用ParamMap重复指定参数
		val paramMap = ParamMap(lr.maxIter -> 20)
			.put(lr.maxIter -> 30)
			.put(lr.regParam -> 0.1, lr.threshold -> 0.55)
		// 也可以combine paramMaps
		val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability") // 这个用来改变列名
		val paramMapCombined = paramMap ++ paramMap2

		val model2 = lr.fit(training, paramMapCombined)
		println(s"Model 2 was fit using parameters: ${model2.parent.extractParamMap}")

		// Prepare test data.
		val test = spark.createDataFrame(Seq(
			(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
			(0.0, Vectors.dense(3.0, 2.0, -0.1)),
			(1.0, Vectors.dense(0.0, 2.2, -1.5))
		)).toDF("label", "features")

		model2.transform(test)
			.select("features", "label", "myProbability", "prediction")
			.collect()
			.foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
				println(s"($features, $label) -> prob=$prob, prediction=$prediction")
			}

		spark.stop()
	}
}
