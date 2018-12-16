package analysis.music

import breeze.linalg.{max, min}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object MusicALS {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf()
			.setMaster("spark://master:7077")
			.setAppName("Music 推荐")
		val sc = new SparkContext(sparkConf)

		val rawUserArtistData = sc.textFile("/data/spark/music_data/user_artist_data.txt")
		// 打印一下数据的统计状态，看看是否需要处理
//		rawUserArtistData.map(_.split(" ")(0).toDouble).stats()
//		rawUserArtistData.map(_.split(" ")(1).toDouble).stats()
		// 因为最大用户和艺术家ID为2443548和10794401，没必要处理这些ID
//      org.apache.spark.util.StatCounter = (count: 24296858, mean: 1947573.265353, stdev: 496000.544975, max: 2443548.000000, min: 90.000000)
//		org.apache.spark.util.StatCounter = (count: 24296858, mean: 1718704.093757, stdev: 2539389.040171, max: 10794401.000000, min: 1.000000)

		// 接着是解析艺术家ID与与艺术家名对应关系。由于文件中少量的行不规范，有些行没有制表符、有些不小心加入了换行符，所以不能直接使用map处理。
		// 这是需要使用flatMap，它将输入对应的两个或多个结果组成的集合简单展开，然后放到一个更大的RDD中。
		val rawArtistData = sc.textFile("/data/spark/music_data/artist_data.txt")
		val artistByID = rawArtistData.flatMap { line =>
			val (id, name) = line.span(_ != '\t')
			if(name.isEmpty){
				None
			} else {
				try {
					Some((id.toInt, name.trim))
				} catch {
					case e: NumberFormatException => None
				}
			}
		}

		// 将拼写错误的艺术家ID 或 非标准的艺术家ID 映射为艺术家的 正规名
		val rawArtistAlias = sc.textFile("/data/spark/music_data/artist_alias.txt")
		val artstAlias = rawArtistAlias.flatMap { line =>
			val token = line.split("\t")
			if(token(0).isEmpty){
				None
			} else {
				Some((token(0).toInt, token(1).toInt))
			}
		}.collectAsMap()
		// artist_alias.txt中第一条为："1092764 1000311"，获取ID为1092764和1000311的艺术家名：
		artistByID.lookup(1092764).head

		// 构建第一个模型
		// 需要做两个转换， 第一， 将艺术家ID转为正规ID；
		// 第二，把数据转换成rating对象，它是ALS算法对“用户-产品-值”的抽象。其中产品指“向人们推荐的物品”
		val bArtistAlias = sc.broadcast(artstAlias)
		val trainData = rawUserArtistData.map{ line =>
			val Array(userId, artistID, count) = line.split(" ").map(_.toInt)
			val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
			Rating(userId, finalArtistID, count)
		}.cache()

		val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

		// 逐个检查推荐结果， 获取用户ID对应的艺术家
		val rawArtistsForUser = rawUserArtistData.map(_.split(" "))
    		.filter{case Array(user, _, _) => user.toInt == 2093760 }

		val existingProducts = rawArtistsForUser.map{case Array(_, artist, _) => artist.toInt}
    		.collect().toSet

		artistByID.filter{case (id, name) => existingProducts.contains(id)}.values.collect().foreach(println)


		// 为此用户做出5个推荐
		val recommendations = model.recommendProducts(2093760, 5)
		recommendations.foreach(println)

		// 得到所推荐艺术家的ID后，就可以用类似的方法查到艺术家的名字：
		val recommendedProductIDs = recommendations.map(_.product).toSet
		artistByID.filter{case (id, name) => recommendedProductIDs.contains(id)}.values.collect().foreach(println)
	}
}
