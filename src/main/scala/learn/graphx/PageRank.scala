package learn.graphx

import breeze.linalg.*
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.log4j.Logger
import org.apache.spark.graphx._
import org.apache.spark.graphx.{Graph, TripletFields, VertexId}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
  *  这个是学习真正的pagerank， 这是org.apache.spark.graphx.lib.PageRank 的源码一样
  *  这里提供了两个实现：
  *  1. 第一个实现是：使用独立的 “Graph” 接口，跑固定iterations 的 PageRank
  *  2. 第二个是使用 “Pregel” 接口， 直到收敛为止。
  */
object PageRank {

	private val logger = LoggerFactory.getLogger(this.getClass)
	/**
	  *
	  * @param graph  图
	  * @param numIter  迭代次数
	  * @param resetProb  随机重置 概率 alpha， 也就是不通过超链接，而是通过直接访问的概率 1-a
	  * @tparam VD 顶点泛型参数
	  * @tparam ED 边泛型参数
	  * @return
	  */
	def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int,
	                                    resetProb: Double = 0.15): Graph[Double, Double] = {
		runwithOptions(graph, numIter, resetProb)
	}

	def runwithOptions[VD: ClassTag, ED: ClassTag](
          graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15,
          srcId: Option[VertexId] = None): Graph[Double, Double] ={
		// 限制条件， iter 大于0，resetProb 大于等于0， 小于等于1
		require(numIter > 0, s"Number of iterations must be greater than 0,")
		require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong")

		val personalized = srcId.isDefined
		val src = srcId.getOrElse(-1L)
		// 使用每条边属性 edge attribute 初始化 PageRank 图， 出度为N的话，每个起始pg 权重都是 1/N
		// 而且，每个顶点的属性都是 1.0 ， 如果运行个性化PageRank， 只有源顶点是1， 其他是0
		// rankGraph 就是类似于最终的排名表。
		var rankGraph: Graph[Double, Double] = graph
			// 获得每个顶点的出度
			.outerJoinVertices(graph.outDegrees){ case (vertextId, vdata, outDeg) => outDeg.getOrElse(0)}
    		// 每条边的属性为 1/N，这里要是源节点的出度为0，岂不是无穷大？ 也就是对A连接矩阵赋值
			// 不会，既然是每条边，必须会有出度啊，所以不可能为0的
			.mapTriplets(e => 1.0/e.srcAttr, TripletFields.Src)
			//  如果运行个性化PageRank， 只有源顶点是1， 其他是0， 也就是对一开始的P0赋值，全是1
    		.mapVertices((id, attr) => if(!(id != src && personalized)) 1.0 else 0.0 )

		def delta(u: VertexId, v: VertexId)= { if(u == v) 1.0 else 0.0}

		var iteration = 0
		var prevPageRank: Graph[Double, Double] = null

		while (iteration < numIter){
			rankGraph.cache()

			val rankUpdates = rankGraph.aggregateMessages[Double](
				// map 函数
				ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr),
				_ + _,
				TripletFields.Src
			)

			// 使用最终的rank更新最新排名，通过join来保留没有收到message信息的顶点，
			// 这个需要shuffle 和 广播 更新的rank。
			prevPageRank = rankGraph
			// reset 准确的来说是阻尼系数，一般为0.85，
			// resetProb: Double = 0.15 就是用户停止浏览，重新跳入其他URL的概率
			val rPro = if(personalized){
				(src: VertexId, id: VertexId) => resetProb * delta(src, id)
			} else {
				(src: VertexId, id: VertexId) => resetProb
			}

			// 对于每个顶点，更新pg 值 并缓存
			rankGraph = rankGraph.outerJoinVertices(rankUpdates){
				case (id, oldPg, msgSum) => rPro(src, id) + (1.0 - resetProb) * msgSum.getOrElse(0.0)
			}.cache()

			rankGraph.edges.foreach(x => {})
			logger.info(s"PageRank finished iteration $iteration.")
			prevPageRank.vertices.unpersist(false)
			prevPageRank.edges.unpersist(false)

			iteration += 1
		}
		// SPARK-18847如果图形具有下沉（没有外边缘的顶点），则校正等级的总和
		normalizeRankSum(rankGraph, personalized)
	}

//	def runParallelPersonalizedPageRank[VD: ClassTag, ED: ClassTag](
//		graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15,
//		sources: Array[VertexId]): Graph[Vector, Double] = {
//
//		require(numIter > 0, s"Number of iterations must be greater than 0,")
//		require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belon")
//		require(sources.nonEmpty, s"The list of sources must be non-empty,")
//
//	}

	def runUntilConvergenceWithOptions[VD: ClassTag, ED: ClassTag](
		graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15,
		srcId: Option[VertexId] = None): Graph[Double, Double] = {
		require(tol >= 0, s"Tolerance must be no less than 0, but got ${tol}")
		require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong")

		val personalized = srcId.isDefined
		val src: VertexId = srcId.getOrElse(-1L)

		val pageRankGraph: Graph[(Double, Double), Double] = graph
    		.outerJoinVertices(graph.outDegrees){
			    (id, vdata, deg) => deg.getOrElse(0)
		    }
			// 设置每条边的权重
    		.mapTriplets(e => 1.0 / e.srcAttr)
			// 设置每个顶点属性 (initialPR, delta = 0)
    		.mapVertices((id, attr) => if(id == src) (0.0, Double.NegativeInfinity) else (0.0, 0.0))
    		.cache()

		def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
			val (oldPR, lastDelta) = attr
			val newPR = oldPR + (1.0 - resetProb) * msgSum
			(newPR, newPR - oldPR)
		}

		def personalizedVertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
			val (oldPR, lastDelta) = attr
			val newPR = if(lastDelta == Double.NegativeInfinity){
				1.0
			} else {
				oldPR + (1.0 - resetProb) * msgSum
			}
			(newPR, newPR - oldPR)
		}

		def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
			// 也就是 (newPR, newPR - oldPR) 后面误差没有达到阈值的时候，不算收敛，就会继续。
			if(edge.srcAttr._2 > tol){
				// 向edge.dstId 发送message， message 为
				Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
			} else {
				Iterator.empty
			}
		}

		def messageCombiner(a: Double, b: Double) = a + b

		val initailMessage = if(personalized) 0.0 else resetProb / (1.0 - resetProb)

		val vp = if(personalized) {
			(id: VertexId, attr: (Double, Double), msgSum: Double) => personalizedVertexProgram(id, attr, msgSum)
		} else {
			(id: VertexId, attr: (Double, Double), msgSum: Double) => vertexProgram(id, attr, msgSum)
		}

		val rankGraph = Pregel(pageRankGraph, initailMessage, activeDirection = EdgeDirection.Out)(
			vp, sendMessage, messageCombiner)
			// 取出节点属性， 第一个是pg值，第二个是误差
    		.mapVertices((vid, attr) => attr._1)
		normalizeRankSum(rankGraph, personalized)
	}

	private def normalizeRankSum(rankGraph: Graph[Double, Double], personalized: Boolean): Graph[Double, Double] = {
		val rankSum = rankGraph.vertices.values.sum()
		if(personalized){
			// 归一化
			rankGraph.mapVertices((id, rank) => rank/rankSum)
		} else {
			val numVertices = rankGraph.numVertices
			val correctionFactor = numVertices.toDouble / rankSum
			rankGraph.mapVertices((id, rank) => rank * correctionFactor)
		}

	}
}
