package learn.util

import java.io.PrintWriter
import java.net.ServerSocket
import java.util.Random

import scala.io.Source

/**
  *  这个类用于模拟流数据生产器，通过读取文件，然后发送到指定的服务器端口
  */
object DataSourceSocket {
	def index(length : Int) = {
		val random = new Random
		random.nextInt(length)
	}

	def main(args: Array[String]): Unit = {
		if(args.length != 3){
			System.err.println("Usage: <filename> <port> <millisecond>")
			System.exit(1)
		}

		val fileName = args(0)
		val lines = Source.fromFile(fileName, "utf-8").getLines().toList
		val rowCount = lines.length

		val listener = new ServerSocket(args(1).toInt)
		while (true){
			// 无限阻塞等待监听 客户端连接
			val socket = listener.accept()
			new Thread(){
				override def run(): Unit ={
					println("Got client connected from: " + socket.getInetAddress)
					val out = new PrintWriter(socket.getOutputStream, true)
					var flag = true
					while (flag){
						Thread.sleep(args(2).toLong)
						val content = lines(index(rowCount))
						println(content)
						out.write(content + '\n')
						out.flush()
						if(!socket.isConnected){
							flag = false
						}
					}
					socket.close()
				}
			}.start()
		}
		listener.close()
	}
}
