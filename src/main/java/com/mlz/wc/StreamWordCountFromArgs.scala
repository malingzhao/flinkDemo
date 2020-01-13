package com.mlz.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/13
 * @描述： 
 */


object StreamWordCountFromArgs {

  def main(args: Array[String]): Unit = {


    val params = ParameterTool.fromArgs(args)

    val host:String=params.get("host")
    val port:Int = params.getInt("port")
    //创建一个流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //接收socket的数据
    val textDataStream = env.socketTextStream(host,port)


    //逐一读取数据  打散之后进行wordcount
    val wordCountStream = textDataStream
      //转义 代表空格
      .flatMap(_.split("\\s"))
      //不为空
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)



    //打印输出
    wordCountStream.print()
      //设置并行度
      .setParallelism(1)

    env.execute("stream word count job")
  }
}
