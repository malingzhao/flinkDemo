package com.mlz.wc

import org.apache.flink.api.scala._

/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/13
 * @描述： 
 */


//批处理代码
object WordCount {

  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    //从文件中读取数据
    val inputPath = "D:\\workspace\\flink\\flinkDemo\\src\\main\\resources\\hello.txt"
    val inputDataSet =env.readTextFile(inputPath)


    //分词之后做count
    // flatMap把一行打散
    val wordCounDataSet = inputDataSet.flatMap(_.split(" "))
      .map((_,1))
    //按照第0个元素分组
      .groupBy(0)
      .sum(1)

    //打印输出
    wordCounDataSet.print()

    //
  }



}
