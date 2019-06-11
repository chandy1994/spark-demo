package com.chandy.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: chandy1994
  * @Date: 2019/06/12 0:27
  */
object Application {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(1, 2, 3))
    print(rdd1.reduce(_ + _))
  }

}
