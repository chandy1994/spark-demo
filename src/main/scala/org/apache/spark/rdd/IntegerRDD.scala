package org.apache.spark.rdd

import com.chandy.spark.rdd.IntegerData
import org.apache.spark.internal.Logging
import org.apache.spark.util.NextIterator
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.GenMap

/**
  * @Author: chandy1994
  * @Date: 2019/06/12 0:35
  */
class IntegerRDD(sc: SparkContext) extends RDD[java.lang.Integer](sc, Nil) with Logging {

  override protected def getPartitions: Array[Partition] = {
    (0 until 10).map { i =>
      val start = i * 10000
      val end = (i + 1) * 10000 - 1
      new IntegerPartition(i, start.toInt, end.toInt)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[java.lang.Integer] = new NextIterator[Integer] {
//    context.addTaskCompletionListener { context => closeIfNeeded() }

    private val integerPartition = split.asInstanceOf[IntegerPartition]
    var integerData = new IntegerData(integerPartition.index, integerPartition.lower, integerPartition.upper)

    /**
      * 注意：
      * 1.
      *
      * @return
      */
    override protected def getNext(): Integer = {
      if (integerData.hasNext) {
        integerData.next()
      } else {
        finished = true
        null.asInstanceOf[Integer]
      }
    }

    /**
      * 关闭资源，但是spark无法保证资源一定被关闭
      */
    override protected def close(): Unit = {
    }

  }

}

private[spark] class IntegerPartition(idx: Int, val lower: Int, val upper: Int) extends Partition {
  override def index: Int = idx
}
