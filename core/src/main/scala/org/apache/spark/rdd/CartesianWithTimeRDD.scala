package org.apache.spark.rdd

import org.apache.spark.{TaskContext, Partition, SparkContext}

import scala.reflect.ClassTag

/**
 * Created by zengdan on 15-7-21.
 */
class CartesianWithTimeRDD[T: ClassTag, U: ClassTag] (
  sc: SparkContext,
  var r1 : RDD[T],
  var r2 : RDD[U])
  extends CartesianRDD[T, U](sc, r1, r2) with Serializable{

  @transient private var time: Long = 0L

  override def compute(split: Partition, context: TaskContext) = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    val iter1 = rdd1.iterator(currSplit.s1, context)
    var iter2 = rdd2.iterator(currSplit.s2, context)

    new Iterator[(T, U)]{
      private var diter = iter2.duplicate
      private var cp2 = diter._1
      private var curVal1: Option[T] = None
      iter2 = diter._2
      private var start = 0L

      override def hasNext = {
        start = System.nanoTime()
        var i2Empty = !cp2.hasNext
        time += (System.nanoTime() - start)
        if(i2Empty || !curVal1.isDefined){
          start = System.nanoTime()
          val i1Empty = iter1.hasNext
          time += (System.nanoTime() - start)
          curVal1 = if(i1Empty){
            if(i2Empty) {
              diter = iter2.duplicate
              cp2 = diter._1
              iter2 = diter._2
              start = System.nanoTime()
              i2Empty = !cp2.hasNext
              time += (System.nanoTime() - start)
            }
            start = System.nanoTime()
            val res = Some(iter1.next())
            time += (System.nanoTime() - start)
            res
          }else{
            None
          }
        }

        curVal1.isDefined && !i2Empty
      }

      override def next = {
        start = System.nanoTime()
        val res = (curVal1.get, cp2.next())
        time += (System.nanoTime() - start)
        res
      }
    }


  }

  def getTime = time
}


