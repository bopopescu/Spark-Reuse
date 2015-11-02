package org.apache.spark.rdd

import org.apache.hadoop.mapred.InputSplit
import org.apache.spark._
import org.apache.spark.storage.{RDDBlockId, BlockId}
import tachyon.thrift.ClientFileInfo

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import java.util.List

/**
 * Created by zengdan on 15-7-21.
 */
private[spark] class TachyonPartition(idx: Int)
  extends Partition {
  override val index: Int = idx
}

class TachyonRDD[T: ClassTag](sc: SparkContext, operatorId: Int)
  extends RDD[T](sc, Nil){
  @transient private lazy val tachyonStore = SparkEnv.get.blockManager.tachyonStore

  override def getPartitions: Array[Partition] = {
    val files: List[ClientFileInfo] = tachyonStore.listStatus(operatorId)
    val ps = new Array[Partition](files.size())
    var i = 0
    while(i < files.size()){
      ps(i) = new TachyonPartition(files.get(i).name.split("_").last.toInt)
      i += 1
    }
    ps
  }

  override def compute(split: Partition, context: TaskContext) = {
    val fileName = "operator_" + operatorId + "_" + split.index
    logInfo("Input Split: " + fileName)
    tachyonStore.getValues(new RDDBlockId(operatorId, split.index, Some(operatorId)))
      .getOrElse(Iterator.empty).asInstanceOf[Iterator[T]]
  }
}

