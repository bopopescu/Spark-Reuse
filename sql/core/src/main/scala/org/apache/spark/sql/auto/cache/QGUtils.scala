package org.apache.spark.sql.auto.cache

import org.apache.spark.sql.catalyst.plans.logical.{QNodeRef, LogicalPlan}
import org.apache.spark.util.SerializableBuffer

import scala.collection.mutable.{ArrayBuffer, HashMap}
import java.nio.ByteBuffer

/**
 * Created by zengdan on 15-3-25.
 */
sealed trait QGUtils extends Serializable

object QGUtils{
  case class PlanDesc(appId: String, jars: HashMap[String, Long], serializedPlan: SerializableBuffer)
  case class PlanUpdate(refs: HashMap[Int, QNodeRef], varNodes: HashMap[Int, ArrayBuffer[NodeDesc]])
  case class NodeDesc(nodeRef: QNodeRef, args: AnyRef*)
}
