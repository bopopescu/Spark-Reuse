/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.{ScalaReflection, trees}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.QNodeRef
import org.apache.spark.sql.catalyst.plans.physical._

import org.apache.spark.sql.catalyst.types.{StringType, NativeType, DataType}

import scala.collection.mutable.ArrayBuffer


object SparkPlan {
  protected[sql] val currentContext = new ThreadLocal[SQLContext]()
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {
  self: Product =>

  //zengdan
  protected[spark]  final val serialVersionUID = 273157889063959800L
  protected[spark] var rowCount:Int = 0
  protected[spark] var avgSize:Int = 0
  protected[spark] var time: Long = 0
  //protected[spark] var partialCollect: Boolean = false
  var id: Int = -1
  var nodeRef: Option[QNodeRef] = None

  //zengdan
  lazy val transformedExpressions: Seq[Expression] = {
    val x = this.expressions.foldLeft(new ArrayBuffer[Expression]){
      (buffers, expression) =>
        buffers.append(expression.transformExpression())
        buffers
    }
    x.toSeq
  }

  //zengdan
  def operatorMatch(plan: SparkPlan):Boolean = {
    //(plan.getClass == this.getClass) && compareExpressions(plan.expressions, this.expressions)
    (plan.getClass == this.getClass) && compareExpressions(plan.transformedExpressions, this.transformedExpressions)
  }

  def compareExpressions(expr1: Seq[Expression], expr2: Seq[Expression]): Boolean ={
    val e1 = expr1.map(_.treeStringByName).sortWith(_.compareTo(_) < 0)
    val e2 = expr2.map(_.treeStringByName).sortWith(_.compareTo(_) < 0)

    if(e1.length != e2.length)
      false
    else if(e1.length == 0)
      true
    else{
      var i = 0
      while(i < e1.length){
        val x = e1(i)
          //.treeStringByName
        val y = e2(i)
            //.treeStringByName
        if(x.compareTo(y) != 0)
          return false
        i += 1
      }
      i != 0
    }
  }


  /**
   * A handle to the SQL Context that was used to create this plan.   Since many operators need
   * access to the sqlContext for RDD operations or configuration this field is automatically
   * populated by the query planning infrastructure.
   */
  //zengdan add lazy
  @transient
  protected[spark] lazy val sqlContext = SparkPlan.currentContext.get()

  protected def sparkContext = sqlContext.sparkContext

  // sqlContext will be null when we are being deserialized on the slaves.  In this instance
  // the value of codegenEnabled will be set by the desserializer after the constructor has run.
  val codegenEnabled: Boolean = if (sqlContext != null) {
    sqlContext.codegenEnabled
  } else {
    false
  }

  /** Overridden make copy also propogates sqlContext to copied plan. */
  override def makeCopy(newArgs: Array[AnyRef]): this.type = {
    SparkPlan.currentContext.set(sqlContext)
    super.makeCopy(newArgs)
  }

  // TODO: Move to `DistributedPlan`
  /** Specifies how data is partitioned across different nodes in the cluster. */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!
  /** Specifies any partition requirements on the input data for this operator. */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /**
   * Runs this query returning the result as an RDD.
   */
  def execute(): RDD[Row]

  /**
   * Runs this query returning the result as an array.
   */
  def executeCollect(): Array[Row] = {
    execute().map(ScalaReflection.convertRowToScala(_, schema)).collect()
  }

  protected def newProjection(
      expressions: Seq[Expression], inputSchema: Seq[Attribute]): Projection = {
    log.debug(
      s"Creating Projection: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if (codegenEnabled) {
      GenerateProjection(expressions, inputSchema)
    } else {
      new InterpretedProjection(expressions, inputSchema)
    }
  }

  protected def newMutableProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute]): () => MutableProjection = {
    log.debug(
      s"Creating MutableProj: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if(codegenEnabled) {
      GenerateMutableProjection(expressions, inputSchema)
    } else {
      () => new InterpretedMutableProjection(expressions, inputSchema)
    }
  }

  //zengdan
  protected def outputSize(schema: Seq[Attribute]):(Int, List[Int]) = {
    var fixedSize = 0
    var i = 0

    var varIndex: List[Int] = Nil
    while(i < schema.size){
      if(schema(i).dataType.isInstanceOf[NativeType]){  //only support nativetype,other:array,map,struct
        var tp = NativeType.defaultSizeOf(schema(i).dataType.asInstanceOf[NativeType])
        if(tp == 4096) {
          varIndex = varIndex ::: List(i)
        }else{
          fixedSize += tp
        }
      }else{
        varIndex = varIndex ::: List(i)
      }
      i+=1
    }
    (fixedSize, varIndex)
  }

  //zengdan
  protected def cacheData(newRdd: RDD[Row], output: Seq[Attribute], nodeRef: Option[QNodeRef]):RDD[Row] = {
    if(nodeRef.isDefined && nodeRef.get.cache) {
      newRdd.cacheID = Some(nodeRef.get.id)
      return sqlContext.cacheData(newRdd, output, nodeRef.get.id)
    }else {
      newRdd
    }
  }


  protected def newPredicate(
      expression: Expression, inputSchema: Seq[Attribute]): (Row) => Boolean = {
    if (codegenEnabled) {
      GeneratePredicate(expression, inputSchema)
    } else {
      InterpretedPredicate(expression, inputSchema)
    }
  }

  protected def newOrdering(order: Seq[SortOrder], inputSchema: Seq[Attribute]): Ordering[Row] = {
    if (codegenEnabled) {
      GenerateOrdering(order, inputSchema)
    } else {
      new RowOrdering(order, inputSchema)
    }
  }
}

private[sql] trait LeafNode extends SparkPlan with trees.LeafNode[SparkPlan] {
  self: Product =>
}

private[sql] trait UnaryNode extends SparkPlan with trees.UnaryNode[SparkPlan] {
  self: Product =>
  override def outputPartitioning: Partitioning = child.outputPartitioning
}

private[sql] trait BinaryNode extends SparkPlan with trees.BinaryNode[SparkPlan] {
  self: Product =>
}
