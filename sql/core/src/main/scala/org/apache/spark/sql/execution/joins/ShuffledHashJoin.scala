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

package org.apache.spark.sql.execution.joins

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.QNodeRef
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Partitioning}
import org.apache.spark.sql.execution.{Exchange, BinaryNode, SparkPlan}
import org.apache.spark.util.collection.CompactBuffer

/**
 * :: DeveloperApi ::
 * Performs an inner hash join of two child relations by first shuffling the data using the join
 * keys.
 */
@DeveloperApi
case class ShuffledHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan,
    optionRef: Option[QNodeRef] = None)
  extends BinaryNode with HashJoin {
  //this.nodeRef = optionRef

  override def operatorMatch(plan: SparkPlan):Boolean = {
    plan match{
      case hj: HashJoin =>
        (this.compareExpressions(leftKeys, hj.leftKeys) &&
          this.compareExpressions(rightKeys, hj.rightKeys)) ||
          (this.compareExpressions(leftKeys, hj.rightKeys) &&
            this.compareExpressions(rightKeys, hj.leftKeys))
      case _ => false
    }
  }

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def execute():RDD[Row] = {
    //if(buildPlan.isInstanceOf[Exchange])
    //  buildPlan.nodeRef = this.nodeRef
    //if(streamedPlan.isInstanceOf[Exchange])
    //  streamedPlan.nodeRef = this.nodeRef

    val loaded = sqlContext.loadData(output, nodeRef)
    if(loaded.isDefined){
      loaded.get
    }else {
      val shouldCollect = nodeRef.isDefined && nodeRef.get.collect
      val newRdd = if (!shouldCollect) {
        buildPlan.execute().zipPartitions(streamedPlan.execute()) { (buildIter, streamIter) =>
          val hashed = HashedRelation(buildIter, buildSideKeyGenerator)
          hashJoin(streamIter, hashed)
        }
      } else {
        buildPlan.execute().zipPartitions(streamedPlan.execute()) { (buildIter, streamIter) =>

          val (hashed, partialTime) = HashedRelation.createRelation(buildIter, buildSideKeyGenerator)
          time += partialTime
          hashJoinWithCollect(streamIter, hashed)
        }
      }

      cacheData(newRdd, output, nodeRef)
    }
  }
}
