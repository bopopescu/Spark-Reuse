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
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.Stats
import scala.collection.mutable.Map
import org.apache.spark.rdd.CartesianWithTimeRDD

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class CartesianProduct(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output = left.output ++ right.output
  @transient lazy val (fixedSize, varIndexes) = outputSize(output)

  override def operatorMatch(plan: SparkPlan):Boolean = {
    plan match{
      case cp: CartesianProduct => true
      case _ => false
    }
  }

  override def execute() = {
    val leftResults = left.execute().map(_.copy())
    val rightResults = right.execute().map(_.copy())

    leftResults.cartesian(rightResults).mapPartitions { iter =>
      val joinedRow = new JoinedRow
      iter.map(r => joinedRow(r._1, r._2))
    }


    val loaded = sqlContext.loadData(output, nodeRef)
    if(loaded.isDefined){
      loaded.get
    }else {
      val shouldCollect = nodeRef.isDefined && nodeRef.get.collect
      val newRdd = if (!shouldCollect) {
        leftResults.cartesian(rightResults).mapPartitions { iter =>
          val joinedRow = new JoinedRow
          iter.map(r => joinedRow(r._1, r._2))
        }
      } else {
        val cartesianRdd = leftResults.cartesianWithTime(rightResults)
        cartesianRdd.mapPartitions { iter =>
          val joinedRow = new JoinedRow
          var start = 0L
          new Iterator[Row]{
            override def hasNext = {
              start = System.nanoTime()
              val continue = iter.hasNext
              time += (System.nanoTime() - start)
              if (!continue && rowCount != 0) {
                time -= cartesianRdd.asInstanceOf[CartesianWithTimeRDD[Row, Row]].getTime
                avgSize = (fixedSize + avgSize / rowCount)
                logDebug(s"CartesianProduct ${nodeRef.get.id}: $time, $rowCount, $avgSize")
                var statistics = Stats.statistics.get()
                if (statistics == null) {
                  //no match in the query graph, so no need to collect information
                  statistics = Map[Int, Array[Int]]()
                  Stats.statistics.set(statistics)
                }
                statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, avgSize * rowCount))
              }
              continue
            }
            override def next = {
              start = System.nanoTime()
              val r = iter.next()
              val res = joinedRow(r._1, r._2)
              time += (System.nanoTime() - start)
              rowCount += 1
              for (index <- varIndexes) {
                //sizeInBytes += result.getString(index).length
                avgSize += output(index).dataType.size(res, index)
              }
              res
            }
          }

        }
      }
      cacheData(newRdd, output, nodeRef)
    }
  }
}
