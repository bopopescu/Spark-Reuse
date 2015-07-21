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

import java.util.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.{SparkEnv, SparkContext}
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.plans.logical.QNodeRef
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Stats
import scala.collection.mutable.Map

/**
 * :: DeveloperApi ::
 * Groups input data by `groupingExpressions` and computes the `aggregateExpressions` for each
 * group.
 *
 * @param partial if true then aggregation is done partially on local data without shuffling to
 *                ensure all values where `groupingExpressions` are equal are present.
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param child the input data source.
 */
@DeveloperApi
case class Aggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan, optionRef: Option[QNodeRef] = None)
  extends UnaryNode {

  //nodeRef = optionRef
  //if(!partial)
  //  child.nodeRef = optionRef

  override def operatorMatch(plan: SparkPlan):Boolean = plan match{
      case agg: Aggregate => this.partial == agg.partial &&
        this.compareExpressions(groupingExpressions.map(_.transformExpression()),
          agg.groupingExpressions.map(_.transformExpression())) &&
        this.compareExpressions(aggregateExpressions.map(_.transformExpression()),
          agg.aggregateExpressions.map(_.transformExpression()))
      case _ => false
  }

  //zengdan
  @transient lazy val (fixedSize: Int, varIndexes: List[Int]) = outputSize(output) //if(partial) (0,Nil) else

  override def requiredChildDistribution =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.
  private[this] val childOutput = child.output

  override def output = aggregateExpressions.map(_.toAttribute)

  /**
   * An aggregate that needs to be computed for each row in a group.
   *
   * @param unbound Unbound version of this aggregate, used for result substitution.
   * @param aggregate A bound copy of this aggregate used to create a new aggregation buffer.
   * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
   */
  case class ComputedAggregate(
      unbound: AggregateExpression,
      aggregate: AggregateExpression,
      resultAttribute: AttributeReference)

  /** A list of aggregates that need to be computed for each group. */
  private[this] val computedAggregates = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: AggregateExpression =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, childOutput),
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
    }
  }.toArray

  /** The schema of the result of all aggregate evaluations */
  private[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group. */
  private[this] def newAggregateBuffer(): Array[AggregateFunction] = {
    val buffer = new Array[AggregateFunction](computedAggregates.length)
    var i = 0
    while (i < computedAggregates.length) {
      buffer(i) = computedAggregates(i).aggregate.newInstance()
      i += 1
    }
    buffer
  }

  /** Named attributes used to substitute grouping attributes into the final result. */
  private[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
   * A map of substitutions that are used to insert the aggregate expressions and grouping
   * expression into the final result expression.
   */
  private[this] val resultMap =
    (computedAggregates.map { agg => agg.unbound -> agg.resultAttribute } ++ namedGroups).toMap

  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  private[this] val resultExpressions = aggregateExpressions.map { agg =>
    agg.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }

  private def getHashTable(iter: Iterator[Row], collect: Boolean = false):HashMap[Row, Array[AggregateFunction]] = {
    val start = System.nanoTime()
    val hashTable = new HashMap[Row, Array[AggregateFunction]]
    val groupingProjection = new InterpretedMutableProjection(groupingExpressions, childOutput)
    if(collect)
      time += (System.nanoTime() - start)

    var currentRow: Row = null
    while (iter.hasNext) {
      currentRow = iter.next()
      val start = System.nanoTime()
      val currentGroup = groupingProjection(currentRow)
      var currentBuffer = hashTable.get(currentGroup)
      if (currentBuffer == null) {
        currentBuffer = newAggregateBuffer()
        hashTable.put(currentGroup.copy(), currentBuffer)
      }

      var i = 0
      while (i < currentBuffer.length) {
        currentBuffer(i).update(currentRow)
        i += 1
      }
      if(collect)
        time += (System.nanoTime() - start)
    }

    hashTable
  }

  override def execute():RDD[Row] = attachTree(this, "execute") {
      if (groupingExpressions.isEmpty) {
        val loaded = sqlContext.loadData(output, nodeRef)
        if(loaded.isDefined){
          loaded.get
        }else {
          val shouldCollect = nodeRef.isDefined && nodeRef.get.collect
          val newRdd = if (shouldCollect) {
            child.execute().mapPartitions { iter =>
              var start = System.nanoTime()
              val buffer = newAggregateBuffer()
              time += (System.nanoTime() - start)
              var currentRow: Row = null
              while (iter.hasNext) {
                currentRow = iter.next()
                start = System.nanoTime()
                var i = 0
                while (i < buffer.length) {
                  buffer(i).update(currentRow)
                  i += 1
                }
                time += (System.nanoTime() - start)
              }

              /*
            if(partial){ //don't collect the time of reading parent data
              while (iter.hasNext) {
                currentRow = iter.next()
                start = System.nanoTime()
                var i = 0
                while (i < buffer.length) {
                  buffer(i).update(currentRow)
                  i += 1
                }
                time += (System.nanoTime() - start)
              }
            }else{
              start = System.nanoTime()
              while (iter.hasNext) {
                currentRow = iter.next()
                var i = 0
                while (i < buffer.length) {
                  buffer(i).update(currentRow)
                  i += 1
                }
              }
              time += (System.nanoTime() - start)
            }
            */

              start = System.nanoTime()
              val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
              val aggregateResults = new GenericMutableRow(computedAggregates.length)

              var i = 0
              while (i < buffer.length) {
                aggregateResults(i) = buffer(i).eval(EmptyRow)
                i += 1
              }
              val result = resultProjection(aggregateResults)
              time += (System.nanoTime() - start)
              rowCount = 1
              for (index <- varIndexes) {
                //sizeInBytes += result.getString(index).length
                avgSize += output(index).dataType.size(result, index)
              }
              avgSize += fixedSize

              //if(!partialCollect) {
              var statistics = Stats.statistics.get()
              if (statistics == null) {
                statistics = Map[Int, Array[Int]]()
                Stats.statistics.set(statistics)
              }
              logDebug(s"Aggregate: $time, $rowCount, $avgSize")
              statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, avgSize * rowCount))
              /*
            if (partial)
              statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, 0))
            else {
              statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, avgSize * rowCount))
            }
            */

              Iterator(result)
            }
          } else {
            child.execute().mapPartitions { iter =>
              val buffer = newAggregateBuffer()
              var currentRow: Row = null
              while (iter.hasNext) {
                currentRow = iter.next()
                var i = 0
                while (i < buffer.length) {
                  buffer(i).update(currentRow)
                  i += 1
                }
              }
              val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
              val aggregateResults = new GenericMutableRow(computedAggregates.length)

              var i = 0
              while (i < buffer.length) {
                aggregateResults(i) = buffer(i).eval(EmptyRow)
                i += 1
              }

              Iterator(resultProjection(aggregateResults))
            }
          }
          cacheData(newRdd, output, nodeRef)
        }
      } else {
        val loaded = sqlContext.loadData(output, nodeRef)
        if(loaded.isDefined){
          loaded.get
        }else {
          val shouldCollect = nodeRef.isDefined && nodeRef.get.collect
          val newRdd = if (shouldCollect) {
            child.execute().mapPartitions { iter =>
              val hashTable = getHashTable(iter, true)
              /*
            if(partial)getHashTable(iter, true) else{
              val start = System.nanoTime()
              val table = getHashTable(iter)
              time += (System.nanoTime() - start)
              table
            }
            */

              new Iterator[Row] {
                val start = System.nanoTime()
                private[this] val hashTableIter = hashTable.entrySet().iterator()
                private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
                private[this] val resultProjection =
                  new InterpretedMutableProjection(
                    resultExpressions, computedSchema ++ namedGroups.map(_._2))
                private[this] val joinedRow = new JoinedRow4
                time += (System.nanoTime() - start)

                override final def hasNext: Boolean = {
                  val continue = hashTableIter.hasNext
                  if (!continue && rowCount != 0) {
                    avgSize = (fixedSize + avgSize / rowCount)
                    logDebug(s"Aggregate: $time, $rowCount, $avgSize")
                    var statistics = Stats.statistics.get()
                    if (statistics == null) {
                      statistics = Map[Int, Array[Int]]()
                      Stats.statistics.set(statistics)
                    }
                    statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, avgSize * rowCount))
                    /*
                  if(partial) {
                    statistics.put(nodeRef.get.id, Array((time/1e6).toInt, 0))
                  }else
                    statistics.put(nodeRef.get.id, Array((time/1e6).toInt, avgSize * rowCount))
                  */
                  }
                  continue
                }

                override final def next(): Row = {
                  val begin = System.nanoTime()
                  val currentEntry = hashTableIter.next()
                  val currentGroup = currentEntry.getKey
                  val currentBuffer = currentEntry.getValue

                  var i = 0
                  while (i < currentBuffer.length) {
                    // Evaluating an aggregate buffer returns the result.  No row is required since we
                    // already added all rows in the group using update.
                    aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
                    i += 1
                  }
                  val result = resultProjection(joinedRow(aggregateResults, currentGroup))
                  time += (System.nanoTime() - begin)
                  rowCount += 1
                  ///*
                  for (index <- varIndexes) {
                    //sizeInBytes += result.getString(index).length
                    avgSize += output(index).dataType.size(result, index)
                  }
                  //*/
                  result
                }
              }
            }
          } else {
            child.execute().mapPartitions { iter =>
              val hashTable = new HashMap[Row, Array[AggregateFunction]]
              val groupingProjection = new InterpretedMutableProjection(groupingExpressions, childOutput)

              var currentRow: Row = null
              while (iter.hasNext) {
                currentRow = iter.next()
                val currentGroup = groupingProjection(currentRow)
                var currentBuffer = hashTable.get(currentGroup)
                if (currentBuffer == null) {
                  currentBuffer = newAggregateBuffer()
                  hashTable.put(currentGroup.copy(), currentBuffer)
                }

                var i = 0
                while (i < currentBuffer.length) {
                  currentBuffer(i).update(currentRow)
                  i += 1
                }
              }

              new Iterator[Row] {
                private[this] val hashTableIter = hashTable.entrySet().iterator()
                private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
                private[this] val resultProjection =
                  new InterpretedMutableProjection(
                    resultExpressions, computedSchema ++ namedGroups.map(_._2))
                private[this] val joinedRow = new JoinedRow4

                override final def hasNext: Boolean = hashTableIter.hasNext

                override final def next(): Row = {
                  val currentEntry = hashTableIter.next()
                  val currentGroup = currentEntry.getKey
                  val currentBuffer = currentEntry.getValue

                  var i = 0
                  while (i < currentBuffer.length) {
                    // Evaluating an aggregate buffer returns the result.  No row is required since we
                    // already added all rows in the group using update.
                    aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
                    i += 1
                  }
                  resultProjection(joinedRow(aggregateResults, currentGroup))
                }
              }
            }
          }
          cacheData(newRdd, output, nodeRef)
        }

        /*
        if(nodeRef.isDefined && nodeRef.get.cache && !partial){
          //newRdd.cacheID = Some(nodeRef.get.id)
          //newRdd = newRdd.sparkContext.saveAndLoadOperatorFile[Row](newRdd.cacheID.get, newRdd.map(ScalaReflection.convertRowToScala(_, schema)))
          newRdd = sqlContext.cacheOrLoadData(newRdd, output, nodeRef.get.id)._1
        }
        newRdd
        */
      }
  }
}
