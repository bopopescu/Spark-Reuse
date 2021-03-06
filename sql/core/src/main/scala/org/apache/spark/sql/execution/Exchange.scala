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
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.auto.cache.QueryGraph
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.types.{ArrayType, NativeType}
import org.apache.spark.{SparkEnv, HashPartitioner, RangePartitioner, SparkConf}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.util.{Stats, MutablePair}

import java.util.HashMap
import scala.collection.mutable.Map

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Exchange(newPartitioning: Partitioning, child: SparkPlan) extends UnaryNode {

  override def outputPartitioning = newPartitioning

  override def output = child.output

  @transient lazy val (fixedSize, varIndexes) = outputSize(output)

  //this.id = child.id

  /** We must copy rows when sort based shuffle is on */
  protected def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  private val bypassMergeThreshold = SparkEnv.get.conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    //child.sqlContext.sparkContext.conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)

  override def execute():RDD[Row] = attachTree(this , "execute") {
    newPartitioning match {
      case HashPartitioning(expressions, numPartitions) =>
        // TODO: Eliminate redundant expressions in grouping key and value.
        // This is a workaround for SPARK-4479. When:
        //  1. sort based shuffle is on, and
        //  2. the partition number is under the merge threshold, and
        //  3. no ordering is required
        // we can avoid the defensive copies to improve performance. In the long run, we probably
        // want to include information in shuffle dependencies to indicate whether elements in the
        // source RDD should be copied.
        val loaded = sqlContext.loadData(output, nodeRef)
        if(loaded.isDefined){
          loaded.get
        }else {
          val shuffleCollect = nodeRef.isDefined && nodeRef.get.collect
          val rdd = if (sortBasedShuffleOn && numPartitions > bypassMergeThreshold) {
            if (shuffleCollect) {
              child.execute().mapPartitions { iter =>
                val hashExpressions = newMutableProjection(expressions, child.output)()
                new Iterator[(Row, Row)] {
                  override def hasNext = {
                    val start = System.nanoTime()
                    val continue = iter.hasNext
                    time += (System.nanoTime() - start)
                    if (!continue) {
                      var statistics = Stats.statistics.get()
                      if (statistics == null) {
                        //no match in the query graph, so no need to collect information
                        statistics = Map[Int, Array[Int]]()
                        Stats.statistics.set(statistics)
                      }
                      logDebug(s"Exchange before Shuffle: $time, $rowCount, $avgSize")
                      statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, 0))
                      /*
                    val partialStats = statistics.get(nodeRef.get.id)
                    if(!partialStats.isDefined)
                      statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, 0))
                    else{
                      val partialTime = partialStats.get(0)
                      statistics.put(nodeRef.get.id, Array((time / 1e6 - partialTime).toInt, 0))
                    }
                    */
                    }
                    continue
                  }

                  override def next = {
                    val start = System.nanoTime()
                    val r = iter.next()
                    time += (System.nanoTime() - start) //summary the time of reading child data
                    (hashExpressions(r).copy(), r.copy())
                  }
                }
              }
            } else {
              child.execute().mapPartitions { iter =>
                val hashExpressions = newMutableProjection(expressions, child.output)()
                iter.map(r => (hashExpressions(r).copy(), r.copy()))
              }
            }
          } else {
            if (shuffleCollect) {
              child.execute().mapPartitions { iter =>
                val hashExpressions = newMutableProjection(expressions, child.output)()
                val mutablePair = new MutablePair[Row, Row]()

                new Iterator[MutablePair[Row, Row]] {
                  override def hasNext = {
                    val start = System.nanoTime()
                    val continue = iter.hasNext
                    time += (System.nanoTime() - start)
                    if (!continue) {
                      var statistics = Stats.statistics.get()
                      if (statistics == null) {
                        //no match in the query graph, so no need to collect information
                        statistics = Map[Int, Array[Int]]()
                        Stats.statistics.set(statistics)
                      }
                      logDebug(s"Exchange before Shuffle: $time, $rowCount, $avgSize")
                      statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, 0))
                      /*
                    val partialStats = statistics.get(nodeRef.get.id)
                    if(!partialStats.isDefined)
                      statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, 0))
                    else{
                      val partialTime = partialStats.get(0)
                      statistics.put(nodeRef.get.id, Array((time / 1e6 - partialTime).toInt, 0))
                    }
                    */
                    }
                    continue
                  }

                  override def next = {
                    val start = System.nanoTime()
                    val r = iter.next()
                    time += (System.nanoTime() - start)
                    mutablePair.update(hashExpressions(r), r)
                  }
                }
              }
            } else {
              child.execute().mapPartitions { iter =>
                val hashExpressions = newMutableProjection(expressions, child.output)()
                val mutablePair = new MutablePair[Row, Row]()
                iter.map(r => mutablePair.update(hashExpressions(r), r))
              }
            }
          }

          if (shuffleCollect) {
            rdd.cacheID = Some(nodeRef.get.id)
          }

          val part = new HashPartitioner(numPartitions)
          val shuffled = new ShuffledRDD[Row, Row, Row](rdd, part)
          shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))
          val newRdd = if (shuffleCollect) {
            shuffled.mapPartitions { iter =>
              new Iterator[Row] {
                override def hasNext = {
                  val start = System.nanoTime()
                  val continue = iter.hasNext
                  time += (System.nanoTime() - start)
                  if (!continue && rowCount != 0) {
                    avgSize = (fixedSize + avgSize / rowCount)
                    logDebug(s"Exchange after Shuffle ${nodeRef.get.id}: $time, $rowCount, $avgSize")
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

                override def next() = {
                  val start = System.nanoTime()
                  val ret = iter.next()._2
                  time += (System.nanoTime() - start)
                  rowCount += 1
                  for (index <- varIndexes) {
                    //sizeInBytes += result.getString(index).length
                    avgSize += output(index).dataType.size(ret, index)
                  }
                  ret
                }
              }
            }
          } else {
            shuffled.map(_._2)
          }
          cacheData(newRdd, output, nodeRef)
        }

      case RangePartitioning(sortingExpressions, numPartitions) =>
        val rdd = if (sortBasedShuffleOn) {
          child.execute().mapPartitions { iter => iter.map(row => (row.copy(), null))}
        } else {
          child.execute().mapPartitions { iter =>
            val mutablePair = new MutablePair[Row, Null](null, null)
            iter.map(row => mutablePair.update(row, null))
          }
        }

        // TODO: RangePartitioner should take an Ordering.
        implicit val ordering = new RowOrdering(sortingExpressions, child.output)

        val part = new RangePartitioner(numPartitions, rdd, ascending = true)
        val shuffled = new ShuffledRDD[Row, Null, Null](rdd, part)
        shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))

        shuffled.map(_._1)

      case SinglePartition =>
        // SPARK-4479: Can't turn off defensive copy as what we do for `HashPartitioning`, since
        // operators like `TakeOrdered` may require an ordering within the partition, and currently
        // `SinglePartition` doesn't include ordering information.
        // TODO Add `SingleOrderedPartition` for operators like `TakeOrdered`
        val loaded = sqlContext.loadData(output, nodeRef)
        if(loaded.isDefined){
          loaded.get
        }else {
          val shuffleCollect = nodeRef.isDefined && nodeRef.get.collect
          val rdd = if (sortBasedShuffleOn) {
            if (shuffleCollect) {
              child.execute().mapPartitions { iter =>
                new Iterator[(Null, Row)] {
                  override def hasNext = {
                    val start = System.nanoTime()
                    val continue = iter.hasNext
                    time += (System.nanoTime() - start)
                    if (!continue) {
                      var statistics = Stats.statistics.get()
                      if (statistics == null) {
                        //no match in the query graph, so no need to collect information
                        statistics = Map[Int, Array[Int]]()
                        Stats.statistics.set(statistics)
                      }
                      logDebug(s"Exchange: $time, $rowCount, $avgSize")
                      statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, 0))
                      /*
                    val partialStats = statistics.get(nodeRef.get.id)
                    if (!partialStats.isDefined)
                      statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, 0))
                    else {
                      val partialTime = partialStats.get(0)
                      statistics.put(nodeRef.get.id, Array((time / 1e6 - partialTime).toInt, 0))
                    }
                    */
                    }
                    continue
                  }

                  override def next() = {
                    val start = System.nanoTime()
                    val r = iter.next()
                    time += (System.nanoTime() - start)
                    (null, r.copy())
                  }
                }
              }
            } else {
              child.execute().mapPartitions { iter => iter.map(r => (null, r.copy()))}
            }
          } else {
            if (shuffleCollect) {
              child.execute().mapPartitions { iter =>
                val mutablePair = new MutablePair[Null, Row]()
                new Iterator[MutablePair[Null, Row]] {
                  override def hasNext = {
                    val start = System.nanoTime()
                    val continue = iter.hasNext
                    time += (System.nanoTime() - start)
                    if (!continue) {
                      var statistics = Stats.statistics.get()
                      if (statistics == null) {
                        //no match in the query graph, so no need to collect information
                        statistics = Map[Int, Array[Int]]()
                        Stats.statistics.set(statistics)
                      }
                      logDebug(s"Exchange: $time, $rowCount, $avgSize")
                      statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, 0))
                      /*
                    val partialStats = statistics.get(nodeRef.get.id)
                    if (!partialStats.isDefined)
                      statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, 0))
                    else {
                      val partialTime = partialStats.get(0)
                      statistics.put(nodeRef.get.id, Array((time / 1e6 - partialTime).toInt, 0))
                    }
                    */
                    }
                    continue
                  }

                  override def next() = {
                    val start = System.nanoTime()
                    val r = iter.next()
                    time += (System.nanoTime() - start)
                    mutablePair.update(null, r)
                  }
                }
              }
            } else {
              child.execute().mapPartitions { iter =>
                val mutablePair = new MutablePair[Null, Row]()
                iter.map(r => mutablePair.update(null, r))
              }
            }
          }

          if (shuffleCollect) {
            rdd.cacheID = Some(nodeRef.get.id)
          }

          val partitioner = new HashPartitioner(1)
          val shuffled = new ShuffledRDD[Null, Row, Row](rdd, partitioner)
          shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))
          val newRdd = if (shuffleCollect) {
            shuffled.mapPartitions { iter =>
              new Iterator[Row] {
                override def hasNext = {
                  val start = System.nanoTime()
                  val continue = iter.hasNext
                  time += (System.nanoTime() - start)
                  if (!continue && rowCount != 0) {
                    avgSize = (fixedSize + avgSize / rowCount)
                    logDebug(s"Exchange after Shuffle ${nodeRef.get.id}: $time, $rowCount, $avgSize")
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

                override def next() = {
                  val start = System.nanoTime()
                  val ret = iter.next()._2
                  time += (System.nanoTime() - start)
                  rowCount += 1
                  for (index <- varIndexes) {
                    //sizeInBytes += result.getString(index).length
                    avgSize += output(index).dataType.size(ret, index)
                  }
                  ret
                }
              }
            }
          } else {
            shuffled.map(_._2)
          }

          cacheData(newRdd, output, nodeRef)
        }

      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }
  }
}

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[Exchange]] Operators where required.
 */
private[sql] case class AddExchange(sqlContext: SQLContext) extends Rule[SparkPlan] {
  // TODO: Determine the number of partitions.
  def numPartitions = sqlContext.numShufflePartitions

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator: SparkPlan =>
      // Check if every child's outputPartitioning satisfies the corresponding
      // required data distribution.
      def meetsRequirements =
        !operator.requiredChildDistribution.zip(operator.children).map {
          case (required, child) =>
            val valid = child.outputPartitioning.satisfies(required)
            logDebug(
              s"${if (valid) "Valid" else "Invalid"} distribution," +
                s"required: $required current: ${child.outputPartitioning}")
            valid
        }.exists(!_)

      // Check if outputPartitionings of children are compatible with each other.
      // It is possible that every child satisfies its required data distribution
      // but two children have incompatible outputPartitionings. For example,
      // A dataset is range partitioned by "a.asc" (RangePartitioning) and another
      // dataset is hash partitioned by "a" (HashPartitioning). Tuples in these two
      // datasets are both clustered by "a", but these two outputPartitionings are not
      // compatible.
      // TODO: ASSUMES TRANSITIVITY?
      def compatible =
        !operator.children
          .map(_.outputPartitioning)
          .sliding(2)
          .map {
            case Seq(a) => true
            case Seq(a,b) => a compatibleWith b
          }.exists(!_)

      // Check if the partitioning we want to ensure is the same as the child's output
      // partitioning. If so, we do not need to add the Exchange operator.
      def addExchangeIfNecessary(partitioning: Partitioning, child: SparkPlan) =
        if (child.outputPartitioning != partitioning) Exchange(partitioning, child) else child

      if (meetsRequirements && compatible) {
        operator
      } else {
        // At least one child does not satisfies its required data distribution or
        // at least one child's outputPartitioning is not compatible with another child's
        // outputPartitioning. In this case, we need to add Exchange operators.
        val repartitionedChildren = operator.requiredChildDistribution.zip(operator.children).map {
          case (AllTuples, child) =>
            addExchangeIfNecessary(SinglePartition, child)
          case (ClusteredDistribution(clustering), child) =>
            addExchangeIfNecessary(HashPartitioning(clustering, numPartitions), child)
          case (OrderedDistribution(ordering), child) =>
            addExchangeIfNecessary(RangePartitioning(ordering, numPartitions), child)
          case (UnspecifiedDistribution, child) => child
          case (dist, _) => sys.error(s"Don't know how to ensure $dist")
        }
        operator.withNewChildren(repartitionedChildren)
      }
  }
}
