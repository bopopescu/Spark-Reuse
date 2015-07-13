package org.apache.spark.sql.hive

import org.apache.spark.sql.auto.cache.QGMaster
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.QNodeRef
import org.apache.spark.sql.columnar.{InMemoryColumnarTableScan, InMemoryRelation}
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by zengdan on 15-4-29.
 */
object TestSQL {
  def main(args: Array[String]) {
    System.setProperty("spark.sql.auto.cache", "true")
    //System.setProperty("spark.sql.shuffle.partitions","1")
    System.setProperty("hive.metastore.warehouse.dir", "/Users/zengdan/hive")
    System.setProperty("spark.tachyonStore.global.baseDir","/global_spark_tachyon")
    val conf = new SparkConf()
    conf.setAppName("TPCH").setMaster("local")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.tachyon.impl","tachyon.hadoop.TFS")

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    new Thread(){
      override def run(){
        QGMaster.main("--host localhost --port 7070".split(" "))
      }
    }.start()

    //Thread.sleep(5000)
    val iter = 4
    val collect = true

    for (i <- 1 to iter) {
      val res0 = sqlContext.sql("""select l_shipmode,sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
                                from orders,lineitem
                                where o_orderkey = l_orderkey and l_shipmode in ('REG AIR', 'MAIL') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= '1995-01-01' and l_receiptdate < '1996-01-01'
                                group by l_shipmode""")
      //order by l_shipmode""")

      if(collect) res0.collect()
      else println("Result count is " + res0.count())
    }
  }
}
