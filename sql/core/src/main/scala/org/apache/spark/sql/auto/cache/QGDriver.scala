package org.apache.spark.sql.auto.cache

/**
 * Created by zengdan on 15-3-23.
 */

import java.io.{DataOutputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util
import java.util.HashMap
import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.actor.ActorRef
import akka.remote.{DisassociatedEvent, AssociatedEvent, RemotingLifecycleEvent}
import org.apache.spark.SparkEnv._
import org.apache.spark.scheduler.Task
import org.apache.spark._
import org.apache.spark.serializer.{Serializer, SerializerInstance}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.QNodeRef
import org.apache.spark.sql.auto.cache.QGMasterMessages._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.storage.BlockManagerMessages.{UpdateBlockInfo, RemoveRdd}
import org.apache.spark.util.{SerializableBuffer, Utils, SignalLogger, AkkaUtils}
import scala.collection.mutable.Map
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.auto.cache.QGUtils.PlanDesc

class QGDriver(sc: SparkContext) extends Actor with Logging{

  //public
  private var qgmaster: ActorRef = _
  private val conf = sc.getConf

  override def preStart() = {
    val timeout = AkkaUtils.lookupTimeout(conf)
    qgmaster = Await.result(context.actorSelection(QGMaster.toAkkaUrl).resolveOne(timeout), timeout)
    logInfo(s"Actor address in QGDriver is ${qgmaster}")
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
  }

  override def receive = {
    case MatchSerializedPlan(planDesc) =>
      logInfo("Got serialized plan in QGDriver")
      sender ! rewritePlan(planDesc)

    case UpdateInfo(stats) =>
      logInfo("Update Statistics in QGDriver")
      sender ! updateStats(stats)

    case CacheFailed(id) =>
      logInfo("Cache Failed in QGDriver")
      sender ! cacheFailed(id)

    case RemoveJars(jars) =>
      logInfo("Remove jars in QGDriver")
      sender ! askMasterWithReply[Boolean](RemoveJars(jars))

    case AssociatedEvent(localAddress, remoteAddress, inbound) =>
      logInfo(s"Successfully connected to $remoteAddress")

    case DisassociatedEvent(_, address, _) => {
      logInfo(s"$address got disassociated.")

    }
  }

  override def postStop() {
    qgmaster ! RemoveJars(sc.addedJars)
    logInfo("PostStop in QGDriver")
  }

  def rewritePlan(planDesc: PlanDesc): HashMap[Int, QNodeRef] = {
    askMasterWithReply[HashMap[Int, QNodeRef]](MatchSerializedPlan(planDesc))
  }

  def cacheFailed(id: Int):Boolean = {
    askMasterWithReply[Boolean](CacheFailed(id))
  }

  def updateStats(stats: Map[Int, Array[Long]]) = {
    askMasterWithReply[Boolean](UpdateInfo(stats))
  }

  private def askMasterWithReply[T](message: Any): T = {
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[T](message, qgmaster,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), timeout)
  }

}

object QGDriver{
  def createActor(sc: SparkContext): (ActorSystem, ActorRef) = {
    val conf = sc.getConf
    val securityMgr = new SecurityManager(conf)
    val hostname = conf.get("spark.driver.host", Utils.localHostName())
    val (actorSystem, _) = AkkaUtils.createActorSystem("sqlDriver", hostname, 7072, conf,
      securityMgr)
    (actorSystem, actorSystem.actorOf(Props(new QGDriver(sc)), "QGDriver"))
  }

  def rewrittenPlan(plan: SparkPlan, sqlContext: SQLContext, actor: ActorRef): HashMap[Int, QNodeRef] = {
    //QueryGraph.qg.planRewritten(plan)
    ///*
    val conf = sqlContext.sparkContext.getConf
    val appId = sqlContext.sparkContext.applicationId  //make subdir in qgmaster to store jars

    val serializer = sqlContext.serializer

    val planBuffer = ByteBuffer.wrap(serializer.newInstance().serialize(plan).array())
    val plandesc = new PlanDesc(appId, sqlContext.sparkContext.addedJars, new SerializableBuffer(planBuffer))

    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout", "60").toLong, "seconds")
    val message = MatchSerializedPlan(plandesc)
    AkkaUtils.askWithReply[HashMap[Int, QNodeRef]](message, actor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), timeout)
    //*/
  }

  def updateStats(stats: Map[Int, Array[Long]], sqlContext: SQLContext, actor: ActorRef): Boolean = {
    val conf = sqlContext.sparkContext.getConf
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[Boolean](UpdateInfo(stats), actor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), timeout)
  }

  def cacheFailed(operatorId: Int, actor: ActorRef) = {
    actor ! CacheFailed(operatorId)
  }

  def removeJars(sqlContext: SQLContext, actor: ActorRef): Boolean = {
    val sc = sqlContext.sparkContext
    val conf = sc.getConf
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[Boolean](RemoveJars(sc.addedJars), actor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), timeout)
  }

}
