package polaris.kafka.util


import kafka.common.TopicAndPartition
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException}
import org.apache.zookeeper.data.Stat
import polaris.utils.JsonUtils._

import scala.language.implicitConversions

/**
  * Created by chengli at 07/12/2017
  */
object ZkUtils {
  val ConsumersPath = "/consumers"
  val BrokerIdsPath = "/brokers/ids"
  val BrokerTopicsPath = "/brokers/topics"
  val TopicConfigPath = "/config/topics"
  val TopicConfigChangesPath = "/config/changes"
  val ControllerPath = "/controller"
  val ControllerEpochPath = "/controller_epoch"
  val ReassignPartitionsPath = "/admin/reassign_partitions"
  val DeleteTopicsPath = "/admin/delete_topics"
  val PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election"
  val AdminPath = "/admin"

  def getTopicPath(topic: String): String = {
    BrokerTopicsPath + "/" + topic
  }

  def getTopicPartitionsPath(topic: String): String = {
    getTopicPath(topic) + "/partitions"
  }

  def getTopicConfigPath(topic: String): String = {
    TopicConfigPath + "/" + topic
  }

  def getDeleteTopicPath(topic: String): String = {
    DeleteTopicsPath + "/" + topic
  }

  /**
    * Update the value of a persistent node with the given path and data.
    * create parent directory if necessary. Never throw NodeExistException.
    * Return the updated path zkVersion
    */
  def updatePersistentPath(curator: CuratorFramework, path: String, ba: Array[Byte], version: Int = -1): Any = {
    try {
      curator.setData().withVersion(version).forPath(path, ba)
    } catch {
      case _: NoNodeException =>
        try {
          createPersistentPath(curator, path, ba)
        } catch {
          case _: NodeExistsException =>
            curator.setData().forPath(path, ba)
          case e2: Throwable => throw e2
        }
      case e2: Throwable => throw e2
    }
  }

  def createPersistentPath(curator: CuratorFramework, path: String): Unit = {
    curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path)
  }

  /**
    * Create a persistent node with the given path and data. Create parents if necessary.
    */
  def createPersistentPath(curator: CuratorFramework, path: String, data: Array[Byte]): Unit = {
    curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, data)
  }

  /**
    * Get JSON partition to replica map from zookeeper.
    */
  def replicaAssignmentZkData(map: Map[String, Seq[Int]]): String = {
    toJson(Map("version" -> 1, "partitions" -> map))
  }

  def readData(curator: CuratorFramework, path: String): (String, Stat) = {
    val stat: Stat = new Stat()
    val dataStr: String = curator.getData.storingStatIn(stat).forPath(path)
    (dataStr, stat)
  }

  def readDataMaybeNull(curator: CuratorFramework, path: String): (Option[String], Stat) = {
    val stat: Stat = new Stat()
    try {
      val dataStr: String = curator.getData.storingStatIn(stat).forPath(path)
      (Option(dataStr), stat)
    } catch {
      case _: NoNodeException => (None, stat)
      case e2: Throwable => throw e2
    }
  }


  def getPartitionReassignmentZkData(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]): String = {
    val data = Map(
      "version" -> 1,
      "partitions" -> partitionsToBeReassigned.map { case (topicAndPartition, partitions) =>
        Map("topic" -> topicAndPartition.topic, "partition" -> topicAndPartition.partition, "replicas" -> partitions)
      }
    )

    toJson(data)
  }
}

