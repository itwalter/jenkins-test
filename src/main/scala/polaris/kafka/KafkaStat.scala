package polaris.kafka

import javax.annotation.PostConstruct

import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.cache._
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import polaris.kafka.config.CuratorConfig
import polaris.kafka.jmx.metric.BrokerMetrics
import polaris.kafka.jmx.{KafkaJMX, KafkaMetrics}
import polaris.kafka.model._
import polaris.kafka.util.ZkUtils
import polaris.utils.{JsonUtils, Logging}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

/**
  * Created by chengli at 07/12/2017
  */
@Service
class KafkaStat extends Logging {
  @Autowired
  var curatorConfig: CuratorConfig = _
  var curator: CuratorFramework = _

  private[this] var brokersPathCache: PathChildrenCache = _
  private[this] var topicsConfigPathCache: PathChildrenCache = _
  private[this] var topicsTreeCache: TreeCache = _

  @PostConstruct
  def initialize(): Unit = {
    curator = CuratorFrameworkFactory.newClient(
      curatorConfig.connect,
      new BoundedExponentialBackoffRetry(
        curatorConfig.baseSleepTimeMs,
        curatorConfig.maxSleepTimeMs,
        curatorConfig.maxRetry)
    )

    brokersPathCache = new PathChildrenCache(curator, ZkUtils.BrokerIdsPath, true)
    topicsConfigPathCache = new PathChildrenCache(curator, ZkUtils.TopicConfigPath, true)
    topicsTreeCache = new TreeCache(curator, ZkUtils.BrokerTopicsPath)

    log.info("Starting curator client...")
    curator.start()
    log.info("Starting topics tree cache...")
    topicsTreeCache.start()
    topicsTreeCache.getListenable.addListener(topicsTreeCacheListener)
    log.info("Starting topics config path cache...")
    topicsConfigPathCache.start(StartMode.BUILD_INITIAL_CACHE)
    log.info("Starting brokers path cache...")
    brokersPathCache.start(StartMode.BUILD_INITIAL_CACHE)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = KafkaStat.this.close()
    })
  }

  @volatile
  private[this] var topicsTreeCacheLastUpdateMillis : Long = System.currentTimeMillis()

  private[this] val topicsTreeCacheListener = new TreeCacheListener {
    override def childEvent(client: CuratorFramework, event: TreeCacheEvent): Unit = {
      event.getType match {
        case TreeCacheEvent.Type.INITIALIZED | TreeCacheEvent.Type.NODE_ADDED |
             TreeCacheEvent.Type.NODE_REMOVED | TreeCacheEvent.Type.NODE_UPDATED =>
          topicsTreeCacheLastUpdateMillis = System.currentTimeMillis()
        case _ => //do nothing
      }
    }
  }

  def getBrokers: Seq[BrokerIdentity] = {
    val data = brokersPathCache.getCurrentData
    data.map { childData =>
      BrokerIdentity(nodeFromPath(childData.getPath), asString(childData.getData))
    }.sortBy(_.id)
  }

  def getBrokerInfo(broker: String): BrokerInfo = {
    val brokerPath = "%s/%s".format(ZkUtils.BrokerIdsPath, broker)
    val cd = brokersPathCache.getCurrentData(brokerPath)
    val brokerIdentity = BrokerIdentity(nodeFromPath(cd.getPath), asString(cd.getData))
    val tryResult = KafkaJMX.doWithConnection(brokerIdentity.host, brokerIdentity.jmxPort, None, None) { mbsc =>
      KafkaMetrics.getBrokerMetrics(mbsc)
    }
    val result = tryResult match {
      case scala.util.Failure(t) =>
        log.error(s"Failed to get topic metrics for broker $broker with ${t.getMessage}")
        null
      case scala.util.Success(bm) => bm
    }
    BrokerInfo(broker, result)
  }

  def getTopicMetric(topic: String): TopicInfo = {
    val topicMetric = getBrokers
      .map(_.id)
      .map(getBrokerInfo)
      .map(_.metrics)
      .foldLeft(BrokerMetrics.DEFAULT){ (l, r) => l + r}

    TopicInfo(topic, topicMetric)
  }

  def getTopics: Seq[TopicDescription] = {
    topicsTreeCache.getCurrentChildren(ZkUtils.BrokerTopicsPath)
      .map { case (path, _) =>
        val topic = nodeFromPath(path)
        getTopicDescription(topic)
      }.toSeq
  }

  def getTopicZookeeperData(topic: String): Option[TopicPartitions] = {
    val topicPath = "%s/%s".format(ZkUtils.BrokerTopicsPath, topic)
    Option(topicsTreeCache.getCurrentData(topicPath)).map { childData =>
      (childData.getStat.getVersion, asString(childData.getData))
    }
    val childData = Option(topicsTreeCache.getCurrentData(topicPath))

    childData.map { cd =>
      val topicMap = JsonUtils.toScalaMap(asString(cd.getData))
      val partMap = topicMap("partitions").asInstanceOf[Map[String, Seq[String]]]
      new TopicPartitions ++= partMap
    }
  }

  private[this] def getTopicConfigString(topic: String) : Option[TopicConfig] = {
    val data: mutable.Buffer[ChildData] = topicsConfigPathCache.getCurrentData
    val result: Option[ChildData] = data.find(_.getPath.endsWith(topic))
    result.map(cd => JsonUtils.fromJson(asString(cd.getData), classOf[TopicConfig]))
  }

  def getTopicDescription(topic: String) : TopicDescription = {
    val topicPartition = getTopicZookeeperData(topic)
    val nodeNum = topicPartition.map(_.values.flatten.toSet.size).getOrElse(0)
    val partitionsPath = "%s/%s/partitions".format(ZkUtils.BrokerTopicsPath, topic)
    val partitions = topicsTreeCache.getCurrentChildren(partitionsPath).toMap
    val states = partitions.flatMap { case (partId, _) =>
      val statePath = s"$partitionsPath/$partId/state"
      val cd = topicsTreeCache.getCurrentData(statePath)
      Map(partId -> JsonUtils.toScalaMap(asString(cd.getData)))
    }
    // { parId -> { controller_epoch -> , leader -> , version -> , leader_epoch -> , isr -> } }
    val leadNodeNum = states.values.map(_.get("leader").map(_.toString)).filter(_.isDefined).map(_.get).toSet.size
    val topicConfig = getTopicConfigString(topic)

    TopicDescription(
      topic,
      topicPartition,
      Option(new PartitionsState ++= states),
      topicConfig,
      nodeNum,
      leadNodeNum
    )
  }

  def close(): Unit = {
    log.info("Shutting down topics tree cache...")
    Try(topicsTreeCache.close())
    log.info("Shutting down topics config path cache...")
    Try(topicsConfigPathCache.close())
    log.info("Shutting down brokers path cache...")
    Try(brokersPathCache.close())
    log.info("Shutting down curator client...")
    Try(curator.close())
  }
}
