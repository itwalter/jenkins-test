package polaris.kafka.jmx

import java.io.File
import javax.management._

import com.yammer.metrics.reporting.JmxReporter.GaugeMBean
import polaris.kafka.config.{KafkaVersion, Kafka_0_10_0_1, Kafka_0_8_1_1}
import polaris.kafka.jmx.metric._

import scala.collection.JavaConversions._
import scala.util.matching.Regex

/**
  * Created by chengli at 09/12/2017
  */
object KafkaMetrics {
  def getBytesInPerSec(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(kafkaVersion, mbsc, "BytesInPerSec", topicOption)
  }

  def getBytesOutPerSec(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(kafkaVersion, mbsc, "BytesOutPerSec", topicOption)
  }

  def getBytesRejectedPerSec(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(kafkaVersion, mbsc, "BytesRejectedPerSec", topicOption)
  }

  def getFailedFetchRequestsPerSec(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(kafkaVersion, mbsc, "FailedFetchRequestsPerSec", topicOption)
  }

  def getFailedProduceRequestsPerSec(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(kafkaVersion, mbsc, "FailedProduceRequestsPerSec", topicOption)
  }

  def getMessagesInPerSec(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, topicOption: Option[String] = None) = {
    getBrokerTopicMeterMetrics(kafkaVersion, mbsc, "MessagesInPerSec", topicOption)
  }

  private def getBrokerTopicMeterMetrics(kafkaVersion: KafkaVersion, mbsc: MBeanServerConnection, metricName: String, topicOption: Option[String]) = {
    getMeterMetric(mbsc, getObjectName(kafkaVersion, metricName, topicOption))
  }

  private def getSep(kafkaVersion: KafkaVersion) : String = {
    kafkaVersion match {
      case Kafka_0_8_1_1 => "\""
      case _ => ""
    }
  }

  def getObjectName(kafkaVersion: KafkaVersion, name: String, topicOption: Option[String] = None) = {
    val sep = getSep(kafkaVersion)
    val topicAndName = kafkaVersion match {
      case Kafka_0_8_1_1 =>
        topicOption.map( topic => s"$sep$topic-$name$sep").getOrElse(s"${sep}AllTopics$name$sep")
      case _ =>
        val topicProp = topicOption.map(topic => s",topic=$topic").getOrElse("")
        s"$name$topicProp"
    }
    new ObjectName(s"${sep}kafka.server$sep:type=${sep}BrokerTopicMetrics$sep,name=$topicAndName")
  }

  /* Gauge, Value : 0 */
  private val replicaFetcherManagerMinFetchRate = new ObjectName(
    "kafka.server:type=ReplicaFetcherManager,name=MinFetchRate,clientId=Replica")

  /* Gauge, Value : 0 */
  private val replicaFetcherManagerMaxLag = new ObjectName(
    "kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica")

  /* Gauge, Value : 0 */
  private val kafkaControllerActiveControllerCount = new ObjectName(
    "kafka.controller:type=KafkaController,name=ActiveControllerCount")

  /* Gauge, Value : 0 */
  private val kafkaControllerOfflinePartitionsCount = new ObjectName(
    "kafka.controller:type=KafkaController,name=OfflinePartitionsCount")

  /* Timer*/
  private val logFlushStats = new ObjectName(
    "kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs")

  /* Operating System */
  private val operatingSystemObjectName = new ObjectName("java.lang:type=OperatingSystem")

  /* Log Segments */
  private val logSegmentObjectName = new ObjectName("kafka.log:type=Log,name=*-LogSegments")

  private val directoryObjectName = new ObjectName("kafka.log:type=Log,name=*-Directory")

  private val LogSegmentsNameRegex = new Regex("%s-LogSegments".format("""(.*)-(\d*)"""), "topic", "partition")

  private val DirectoryNameRegex = new Regex("%s-Directory".format("""(.*)-(\d*)"""), "topic", "partition")

  val LogSegmentRegex = new Regex(
    "baseOffset=(.*), created=(.*), logSize=(.*), indexSize=(.*)",
    "baseOffset", "created", "logSize", "indexSize"
  )

  private def getOSMetric(mbsc: MBeanServerConnection) = {
    try {
      val attributes = mbsc.getAttributes(
        operatingSystemObjectName,
        Array("ProcessCpuLoad", "SystemCpuLoad")
      ).asList
      OSMetric(
        getDoubleValue(attributes, "ProcessCpuLoad"),
        getDoubleValue(attributes, "SystemCpuload")
      )
    } catch {
      case _: InstanceNotFoundException => OSMetric(0D, 0D)
    }
  }

  private def getMeterMetric(mbsc: MBeanServerConnection, name: ObjectName) = {
    try {
      val attributeList = mbsc.getAttributes(name, Array("Count", "FifteenMinuteRate", "FiveMinuteRate", "OneMinuteRate", "MeanRate"))
      val attributes = attributeList.asList()
      MeterMetric(getLongValue(attributes, "Count"),
        getDoubleValue(attributes, "FifteenMinuteRate"),
        getDoubleValue(attributes, "FiveMinuteRate"),
        getDoubleValue(attributes, "OneMinuteRate"),
        getDoubleValue(attributes, "MeanRate"))
    } catch {
      case _: InstanceNotFoundException => MeterMetric(0, 0, 0, 0, 0)
    }
  }

  private def getLongValue(attributes: Seq[Attribute], name: String) = {
    attributes.find(_.getName == name).map(_.getValue.asInstanceOf[Long]).getOrElse(0L)
  }

  private def getDoubleValue(attributes: Seq[Attribute], name: String) = {
    attributes.find(_.getName == name).map(_.getValue.asInstanceOf[Double]).getOrElse(0D)
  }

  private def topicAndPartition(name: String, regex: Regex) = {
    try {
      val matches = regex.findAllIn(name).matchData.toSeq
      require(matches.size == 1)
      val m = matches.head
      val topic = m.group("topic")
      val partition = m.group("partition").toInt

      (topic, partition)
    }
    catch {
      case e: Exception =>
        throw new IllegalStateException("Can't parse topic and partition from: <%s>".format(name), e)
    }
  }

  private def queryValues[K, V](
    mbsc: MBeanServerConnection,
    objectName: ObjectName,
    keyConverter: String => K,
    valueConverter: Object => V
  ) = {
    val logsSizeObjectNames = mbsc.queryNames(objectName, null).toSeq
    logsSizeObjectNames.par.map {
      objectName => queryValue(mbsc, objectName, keyConverter, valueConverter)
    }.seq
  }

  private def queryValue[K, V](
    mbsc: MBeanServerConnection,
    objectName: ObjectName,
    keyConverter: String => K,
    valueConverter: Object => V
  ) = {
    val name = objectName.getKeyProperty("name")
    val mbean = MBeanServerInvocationHandler.newProxyInstance(mbsc, objectName, classOf[GaugeMBean], true)
    (keyConverter(name), valueConverter(mbean.getValue))
  }

  private def parseLogSegment(str: String): LogSegment = {
    try {
      val matches = LogSegmentRegex.findAllIn(str).matchData.toSeq
      require(matches.size == 1)
      val m = matches.head

      LogSegment(
        baseOffset = m.group("baseOffset").toLong,
        created = m.group("created").toLong,
        logBytes = m.group("logSize").toLong,
        indexBytes = m.group("indexSize").toLong
      )
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Can't parse segment info from: <%s>".format(str), e)
    }
  }

  def getLogSegmentsInfo(mbsc: MBeanServerConnection) = {
    val logSegmentsMap = {
      queryValues(
        mbsc,
        logSegmentObjectName,
        key => topicAndPartition(key, LogSegmentsNameRegex),
        value => {
          val lst = value.asInstanceOf[java.util.List[String]]
          lst.map(parseLogSegment)
        }
      )
    }.toMap

    val directoryMap = {
      queryValues(
        mbsc,
        directoryObjectName,
        key => topicAndPartition(key, DirectoryNameRegex),
        value => value.asInstanceOf[String]
      )
    }.toMap

    val stats: Seq[(String, (Int, LogInfo))] = for (
      key <- (logSegmentsMap.keySet ++ directoryMap.keySet).toSeq;
      directory <- directoryMap.get(key);
      logSegments <- logSegmentsMap.get(key)
    ) yield {
      val directoryFile = new File(directory)
      val dir = directoryFile.getParentFile.getAbsolutePath

      val (topic, partition) = key

      (topic, (partition, LogInfo(dir, logSegments)))
    }

    stats.groupBy(_._1).mapValues(_.map(_._2).toMap)
  }

  // return broker metrics with segment metric only when it's provided. if not, it will contain segment metric with value 0L
  def getBrokerMetrics(
    mbsc: MBeanServerConnection,
    kafkaVersion: KafkaVersion = Kafka_0_10_0_1,
    segmentsMetric: Option[SegmentsMetric] = None,
    topic: Option[String] = None
  ) : BrokerMetrics = {

    BrokerMetrics(
      KafkaMetrics.getBytesInPerSec(kafkaVersion, mbsc, topic),
      KafkaMetrics.getBytesOutPerSec(kafkaVersion, mbsc, topic),
      KafkaMetrics.getBytesRejectedPerSec(kafkaVersion, mbsc, topic),
      KafkaMetrics.getFailedFetchRequestsPerSec(kafkaVersion, mbsc, topic),
      KafkaMetrics.getFailedProduceRequestsPerSec(kafkaVersion, mbsc, topic),
      KafkaMetrics.getMessagesInPerSec(kafkaVersion, mbsc, topic),
      KafkaMetrics.getOSMetric(mbsc),
      segmentsMetric.getOrElse(SegmentsMetric(0L))
    )
  }
}
