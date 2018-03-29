package polaris.kafka.model

import polaris.kafka.jmx.metric.BrokerMetrics

/**
  * Created By chengli at 04/01/2018
  */
case class TopicInfo(topic: String, metrics: BrokerMetrics)
