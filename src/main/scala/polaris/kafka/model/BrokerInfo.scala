package polaris.kafka.model

import polaris.kafka.jmx.metric.BrokerMetrics


/**
  * Created by chengli at 15/12/2017
  */
case class BrokerInfo(id: String, metrics: BrokerMetrics)
