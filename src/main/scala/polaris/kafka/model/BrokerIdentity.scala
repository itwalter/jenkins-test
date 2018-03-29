package polaris.kafka.model

import polaris.utils.JsonUtils

/**
  * Created by chengli at 15/12/2017
  */
case class BrokerIdentity(
  id: String,
  host: String,
  port: Int,
  jmxPort: Int,
  endpoints: String) {
}

object BrokerIdentity {
  def apply(id: String, config: String): BrokerIdentity = {
    val configMap = JsonUtils.toScalaMap(config)
    val host = configMap("host").toString
    val port = configMap("port").toString.toInt
    val jmxPort = configMap("jmx_port").toString.toInt
    val endpoints = configMap("endpoints").asInstanceOf[Seq[String]].mkString(",")

    new BrokerIdentity(id, host, port, jmxPort, endpoints)
  }
}
