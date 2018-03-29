package polaris.kafka.jmx

import javax.management.MBeanServerConnection
import javax.management.remote.{JMXConnector, JMXConnectorFactory, JMXServiceURL}
import javax.management.remote.rmi.RMIConnectorServer
import javax.naming.Context
import javax.rmi.ssl.SslRMIClientSocketFactory

import scala.collection.JavaConversions._
import scala.util.{Failure, Try}

/**
  * Created by chengli at 08/12/2017
  */
object KafkaJMX {
  private[this] val defaultJmxConnectorProperties = Map[String, Any] (
    "jmx.remote.x.request.waiting.timeout" -> "3000",
    "jmx.remote.x.notification.fetch.timeout" -> "3000",
    "sun.rmi.transport.connectionTimeout" -> "3000",
    "sun.rmi.transport.tcp.handshakeTimeout" -> "3000",
    "sun.rmi.transport.tcp.responseTimeout" -> "3000"
  )

  def doWithConnection[T](jmxHost: String, jmxPort: Int, jmxUser: Option[String], jmxPass: Option[String], jmxSsl: Boolean = false)(fn: MBeanServerConnection => T) : Try[T] = {
    val urlString = s"service:jmx:rmi:///jndi/rmi://$jmxHost:$jmxPort/jmxrmi"
    val url = new JMXServiceURL(urlString)
    try {
      require(jmxPort > 0, "No jmx port but jmx polling enabled!")
      val credsProps: Option[Map[String, _]] = for {
        user <- jmxUser
        pass <- jmxPass
      } yield {
        Map(JMXConnector.CREDENTIALS -> Array(user, pass))
      }
      val sslProps: Option[Map[String, _]] = if (jmxSsl) {
        val clientSocketFactory = new SslRMIClientSocketFactory()
        Some(Map(
          Context.SECURITY_PROTOCOL -> "ssl",
          RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE -> clientSocketFactory,
          "com.sun.jndi.rmi.factory.socket" -> clientSocketFactory
        ))
      } else {
        None
      }
      val jmxConnectorProperties = List(credsProps, sslProps).flatten.foldRight(defaultJmxConnectorProperties)(_ ++ _)
      val jmxc = JMXConnectorFactory.connect(url, jmxConnectorProperties)
      try {
        Try {
          fn(jmxc.getMBeanServerConnection)
        }
      } finally {
        jmxc.close()
      }
    } catch {
      case e: Exception => Failure(e)
    }
  }
}
