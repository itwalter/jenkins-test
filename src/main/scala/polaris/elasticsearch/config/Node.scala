package polaris.elasticsearch.config

import java.net.InetAddress

import org.elasticsearch.common.transport.InetSocketTransportAddress

import scala.beans.BeanProperty

/**
  * Created by chengli at 15/12/2017
  */
class Node {
  @BeanProperty var host: String = _
  @BeanProperty var port: String = _

  override def toString: String = s"${host.trim}:${port.trim}"

  def toTransportAddress: InetSocketTransportAddress = {
    new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt)
  }
}

object Node {
  def apply(host: String, port: String): Node = {
    val node = new Node
    node.setHost(host)
    node.setPort(port)
    node
  }

  def apply(host: String, port: Int): Node = Node(host, port.toString)
}