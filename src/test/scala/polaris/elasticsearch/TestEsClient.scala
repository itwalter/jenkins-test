package polaris.elasticsearch

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.carrotsearch.hppc.cursors.ObjectCursor
import org.elasticsearch.client.{ClusterAdminClient, IndicesAdminClient}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by chengli at 16/12/2017
  */
class TestEsClient extends FunSuite with BeforeAndAfterAll {
  private[this] var client: TransportClient = _
  private[this] var indices: IndicesAdminClient = _
  private[this] var cluster: ClusterAdminClient = _

  override def beforeAll(): Unit = {
    val settings = Settings.builder
      .put("client.transport.ignore_cluster_name", true)
      .put("transport.tcp.connect_timeout", new TimeValue(10, TimeUnit.SECONDS))
      .put("transport.tcp.compress", true)
      .put("transport.netty.worker_count", 2)
      .build

    client = new PreBuiltTransportClient(settings)
    client.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("local-1"), 9300))
    client.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("local-2"), 9300))
    client.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("local-3"), 9300))

    indices = client.admin.indices()
    cluster = client.admin.cluster()
  }

  test("get cluster node name") {
    val response = cluster.prepareNodesInfo().get

    response.getNodes.foreach { nodeInfo =>
      println(nodeInfo.getHostname)
      println(nodeInfo.getNode.getName)
    }
  }

  def clusterStatus = {
    val health = cluster.prepareHealth().get
    val clusterStat = cluster.prepareClusterStats().get

    val map = mutable.HashMap[String, Any]()
    map += ("timestamp" -> clusterStat.getTimestamp)
    map += ("status" -> clusterStat.getStatus.name)
    map += ("node_total" -> clusterStat.getNodesStats.getCounts.getTotal)
    map ++= clusterStat.getNodesStats.getCounts.getRoles.map(entry => "node_" + entry._1 -> entry._2).toMap
    map += ("shard_total" -> clusterStat.getIndicesStats.getShards.getTotal)
    map += ("shard_success" -> health.getActiveShards)
    map += ("index_total" -> clusterStat.getIndicesStats.getIndexCount)
    map += ("doc_total" -> clusterStat.getIndicesStats.getDocs.getCount)
    map += ("disk_total" -> clusterStat.getIndicesStats.getStore.sizeInBytes)
    map += ("mem_total" -> clusterStat.getIndicesStats.getFieldData.getMemorySizeInBytes)
    map += ("replication" -> clusterStat.getIndicesStats.getShards.getReplication)
    map
  }

  def templateState = {
    val templateResponse = indices.prepareGetTemplates().get

    templateResponse.getIndexTemplates.map { templateMetaData: IndexTemplateMetaData =>
      val map = mutable.HashMap[String, Any]()

      map += "name" -> templateMetaData.getName
      map += "settings" -> templateMetaData.getSettings.getAsMap
      map
    }
  }

  test("test template stat") {
    templateState.foreach(println)
  }

  test("test cluster stat") {
    println(clusterStatus)
  }

  override def afterAll(): Unit = {
    client.close()
  }
}
