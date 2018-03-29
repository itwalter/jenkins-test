package polaris.elasticsearch

import java.util.concurrent.TimeUnit
import javax.annotation.PostConstruct

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats
import org.elasticsearch.action.admin.indices.stats.IndexStats
import org.elasticsearch.client.{ClusterAdminClient, IndicesAdminClient}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import polaris.elasticsearch.config.EsConfig

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by chengli at 15/12/2017
  */
@Service
class EsStat {
  @Autowired var esConfig: EsConfig = _

  private[this] var client: TransportClient = _
  private[this] var indices: IndicesAdminClient = _
  private[this] var cluster: ClusterAdminClient = _

  @PostConstruct
  def initialize(): Unit = {
    val settings = Settings.builder
      .put("client.transport.ignore_cluster_name", true)
      .put("transport.tcp.connect_timeout", new TimeValue(10, TimeUnit.SECONDS))
      .put("transport.tcp.compress", true)
      .put("transport.netty.worker_count", 2)
      .build

    // create client
    client = new PreBuiltTransportClient(settings)
    client.addTransportAddresses(esConfig.node.map(_.toTransportAddress):_*)

    indices = client.admin.indices()
    cluster = client.admin.cluster()

    Runtime.getRuntime.addShutdownHook(new Thread(){
      override def run(): Unit = EsStat.this.close()
    })
  }

  /**
    * get the status of cluster
    * @return
    */
  def clusterStatus: Map[String, Any] = {
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
    map += ("disk_used" -> clusterStat.getIndicesStats.getStore.sizeInBytes)
    map += ("mem_used" -> clusterStat.getIndicesStats.getFieldData.getMemorySizeInBytes)
    map += ("replication" -> clusterStat.getIndicesStats.getShards.getReplication)
    map.toMap
  }

  /**
    * Get the status of all nodes
    * @return array of node's statuses
    */
  def nodeStatus: Seq[Map[String, Any]] = {
    val response = cluster.prepareNodesStats()
      .setFs(true)
      .setHttp(true)
      .setJvm(true)
      .setOs(true)
      .setIndices(true)
      .setProcess(true)
      .get

    val nodes = response.getNodesMap.map { case (id: String, node: NodeStats) =>
      val map = mutable.HashMap[String, Any]()
      val indices = node.getIndices
      val jvm = node.getJvm
      val fs = node.getFs
      val os = node.getOs
      val process = node.getProcess
      val http = node.getHttp

      /** Summary */
      map += ("node_id" -> id)
      map += ("node_name" -> node.getNode.getName)
      map += ("host" -> node.getHostname)
      map += ("uptime" -> jvm.getUptime.getDays)

      /** file system */
      map += ("fs_store_size" -> indices.getStore.getSizeInBytes)
      map += ("fs_doc_count" -> indices.getDocs.getCount)
      // fs_doc_del_percent <= 0.1 = Pass, fs_doc_del_percent <= 0.25 = Warning
      map += ("fs_doc_del_percent" -> indices.getDocs.getDeleted.toDouble / indices.getDocs.getCount)
      map += ("fs_merge_size" -> indices.getMerge.getTotalSizeInBytes)
      map += ("fs_merge_rate" -> indices.getMerge.getTotalSizeInBytes.toDouble / indices.getMerge.getTotalTimeInMillis / 1000)
      map += ("fs_file_descriptors" -> process.getOpenFileDescriptors)
      // fs_disk_used_percent <= 0.8 = Pass, fs_disk_used_percent <= 0.9 = Warning
      map += ("fs_disk_used_percent" -> fs.getTotal.getFree.getBytes.toDouble / fs.getTotal.getTotal.getBytes)
      map += ("fs_disk_free" -> fs.getTotal.getFree.getBytes)

      /** index activity */
      // index_indexing_index_per_time <= 10 = Pass, index_indexing_index_per_time <= 50 = Warning
      map += ("index_indexing_index_per_time" -> indices.getIndexing.getTotal.getIndexTime.getMillis.toDouble / indices.getIndexing.getTotal.getIndexCount)
      // index_indexing_delete_per_time <= 10 = Pass, index_indexing_delete_per_time <= 50 = Warning
      map += ("index_indexing_delete_per_time" -> indices.getIndexing.getTotal.getDeleteTime.getMillis.toDouble / indices.getIndexing.getTotal.getDeleteCount)
      // index_search_query_per_time <= 50 = Pass, index_search_query_per_time <= 500 = Warning
      map += ("index_search_query_per_time" -> indices.getSearch.getTotal.getQueryTime.getMillis.toDouble / indices.getSearch.getTotal.getQueryCount)
      // index_search_fetch_per_time <= 8 = Pass, index_search_query_per_time <= 15 = Warning
      map += ("index_search_fetch_per_time" -> indices.getSearch.getTotal.getFetchTime.getMillis.toDouble / indices.getSearch.getTotal.getFetchCount)

      /** cache activity */
      map += ("cache_mem_size" -> indices.getFieldData.getMemorySizeInBytes)
      map += ("cache_query_mem_size" -> indices.getQueryCache.getMemorySizeInBytes)
      map += ("cache_query_mem_size" -> indices.getQueryCache.getMemorySizeInBytes)
      map += ("cache_query_hit_rate" -> indices.getQueryCache.getHitCount.toDouble / indices.getQueryCache.getTotalCount)

      /** memory */
      map += ("mem_total_size" -> os.getMem.getTotal.getBytes)
      // mem_heap_size <= 31,457,280 = Pass, mem_heap_size <= 33,554,432 = Warning
      map += ("mem_heap_size" -> jvm.getMem.getHeapCommitted.getBytes)
      // mem_heap_in_total <= 0.6 = Pass, mem_heap_in_total <= 0.75 = Warning
      map += ("mem_heap_in_total" -> jvm.getMem.getHeapCommitted.getBytes.toDouble / os.getMem.getTotal.getBytes)
      map += ("mem_heap_used_percent" -> jvm.getMem.getHeapUsed.getBytes.toDouble / jvm.getMem.getHeapCommitted.getBytes)

      if (jvm.getGc.getCollectors.exists(_.getName == "young")) {
        val young = jvm.getGc.getCollectors.find(_.getName == "young").get
        // mem_gc_young_freq >= 30 = Pass, mem_gc_young_freq >= 15 = Warning
        map += ("mem_gc_young_freq" -> jvm.getUptime.getSeconds.toDouble / young.getCollectionCount)
        // mem_gc_young_duration <= 150 = Pass, mem_gc_young_duration <= 400 = Warning
        map += ("mem_gc_young_duration" -> young.getCollectionTime.getMillis.toDouble / young.getCollectionCount)
      }

      if (jvm.getGc.getCollectors.exists(_.getName == "old")) {
        val old = jvm.getGc.getCollectors.find(_.getName == "old").get
        // mem_gc_old_freq >= 30 = Pass, mem_gc_old_freq >= 15 = Warning
        map += ("mem_gc_old_freq" -> jvm.getUptime.getSeconds.toDouble / old.getCollectionCount)
        // mem_gc_old_duration <= 150 = Pass, mem_gc_old_duration <= 400 = Warning
        map += ("mem_gc_old_duration" -> old.getCollectionTime.getMillis.toDouble / old.getCollectionCount)
      }

      /** http activity */
      // http_connection_rate <= 5 = Pass, http_connection_rate <= 30 = Warning
      map += ("http_connection_rate" -> http.getTotalOpen.toDouble / jvm.getUptime.getSeconds)
      map.toMap
    }.toSeq
    nodes
  }

  def templateState: Seq[mutable.HashMap[String, Any]] = {
    val templateResponse = indices.prepareGetTemplates().get

    val sources = templateResponse.getIndexTemplates.map { templateMetaData: IndexTemplateMetaData =>
      val map = mutable.HashMap[String, Any]()

      map += "name" -> templateMetaData.getName
      map += "settings" -> templateMetaData.getSettings.getAsMap
      map
    }
    sources
  }

  /**
    * get the status of a source
    * @param source the alias of a source
    * @return a source's status
    */
  def sourceStat(source: String): Seq[mutable.HashMap[String, Any]] = {
    val aliasesResponse = indices.prepareGetAliases(source).get

    val sources = aliasesResponse.getAliases.map(_.key).toSeq
    if (sources.isEmpty) return Seq()
    val indicesResponse = indices.prepareStats(sources:_*).get

    indicesResponse.getIndices.map { case (index: String, stat: IndexStats) =>
      val map = mutable.HashMap[String, Any]()
      val date = index.substring(source.length + 1)
      map += ("date" -> date)
      map += ("name" -> stat.getIndex)
      map += ("doc_count" -> stat.getTotal.getDocs.getCount)
      map += ("doc_size" -> stat.getTotal.getStore.getSizeInBytes)
      map += ("primary_count" -> stat.getPrimaries.getDocs.getCount)
      map += ("primary_size" -> stat.getPrimaries.getStore.getSizeInBytes)
      map += ("total_shards" -> stat.getShards.length)
    }.toSeq
  }

  def close(): Unit = {
    client.close()
  }
}
