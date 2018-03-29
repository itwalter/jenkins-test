package polaris.kafka

import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.cache._
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import polaris.kafka.util.ZkUtils

import scala.collection.JavaConversions._

/**
  * Created by chengli at 08/12/2017
  */
class TestZk extends FunSuite with BeforeAndAfterAll {
  var curator: CuratorFramework = _

  private[this] var brokersPathCache: PathChildrenCache = _
  private[this] var topicsConfigPathCache: PathChildrenCache = _
  private[this] var topicsTreeCache: TreeCache = _

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

  override def beforeAll(): Unit = {
    curator = CuratorFrameworkFactory.newClient(
      "local-1:2181",
      new BoundedExponentialBackoffRetry(100, 1000, 100)
    )

    brokersPathCache = new PathChildrenCache(curator, ZkUtils.BrokerIdsPath, true)
    topicsConfigPathCache = new PathChildrenCache(curator, ZkUtils.TopicConfigPath, true)
    topicsTreeCache = new TreeCache(curator, ZkUtils.BrokerTopicsPath)

    curator.start()
    topicsTreeCache.start()
    topicsTreeCache.getListenable.addListener(topicsTreeCacheListener)
    topicsConfigPathCache.start(StartMode.BUILD_INITIAL_CACHE)
    brokersPathCache.start(StartMode.BUILD_INITIAL_CACHE)
  }

  test("test brokers") {
    brokersPathCache.getCurrentData.foreach { cd =>
      println(cd.getPath)
      println(asString(cd.getData))
    }
  }

  test("test topicsConf") {
    topicsConfigPathCache.getCurrentData.foreach { cd =>
      println(cd.getPath)
      println(asString(cd.getData))
    }
  }

  test("test topics") {
    val topics = topicsConfigPathCache.getCurrentData.map(x => nodeFromPath(x.getPath))

    topics.foreach { topic =>
      val topicPath = "%s/%s".format(ZkUtils.BrokerTopicsPath, topic)
      topicsTreeCache.getCurrentChildren(topicPath).foreach { case(path, cd) =>
        println(path)
        println(cd)
      }
    }

    val topic = topics.head
    val topicPath = "%s/%s".format(ZkUtils.BrokerTopicsPath, topic)
    val childData = topicsTreeCache.getCurrentData(topicPath)
    println(childData.getStat.getVersion)
    println(asString(childData.getData))
  }

  override def afterAll(): Unit = {
    brokersPathCache.close()
    topicsConfigPathCache.close()
    topicsTreeCache.close()
    curator.close()
  }
}
