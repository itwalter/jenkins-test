package polaris.kafka.model

import scala.collection.mutable


/**
  * Created by chengli at 15/12/2017
  */
class TopicPartitions extends mutable.HashMap[String, Seq[String]] {
  def getPartitions: Seq[String] = this.keySet.toSeq

  def getBrokersByPartition(partitionId: String): Seq[String] =
    this.getOrElse(partitionId, Seq[String]())
}
