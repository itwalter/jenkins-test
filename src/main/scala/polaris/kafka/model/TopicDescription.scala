package polaris.kafka.model

/**
  * Created by chengli at 15/12/2017
  */
case class TopicDescription(topic: String,
                            partitions: Option[TopicPartitions],
                            partitionState: Option[PartitionsState],
                            config:Option[TopicConfig],
                            nodeNum: Int,
                            leaderNodeNum: Int)
