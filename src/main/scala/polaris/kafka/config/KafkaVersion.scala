package polaris.kafka.config

/**
  * Created by chengli at 08/12/2017
  */
sealed trait KafkaVersion
case object Kafka_0_8_1_1 extends KafkaVersion {
  override def toString = "0.8.1.1"
}
case object Kafka_0_8_2_0 extends KafkaVersion {
  override def toString = "0.8.2.0"
}
case object Kafka_0_8_2_1 extends KafkaVersion {
  override def toString = "0.8.2.1"
}
case object Kafka_0_8_2_2 extends KafkaVersion {
  override def toString = "0.8.2.2"
}
case object Kafka_0_9_0_0 extends KafkaVersion {
  override def toString = "0.9.0.0"
}
case object Kafka_0_9_0_1 extends KafkaVersion {
  override def toString = "0.9.0.1"
}
case object Kafka_0_10_0_0 extends KafkaVersion {
  override def toString = "0.10.0.0"
}
case object Kafka_0_10_0_1 extends KafkaVersion {
  override def toString = "0.10.0.1"
}

case object Kafka_0_10_1_0 extends KafkaVersion {
  override def toString = "0.10.1.0"
}

case object Kafka_0_10_1_1 extends KafkaVersion {
  override def toString = "0.10.1.1"
}

case object Kafka_0_10_2_0 extends KafkaVersion {
  override def toString = "0.10.2.0"
}

case object Kafka_0_10_2_1 extends KafkaVersion {
  override def toString = "0.10.2.1"
}

case object Kafka_0_11_0_0 extends KafkaVersion {
  override def toString = "0.11.0.0"
}

object KafkaVersion {
  val supportedVersions: Map[String,KafkaVersion] = Map(
    "0.8.1.1" -> Kafka_0_8_1_1,
    "0.8.2-beta" -> Kafka_0_8_2_0,
    "0.8.2.0" -> Kafka_0_8_2_0,
    "0.8.2.1" -> Kafka_0_8_2_1,
    "0.8.2.2" -> Kafka_0_8_2_2,
    "0.9.0.0" -> Kafka_0_9_0_0,
    "0.9.0.1" -> Kafka_0_9_0_1,
    "0.10.0.0" -> Kafka_0_10_0_0,
    "0.10.0.1" -> Kafka_0_10_0_1,
    "0.10.1.0" -> Kafka_0_10_1_0,
    "0.10.1.1" -> Kafka_0_10_1_1,
    "0.10.2.0" -> Kafka_0_10_2_0,
    "0.10.2.1" -> Kafka_0_10_2_1,
    "0.11.0.0" -> Kafka_0_11_0_0
  )

  val formSelectList : IndexedSeq[(String,String)] = supportedVersions.toIndexedSeq.filterNot(_._1.contains("beta")).map(t => (t._1,t._2.toString)).sortWith((a, b) => sortVersion(a._1, b._1))

  def apply(s: String) : KafkaVersion = {
    supportedVersions.get(s) match {
      case Some(v) => v
      case None => throw new IllegalArgumentException(s"Unsupported kafka version : $s")
    }
  }

  def unapply(v: KafkaVersion) : Option[String] = {
    Some(v.toString)
  }

  private def sortVersion(versionNum: String, kafkaVersion: String): Boolean = {
    val separator = "\\."
    val versionNumList = versionNum.split(separator, -1).toList
    val kafkaVersionList = kafkaVersion.split(separator, -1).toList
    def compare(a: List[String], b: List[String]): Boolean = a.nonEmpty match {
      case true if b.nonEmpty =>
        if (a.head == b.head) compare(a.tail, b.tail) else a.head.toInt < b.head.toInt
      case true if b.isEmpty => false
      case false if b.nonEmpty => true
      case _ => true
    }
    compare(versionNumList, kafkaVersionList)
  }
}
