package polaris.kafka.jmx.metric

/**
  * Created by chengli at 09/12/2017
  */
case class BrokerMetrics(
  bytesInPerSec: MeterMetric,
  bytesOutPerSec: MeterMetric,
  bytesRejectedPerSec: MeterMetric,
  failedFetchRequestsPerSec: MeterMetric,
  failedProduceRequestsPerSec: MeterMetric,
  messagesInPerSec: MeterMetric,
  oSystemMetrics: OSMetric,
  size: SegmentsMetric
) {
  def +(o: BrokerMetrics) : BrokerMetrics = {
    BrokerMetrics(
      o.bytesInPerSec + bytesInPerSec,
      o.bytesOutPerSec + bytesOutPerSec,
      o.bytesRejectedPerSec + bytesRejectedPerSec,
      o.failedFetchRequestsPerSec + failedFetchRequestsPerSec,
      o.failedProduceRequestsPerSec + failedProduceRequestsPerSec,
      o.messagesInPerSec + messagesInPerSec,
      oSystemMetrics,
      o.size + size)
  }
}

object BrokerMetrics {
  val DEFAULT = BrokerMetrics(
    MeterMetric(0, 0, 0, 0, 0),
    MeterMetric(0, 0, 0, 0, 0),
    MeterMetric(0, 0, 0, 0, 0),
    MeterMetric(0, 0, 0, 0, 0),
    MeterMetric(0, 0, 0, 0, 0),
    MeterMetric(0, 0, 0, 0, 0),
    OSMetric(0D, 0D),
    SegmentsMetric(0L))
}
