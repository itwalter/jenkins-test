package polaris.kafka.jmx.metric

/**
  * Created by chengli at 09/12/2017
  */
case class SegmentsMetric(bytes: Long) {
  def +(o: SegmentsMetric) : SegmentsMetric = {
    SegmentsMetric(o.bytes + bytes)
  }

  def formatSize: String = {
    sizeFormat(bytes)
  }
}
