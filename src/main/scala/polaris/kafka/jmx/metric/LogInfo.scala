package polaris.kafka.jmx.metric

/**
  * Created by chengli at 09/12/2017
  */
case class LogInfo(dir: String, logSegments: Seq[LogSegment]) {
  val bytes: Long = logSegments.map(_.bytes).sum
}

case class LogSegment(baseOffset: Long, created: Long, logBytes: Long, indexBytes: Long) {
  val bytes: Long = logBytes + indexBytes
}
