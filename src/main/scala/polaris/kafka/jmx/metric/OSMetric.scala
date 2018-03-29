package polaris.kafka.jmx.metric

/**
  * Created by chengli at 09/12/2017
  */
case class OSMetric(processCpuLoad: Double,
                    systemCpuLoad: Double) {

  def formatProcessCpuLoad: String = {
    rateFormat(processCpuLoad, 0)
  }

  def formatSystemCpuLoad: String = {
    rateFormat(systemCpuLoad, 0)
  }
}
