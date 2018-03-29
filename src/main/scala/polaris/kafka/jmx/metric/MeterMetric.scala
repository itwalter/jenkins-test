package polaris.kafka.jmx.metric


/**
  * Created by chengli at 09/12/2017
  */
case class MeterMetric(count: Long,
                       fifteenMinuteRate: Double,
                       fiveMinuteRate: Double,
                       oneMinuteRate: Double,
                       meanRate: Double) {

  def formatFifteenMinuteRate: String = {
    rateFormat(fifteenMinuteRate, 0)
  }

  def formatFiveMinuteRate: String = {
    rateFormat(fiveMinuteRate, 0)
  }

  def formatOneMinuteRate: String = {
    rateFormat(oneMinuteRate, 0)
  }

  def formatMeanRate: String = {
    rateFormat(meanRate, 0)
  }

  def +(o: MeterMetric) : MeterMetric = {
    MeterMetric(
      o.count + count,
      o.fifteenMinuteRate + fifteenMinuteRate,
      o.fiveMinuteRate + fiveMinuteRate,
      o.oneMinuteRate + oneMinuteRate,
      o.meanRate + meanRate)
  }
}
