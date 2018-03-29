package polaris.kafka.jmx

/**
  * Created by chengli at 09/12/2017
  */
package object metric {
  private[this] val UNIT = Array[Char]('k', 'm', 'b', 't')

  // See: http://stackoverflow.com/a/4753866
  def rateFormat(rate: Double, iteration: Int): String = {
    if (rate < 100) {
      BigDecimal(rate).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString
    } else {
      val value = (rate.toLong / 100) / 10.0
      val isRound: Boolean = (value * 10) % 10 == 0 //true if the decimal part is equal to 0 (then it's trimmed anyway)
      if (value < 1000) {
        //this determines the class, i.e. 'k', 'm' etc
        if (value > 99.9 || isRound || (!isRound && value > 9.99)) {
          //this decides whether to trim the decimals
          value.toInt * 10 / 10 + "" + UNIT(iteration) // (int) value * 10 / 10 drops the decimal
        }
        else {
          value + "" + UNIT(iteration)
        }
      }
      else {
        rateFormat(value, iteration + 1)
      }
    }
  }

  // See: http://stackoverflow.com/a/3758880
  def sizeFormat(bytes: Long): String = {
    val unit = 1000
    if (bytes < unit) {
      bytes + " B"
    } else {
      val exp = (math.log(bytes) / math.log(unit)).toInt
      val pre = "kMGTPE".charAt(exp-1)
      "%.1f %sB".format(bytes / math.pow(unit, exp), pre)
    }
  }
}
