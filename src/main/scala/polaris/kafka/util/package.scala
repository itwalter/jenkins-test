package polaris.kafka

import java.nio.charset.StandardCharsets
import java.text.NumberFormat

import scala.language.implicitConversions

/**
  * Created by chengli at 07/12/2017
  */
package object util {
  private[this] val numberFormat = NumberFormat.getInstance()

  implicit class LongFormatted(val x: Long) {
    def formattedAsDecimal: String = numberFormat.format(x)
  }

  implicit def serializeString(data: String) : Array[Byte] = {
    data.getBytes(StandardCharsets.UTF_8)
  }

  implicit def deserializeString(data: Array[Byte]) : String  = {
    new String(data, StandardCharsets.UTF_8)
  }
}
