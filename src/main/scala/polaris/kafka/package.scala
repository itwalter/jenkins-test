package polaris

import java.nio.charset.StandardCharsets

import scala.language.implicitConversions

/**
  * Created by chengli at 07/12/2017
  */
package object kafka {
  def nodeFromPath(s: String) : String = {
    val l = s.lastIndexOf("/")
    s.substring(l+1)
  }

  implicit def asString(ba: Array[Byte]) : String = {
    new String(ba, StandardCharsets.UTF_8)
  }

  implicit def asByteArray(str: String) : Array[Byte] = {
    str.getBytes(StandardCharsets.UTF_8)
  }
}
