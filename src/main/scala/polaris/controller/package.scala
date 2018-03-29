package polaris

import com.fasterxml.jackson.core.JsonProcessingException
import org.springframework.http.{HttpStatus, ResponseEntity}
import polaris.utils.JsonSerialization

import scala.util.control.NonFatal

/**
  * Created by chengli at 14/12/2017
  */
package object controller {
  case class RestResponseTemplate(
                                   var code: Int = 0,
                                   var msg: String = null,
                                   var data: Object = null
                                 ) extends JsonSerialization

  def httpTemplate(body: => Object): ResponseEntity[RestResponseTemplate] = {
    val httpResponse = RestResponseTemplate()
    try {
      httpResponse.data = body
    } catch {
      case e: IllegalArgumentException =>
        httpResponse.code = -1
        httpResponse.msg = e.getMessage
      case e: JsonProcessingException =>
        httpResponse.code = -2
        httpResponse.msg = e.getMessage
      case e: RuntimeException =>
        httpResponse.code = -3
        httpResponse.msg = e.getMessage
      case NonFatal(e) =>
        httpResponse.code = -4
        httpResponse.msg = e.toString
    }
    new ResponseEntity[RestResponseTemplate](httpResponse, HttpStatus.OK)
  }
}
