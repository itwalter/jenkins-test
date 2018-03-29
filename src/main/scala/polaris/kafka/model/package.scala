package polaris.kafka

import com.fasterxml.jackson.databind.JsonNode

/**
  * Created by chengli at 07/12/2017
  */
package object model {
  def field[T](name: String)(json: JsonNode): T = {
    json.get(name).deepCopy()
  }
}
