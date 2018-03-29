package polaris.elasticsearch.config

import java.util

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

import scala.beans.BeanProperty

/**
  * Created by chengli at 15/12/2017
  */
@Component
@ConfigurationProperties("es")
class EsConfig {
  @BeanProperty val node = new util.ArrayList[Node]
}
