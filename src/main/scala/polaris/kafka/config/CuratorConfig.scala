package polaris.kafka.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

import scala.beans.BeanProperty

/**
  * Created by chengli at 08/12/2017
  */
@Component
@ConfigurationProperties("zookeeper")
class CuratorConfig {
  @BeanProperty
  var connect: String = _

  @Value("${zookeeper.max.retry:100}")
  var maxRetry: Int = 100

  @Value("${zookeeper.base.sleep.ms:100}")
  var baseSleepTimeMs: Int = 100

  @Value("${zookeeper.max.sleep.ms:1000}")
  var maxSleepTimeMs: Int = 1000
}
