package polaris

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.netflix.eureka.EnableEurekaClient
import org.springframework.context.annotation.{Bean, Primary}

/**
  * Created by chengli at 07/12/2017
  */
@EnableEurekaClient
@SpringBootApplication
class Polaris {
  /**
    * Custom Jackson ObjectMapper for ScalaModule, see link:
    * [[https://docs.spring.io/spring-boot/docs/current/reference/html/howto-spring-mvc.html#howto-customize-the-jackson-objectmapper]]
    * @return new ObjectMapper
    */
  @Primary @Bean
  def mapper: ObjectMapper = {
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    mapper.disable(MapperFeature.DEFAULT_VIEW_INCLUSION)
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
    mapper
  }
}

object Polaris extends App {
  // set java runtime properties
  System.setProperty("java.net.preferIPv4Stack", "true")
  // start main program
  SpringApplication.run(classOf[Polaris], args:_*)
}