package polaris.controller

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Configuration
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.{EnableWebSecurity, WebSecurityConfigurerAdapter}


/**
  * Created by chengli at 15/12/2017
  */

@Configuration
@EnableWebSecurity
class WebSecurityConfig extends WebSecurityConfigurerAdapter {

  @throws[Exception]
  override def configure(http: HttpSecurity): Unit = http.csrf.disable

  @Autowired
  @throws[Exception]
  def configureGlobal(auth: AuthenticationManagerBuilder): Unit = {}
}

