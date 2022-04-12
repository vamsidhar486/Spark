package com.movoto.analytics.dataqualityframework.conf

import com.typesafe.config.ConfigFactory
import com.movoto.analytics.dataqualityframework.ops.AppLogger

/**
 * Interface which reads application.conf file stored in resources directory or
 * conf file can be passed during spark-submit
 */
trait AppConfig extends AppLogger {
  val config = new ConfValues(ConfigFactory.load("app.conf"))

}
