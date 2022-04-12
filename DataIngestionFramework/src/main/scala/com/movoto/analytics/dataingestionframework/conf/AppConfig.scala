package com.movoto.analytics.dataingestionframework.conf

import com.typesafe.config.ConfigFactory
import com.movoto.analytics.dataingestionframework.ops.AppLogger

trait AppConfig extends AppLogger {
  val config = new ConfValues(ConfigFactory.load("ES-S3.conf"))

}
