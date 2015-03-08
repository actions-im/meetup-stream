package core

import org.apache.spark.Logging

import org.apache.log4j.{Level, Logger}

/** Utility functions for Spark Streaming examples. */
object Loggable extends Logging {

  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [ERROR] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("akka").setLevel(Level.ERROR)
      Logger.getLogger("streaming").setLevel(Level.WARN)
      Logger.getLogger("spark").setLevel(Level.WARN)
    }
  }
}