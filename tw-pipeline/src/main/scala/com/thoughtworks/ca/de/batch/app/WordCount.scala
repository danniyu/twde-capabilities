package com.thoughtworks.ca.de.batch.app

import java.time.Clock

import com.thoughtworks.ca.de.common.utils.DateUtils
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager}

object WordCount {
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Spark 2 Word Count").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    //Parse argument/s
    var processingDate = DateUtils.date2TWFormat()
    if (!args.isEmpty) {
      processingDate = DateUtils.parseISO2TWFormat(args(0))
    }

    val outputPath = conf.getString("apps.WordCount.output").format(processingDate)

    import spark.implicits._

    log.info("Reading data: " + conf.getString("apps.WordCount.input"))
    log.info("Writing data: " + outputPath)

    spark.read
      .text(conf.getString("apps.WordCount.input")) // Read file
      .as[String]                                   // As a data set
      .flatMap(text => text.split("\\s+"))          // Split words using regex (distributed map)
      .groupByKey(_.toLowerCase())                  // Reduce action
      .count()                                      // Aggregate
      .withColumnRenamed("count(1)","count")        // Cannot save data with column name count(1)
      .write                                        // Write to file
      .csv(outputPath)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()

  }
}
