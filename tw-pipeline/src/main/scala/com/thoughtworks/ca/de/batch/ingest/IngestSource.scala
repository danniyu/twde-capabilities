package com.thoughtworks.ca.de.batch.ingest

import java.time.Clock

import com.thoughtworks.ca.de.common.utils.{DataframeUtils, DateUtils}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

object IngestSource {
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]) {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Skinny Pipeline: Ingest").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    var processingDate = DateUtils.date2TWFormat()
    var ingestSource = ""
    var targetPath = ""

    if (!args.isEmpty) {
      if (args.length == 3) {
        processingDate = DateUtils.parseISO2TWFormat(args(2))
      }

      ingestSource = args(0)
      log.info("Ingest Source: " + ingestSource.toString())

      targetPath = args(1) + "/" + processingDate
      log.info("Target Path: " + targetPath.toString())

    } else {
      log.error("Provide ingest source and target path args")
    }

    DataframeUtils.formatColumnHeaders(spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .csv(ingestSource))
      .write
      .parquet(targetPath)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
