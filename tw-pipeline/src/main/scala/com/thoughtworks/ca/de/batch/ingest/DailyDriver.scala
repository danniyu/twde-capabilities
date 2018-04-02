package com.thoughtworks.ca.de.batch.ingest

import java.time.Clock

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import com.thoughtworks.ca.de.common.utils.{DataframeUtils, DateUtils}
import org.apache.log4j.{Level, LogManager}
import com.thoughtworks.ca.de.common.utils.{ConfigUtils, CredentialUtils}

object DailyDriver {
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]) {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Skinny Pipeline: Ingest").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    //Parse argument/s
    var processingDate = DateUtils.date2TWFormat()
    if (!args.isEmpty) {
      processingDate = DateUtils.parseISO2TWFormat(args(0))
    }

    //Set S3 credentials
    CredentialUtils.setCredentialsToContext(spark)

    log.info("Load ingest configurations...")
    val ingestMap = ConfigUtils.getObjectMap(conf, "ingest.sources")
    log.info("Ingest Map: " + ingestMap.toString())

    log.info("Load target configurations...")
    val targetMap =
      ConfigUtils.getObjectMap(conf, "ingest.output.hdfs.dataSets")
    log.info("Target Map: " + targetMap.toString())

    ingestMap.foreach((ingestConfig) => {
      if (targetMap.contains(ingestConfig._1)) {
        val targetPath = conf
          .getString("ingest.output.hdfs.uri")
          .format(conf.getString("common.hdfs.lake1Path"),
                  targetMap.get(ingestConfig._1).get,
                  processingDate)
        log.info("Reading data from: " + ingestConfig._2)
        log.info("writing data to: " + targetPath)
        DataframeUtils.formatColumnHeaders(spark.read
          .format("org.apache.spark.csv")
          .option("header", true)
          .csv(ingestConfig._2))
          .write
          .parquet(targetPath)
      }
    })

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
