package com.thoughtworks.ca.de.batch.transform

import java.time.Clock

import com.thoughtworks.ca.de.common.utils.{ConfigUtils, DateUtils}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

object DailyDriver {
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]) {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Skinny Pipeline: Transform").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    //Parse argument/s
    var processingDate = DateUtils.date2TWFormat()
    if (!args.isEmpty) {
      processingDate = DateUtils.parseISO2TWFormat(args(0))
    }

    log.info("Load transformation configurations...")
    val dataSets = ConfigUtils.getObjectMap(conf,"transform.hdfs.dataSets")
    log.info("Transformation data sets: "+dataSets.toString())

    dataSets.foreach((dataSet)=>{
      val inputPath = conf.getString("transform.hdfs.uri").format(conf.getString("common.hdfs.lake1Path"),
        dataSet._2, processingDate)
      val outputPath = conf.getString("transform.hdfs.uri").format(conf.getString("common.hdfs.lake2Path"),
        dataSet._2, processingDate)
      log.info("Reading data from: "+inputPath)
      log.info("writing data to: "+outputPath)
      Transformation.transform(spark.read.parquet(inputPath),dataSet._2).write.parquet(outputPath)
    })

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
