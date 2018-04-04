package com.thoughtworks.ca.de.batch.transform

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, collect_list, date_format, lit, to_date, udf}

case class Bike(tripduration: String,
                starttime: String,
                stoptime: String,
                start_station_id: String,
                start_station_name: String,
                start_station_latitude: String,
                start_station_longitude: String,
                end_station_id: String,
                end_station_name: String,
                end_station_latitude: String,
                end_station_longitude: String,
                bikeid: String,
                usertype: String,
                birth_year: String,
                gender: String,
                uuid: String)

object Transformation {
  val log = LogManager.getRootLogger
  //Sample udf
  def humidityToRangeUdf = udf((humidity: Int) => ((humidity + 5) / 10) * 10)

  //Dataframe transformation functions
  def uberTransformation(dataFrame: DataFrame) = {
    dataFrame
      .withColumn("DATE", to_date(col("DATE"), "MM/dd/yyyy"))
      .withColumn("dayofweek", date_format(col("DATE"), "EEEE"))
  }
  def weatherTransformation(dataFrame: DataFrame) = {
    dataFrame
      .withColumn("date", to_date(col("date"), "yyyyMMdd"))
      .withColumn("dayofweek", date_format(col("date"), "EEEE"))
      .withColumn("humidity_range", humidityToRangeUdf(col("hum_avg")))
  }
  def transitTransformation(dataFrame: DataFrame) = dataFrame

  def mapBikeGroups(key:String,iterator: Iterator[Bike]):Array[Bike]={
    println("mapBikeGroups key: "+key)
    val bikeTripArr = iterator.toArray
    val start = 1
    var end = bikeTripArr.size / 10
    if (end < 2)
      end = 2
    val withId = bikeTripArr.map(bikeTrip=> {
      println("mapBikeGroups bikeTrip: "+bikeTrip)
      Bike(
        bikeTrip.tripduration,
        bikeTrip.starttime,
        bikeTrip.stoptime,
        bikeTrip.start_station_id,
        bikeTrip.start_station_name,
        bikeTrip.start_station_latitude,
        bikeTrip.start_station_longitude,
        bikeTrip.end_station_id,
        bikeTrip.end_station_name,
        bikeTrip.end_station_latitude,
        bikeTrip.end_station_longitude,
        bikeTrip.bikeid,
        bikeTrip.usertype,
        bikeTrip.birth_year,
        bikeTrip.gender,
        key +"_"+ new scala.util.Random().nextInt((end - start) + 1))
    })

    print("mapBikeGroups bikeTripArray 1: "+withId)
    withId
  }
  def bikeshareTransformation(dataFrame: DataFrame) = {
    import dataFrame.sparkSession.implicits._
    val newDF = dataFrame.withColumn("uuid", lit(""))
    val bikesData = newDF.as[Bike]
    val groupedBikes = bikesData.groupByKey(bike =>
      bike.gender + "_" + bike.usertype + "_" + bike.birth_year)
    println("DEBUG groupedBikes data : "+groupedBikes.keys.show(15))
    val mappedGrpoups = groupedBikes.mapGroups((key, iterator) => mapBikeGroups(key,iterator))
    println("DEBUG COUNT of groups: "+mappedGrpoups.count())
    println("DEBUG Schema of groups: "+mappedGrpoups.show(15))
    mappedGrpoups.printSchema()
    mappedGrpoups.flatMap(bikes => bikes).toDF()
  }

  //Mapping of transformation functions to date set ids
  val transformationMap = Map[String, (DataFrame) => DataFrame](
    "uberdata" -> uberTransformation,
    "weatherdata" -> weatherTransformation,
    "transitData" -> transitTransformation,
    "bikesharedata" -> bikeshareTransformation
  )

  def transform(dataFrame: DataFrame, datasetId: String): DataFrame = {
    dataFrame.printSchema()
    transformationMap.get(datasetId).get(dataFrame)
  }
}
