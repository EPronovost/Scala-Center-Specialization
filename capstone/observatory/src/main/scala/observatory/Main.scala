package observatory

import org.apache.log4j.{Level, Logger}

package object sparkSetup {
  
  import org.apache.spark.sql.SparkSession
  
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Capstone")
      .config("spark.master", "local[*]")
      .getOrCreate()
  
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._
}

object Main extends App {
  val stationsFile = "/stations.csv"
  val yearFile = "/2015.csv"
  
  val temperatureLocations =
    Extraction.locateTemperatures(2015, stationsFile, yearFile)
  
  println("Averaging temperatures")
  
  val averagedTemps =
    Extraction.locationYearlyAverageRecords(temperatureLocations)
  
  println("Rendering visualization")
  
  val temperatureMap =
    Visualization.visualize(averagedTemps, Visualization.colorScale)
  
  temperatureMap.output("images/tempMap.png")
}
