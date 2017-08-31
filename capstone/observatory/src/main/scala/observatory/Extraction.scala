package observatory

import java.io.{File, InputStream}
import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * 1st milestone: data extraction
  */
object Extraction {
  
  import projectHelpers._
  import spark.implicits._
  
  val stationsSchema = StructType(
    StructField("STN", StringType, true) ::
      StructField("WBAN", StringType, true) ::
      StructField("Lat", new DecimalType(10, 6), true) ::
      StructField("Lon", new DecimalType(10, 6), true) ::
      Nil
  )
  
  val temperatureSchema = StructType(
    StructField("STN", StringType, true) ::
      StructField("WBAN", StringType, true) ::
      StructField("Month", IntegerType, true) ::
      StructField("Day", IntegerType, true) ::
      StructField("Temp", new DecimalType(10, 6), true) ::
      Nil
  )
  
  protected def parseCsv(resource: String): RDD[Array[String]] = {
    val inputStream = Extraction.getClass.getResourceAsStream(resource)
    val iterator = scala.io.Source.fromInputStream(inputStream).getLines()
  
    spark.sparkContext
      .parallelize(iterator.toSeq)
      .map(_.split(","))
  }
  
  /**
    * @param resource       The path to the station resource
    *
    * @return The read stations DataFrame along with its column names.
    * */
  protected def getStations(resource: String = "/stations.csv"): DataFrame = {
    
    def stationsRow(columns: Array[String]): Option[Row] = columns match {
      case Array(_, _, _, _, "") => stationsRow(columns.take(4))
      case Array(stn, wban, lat, long) =>
        Try(Row(stn, wban,
            BigDecimal(lat), BigDecimal(long))).toOption
      case _ => None
    }
    
    val data = parseCsv(resource).flatMap(stationsRow)
    spark.createDataFrame(data, stationsSchema)
  }
  
  protected def getTemperatures(resource: String): DataFrame = {
  
    def convertTemp(d: BigDecimal): BigDecimal = d match {
      case _  if (d < 9999) => (d - 32) * 5.0 / 9.0
    }
  
    def temperatureRow(columns: Array[String]): Option[Row] = columns match {
      case Array(_, _, _, _, _, "") => temperatureRow(columns.take(5))
      case Array(stn, wban, month, day, temp) =>
        Try(Row(stn, wban, month.toInt, day.toInt,
          convertTemp(BigDecimal(temp)))).toOption
      case _ => None
    }
  
    val data = parseCsv(resource).flatMap(temperatureRow)
    spark.createDataFrame(data, temperatureSchema)
  }
  
  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {
  
    val stationsDataFrame: DataFrame = getStations(stationsFile)
    val temperatureDataFrame: DataFrame = getTemperatures(temperaturesFile)
    
    val joinedDataFrame = stationsDataFrame.join(temperatureDataFrame, List("STN", "WBAN"))
    
    def rowMapper(r: Row): (LocalDate, Location, Double) =
      (LocalDate.of(year, r.getAs[Int]("Month"), r.getAs[Int]("Day")),
      Location(r.getAs[java.math.BigDecimal]("Lat").doubleValue, r.getAs[java.math.BigDecimal]("Lon").doubleValue),
      r.getAs[java.math.BigDecimal]("Temp").doubleValue)
    
    joinedDataFrame
      .toLocalIterator.toIterable
      .map(rowMapper)
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    val locationGroups = records.map(t => (t._2, t._3)).groupBy(_._1)
    locationGroups.mapValues(
      _.map(t => (t._2, 1.0)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    ).mapValues(t => t._1 / t._2)
  }
  
  def temperaturesForYear(year: Int): Dataset[(Location, Double)] = {
    
    val file = new File(s"./target/yearTemps/$year.dat")
    
    if (file.exists()) {
      spark.sparkContext.objectFile[(Location, Double)](file.getPath).toDS
    }
    else {
      val stationsDataFrame: DataFrame = getStations() // ["STN", "WBAN", "Lat", "Lon"]
      val temperatureDataFrame: DataFrame = getTemperatures(s"/$year.csv").select("STN", "WBAN", "Temp")
      val joinedDataFrame = stationsDataFrame.join(temperatureDataFrame, List("STN", "WBAN")).select("Lat", "Lon", "Temp")
  
      val data = joinedDataFrame.groupBy("Lat", "Lon").avg("Temp").withColumnRenamed("avg(Temp)", "Temp")
      
      val result = data.map(r =>
        (Location(r.getAs[java.math.BigDecimal]("Lat").doubleValue, r.getAs[java.math.BigDecimal]("Lon").doubleValue),
          r.getAs[java.math.BigDecimal]("Temp").doubleValue))
      
      file.getParentFile.mkdirs
      result.rdd.saveAsObjectFile(file.getPath)
      result
    }
  }
  
  def main(args: Array[String]): Unit = {
    val temps = temperaturesForYear(2015)
    
    temps.show()
  }
}
