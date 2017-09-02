package observatory

import org.apache.log4j.{Level, Logger}

package object projectHelpers {
  
  import org.apache.spark.sql.SparkSession
  
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Capstone")
      .config("spark.master", "local[*]")
      .getOrCreate()
}

package object timer {
  private val timeMap = collection.mutable.Map[String, (Double, Int)]()
  
  def clear() = timeMap.clear()
  
  def timed[T](title: String)(body: =>T): T = {
    val (totalTime: Long, res: T) = /*measure*/ {
      val startTime = System.currentTimeMillis()
      val r = body
      ((System.currentTimeMillis() - startTime), r)
    }
    
    timeMap.get(title) match {
      case Some((total, num)) => timeMap(title) = (total + totalTime, num + 1)
      case None => timeMap(title) = (totalTime, 1)
    }
    
    res
  }
  
  override def toString = {
    timeMap map {
      case (k, (total, num)) => s"$k : ${(total / (num * 1000)).toInt} s ($num measurements)"
    } mkString("\n")
  }
}

object Main extends App {
  
  val startYear = 1975
  val changeYear = 1990
  val endYear = 2015
  
  for (year <- startYear to endYear) {
    Manipulation.saveGridYear(year)
    Visualization2.generateTiles(year, LayerName.Temperatures, Manipulation.averageYears(year))
  }
  
  val baseline = Manipulation.averageYears(1975, changeYear - 1)
  
  for (year <- changeYear to endYear) {
    val newTemps = Manipulation.averageYears(year)
    val devs = (x: Int, y: Int) => newTemps(x, y) - baseline(x, y)
    Visualization2.generateTiles(year, LayerName.Deviations, devs)
  }
  
  println(timer.toString)
  
}
