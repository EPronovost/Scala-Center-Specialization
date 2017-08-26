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

package object timer {
  private val timeMap = collection.mutable.Map[String, (Double, Int)]()
  
  def clear() = timeMap.clear()
  
  def timed[T](title: String)(body: =>T): T = {
    var res: Option[T] = None
    val totalTime = /*measure*/ {
      val startTime = System.currentTimeMillis()
      res = Some(body)
      (System.currentTimeMillis() - startTime)
    }
    
    timeMap.get(title) match {
      case Some((total, num)) => timeMap(title) = (total + totalTime, num + 1)
      case None => timeMap(title) = (totalTime, 1)
    }
    
    //println(s"$title: ${totalTime} ms; avg: ${timeMap(title)._1 / timeMap(title)._2}")
    res.get
  }
  
  override def toString = {
    timeMap map {
      case (k, (total, num)) => s"$k : ${(total / num * 100).toInt / 100.0} ms ($num measurements)"
    } mkString("\n")
  }
}

object Main extends App {
  
}
