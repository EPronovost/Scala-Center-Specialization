package observatory

import observatory.projectHelpers.spark
import org.apache.log4j.{Level, Logger}

import scala.reflect.ClassTag
import scala.util.Random

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
      case (k, (total, num)) => s"$k : ${(total / num).toInt} ms ($num measurements)"
    } mkString("\n")
  }
}

object Main extends App {
  
}
