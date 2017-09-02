package observatory

import java.io.{File, PrintWriter}

import scala.collection.GenSeq
import scala.collection.JavaConversions.asScalaIterator
import scala.collection.parallel.ParSeq
import scala.io.Source
import scala.util.Try

/**
  * 4th milestone: value-added information
  */
object Manipulation {
  
  val locations: ParSeq[Location] =
    (for (lat <- -89 to 90; lon <- -180 to 179) yield Location(lat.toDouble, lon.toDouble)).par
  
  def boundedFunction(f: (Int, Int) => Double): (Int, Int) => Double =
    (x, y) => if (-89 <= x && x <= 90 && -180 <= y && y <= 179) f(x, y) else Double.NaN

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Double)]): (Int, Int) => Double = {
    val parTemperatures = temperatures.par
    val predicted: ParSeq[Double] = timer.timed("parPredictTemperature")
      { locations.map(Visualization.parPredictTemperature(parTemperatures, _)) }
    boundedFunction((x, y) => predicted(((x + 89) * 360 + (y + 180))))
  }
  
  def addSequences(a0: GenSeq[Double], a1: GenSeq[Double]): GenSeq[Double] = a0.zip(a1).map(t => t._1 + t._2)
  
  /**
    * @param temperatures Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperatures: Iterable[Iterable[(Location, Double)]]): (Int, Int) => Double = {
    
    def tempsToArray(temps: Iterable[(Location, Double)]): ParSeq[Double]= {
      val parTemperatures = temps.par
      val predicted: ParSeq[Double] = timer.timed("parPredictTemperature")
        { locations.map(Visualization.parPredictTemperature(parTemperatures, _)) }
      predicted
    }
    
    val summed = temperatures.map(tempsToArray).reduce(addSequences)
    boundedFunction((x, y) => summed(((x + 89) * 360 + (y + 180))) / temperatures.size)
  }

  /**
    * @param temperatures Known temperatures
    * @param normals A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Double)], normals: (Int, Int) => Double): (Int, Int) => Double = {
    val newTemps = makeGrid(temperatures)
    boundedFunction((x, y) => newTemps(x, y) - normals(x, y))
  }
  
  def saveGridYear(year: Int): Unit = {
    val file = new File(s"./target/yearGrids/$year")
    
    if (!file.exists) {
      file.getParentFile.mkdirs
  
      println(s"Extracting temperatures for year $year")
      val temperatures = timer.timed("extractTemperatures")
        { Extraction.temperaturesForYear(year).toLocalIterator.toIterable }
      println(s"Computing grid for year $year (got ${temperatures.size} temps)")
      val gridFunction = timer.timed("makeGrid") { makeGrid(temperatures) }
      val data = for (lat <- -89 to 90; lon <- -180 to 179) yield gridFunction(lat, lon)
      
      val writer = new PrintWriter(file)
      writer.write(data.mkString("\n"))
      writer.close
      println(s"Finished with year $year")
    }
  }
  
  def loadGridYear(year: Int): Option[Vector[Double]] = {
    val file = new File(s"./target/yearGrids/$year")
    
    if (!file.exists)
      None
    else
      Some((for (line <- Source.fromFile(file).getLines; d <- Try(line.toDouble).toOption) yield d).toVector)
  }
  
  def averageYears(_from: Int, _to: Int): (Int, Int) => Double = {
    val grids = (for (year <- _from to _to) yield loadGridYear(year)).flatten
    
    if (grids.isEmpty) (x, y) => Double.NaN
    else {
      val summed = grids.reduce(addSequences)
      boundedFunction((x, y) => summed(((x + 89) * 360 + (y + 180))) / (grids.size))
    }
  }
  
  def averageYears(x: Int): (Int, Int) => Double = averageYears(x, x)

  def main(args: Array[String]): Unit = {
    
    val startYear = 2012
    val endYear = 2012
    
    for (year <- startYear to endYear) saveGridYear(year)
    
    println(timer.toString)
  }

}

