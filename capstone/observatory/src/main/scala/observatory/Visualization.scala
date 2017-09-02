package observatory

import java.io.File
import java.lang.Math._

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.spark.sql.Dataset

import scala.collection.parallel.{ParIterable, ParSeq}

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  
  import projectHelpers._
  import spark.implicits._
  
  // Constants
  
  type Visualizer = (Iterable[(Location, Double)], Iterable[(Double, Color)]) => Image
  
  val EarthRadius: Double = 6371
  protected val DisplayWidth: Int = 360
  protected val DisplayHeight: Int = 180
  protected val InterpolationPower: Double = 5
  
  val colorScale: Seq[(Double, Color)] = Seq(
    (60, Color(255, 255, 255)),
    (32, Color(255, 0, 0)),
    (12, Color(255, 255, 0)),
    (0, Color(0, 255, 255)),
    (-15, Color(0, 0, 255)),
    (-27, Color(255, 0, 255)),
    (-50, Color(33, 255, 107)),
    (-60, Color(0, 0, 0))
  )
  
  // Helpter functions
  
  protected val degreesToRadians: Double => Double = _ * PI / 180
  
  protected def spotToLocation(x: Int, y: Int): Location = Location(90 - y, x - 180)
  
  /** https://en.wikipedia.org/wiki/Great-circle_distance */
  protected def greatCircleDistance(p1: Location, p2: Location): Double =
    EarthRadius * acos(p1.latRadSin * p2.latRadSin +
      p1.latRadCos * p2.latRadCos * cos(p1.lonRad - p2.lonRad))
  
  // Required Milestone Methods
  
  /**
    * https://en.wikipedia.org/wiki/Inverse_distance_weighting
    *
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Double)], location: Location): Double = {
    val distancedTemps: Iterable[(Double, Double)] =
      temperatures.map({ case (loc, temp) => (greatCircleDistance(loc, location), temp) })
    val closestLocation = distancedTemps.minBy(_._1)
    
    if (closestLocation._1 <= EarthRadius / DisplayWidth) closestLocation._2
    else {
      val weightedTemps =
        distancedTemps.map({ case (d, t) => (Math.pow(d, -InterpolationPower), t)})
      
      weightedTemps.map(t => t._1 * t._2).reduce(_ + _) / weightedTemps.map(_._1).reduce(_ + _)
    }
  }
  
  /**
    * https://en.wikipedia.org/wiki/Linear_interpolation
    *
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Double, Color)], value: Double): Color = {
    
    def interpolate(t: Double)(low: Int, high: Int): Int =
      (high * t + low * (1 - t)).round.toInt
    
    val sortedPoints = points.toSeq.sortBy(_._1)
    val rightIndex = sortedPoints.indexWhere(_._1 >= value)
    
    rightIndex match {
      case -1 => sortedPoints.last._2
      case 0 => sortedPoints.head._2
      case _ => {
        val (lower, upper) = (sortedPoints(rightIndex - 1), sortedPoints(rightIndex))
        val interpolation: (Int, Int) => Int = interpolate((value - lower._1) / (upper._1 - lower._1))
        
        Color(interpolation(lower._2.red, upper._2.red),
          interpolation(lower._2.green, upper._2.green),
          interpolation(lower._2.blue, upper._2.blue)
        )
      }
    }
  }
  
  // Visualizer
  
  val naiveVisualize: Visualizer =
    (temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]) => {
    val sortedColors = colors.toSeq.sortBy(_._1)
  
    def getPixelForSpot(x: Int, y: Int): Pixel = {
      val temp = predictTemperature(temperatures, spotToLocation(x, y))
      val color = interpolateColor(sortedColors, temp)
      Pixel(color.red, color.green, color.blue, 255)
    }
  
    val pixels =
      for (y <- 0 until DisplayHeight; x <- 0 until DisplayWidth)
        yield getPixelForSpot(x, y)
  
    Image(DisplayWidth, DisplayHeight, pixels.toArray)
  }
  
  def sortedInterpolateColor(sortedPoints: Seq[(Double, Color)], value: Double): Color = {
    
    def interpolate(t: Double)(low: Int, high: Int): Int =
      (low + (high - low) * t).toInt
    
    val rightIndex = sortedPoints.indexWhere(_._1 >= value)
    
    rightIndex match {
      case -1 => sortedPoints.last._2
      case 0 => sortedPoints.head._2
      case _ => {
        val (lower, upper) = (sortedPoints(rightIndex - 1), sortedPoints(rightIndex))
        val interpolation: (Int, Int) => Int = interpolate((value - lower._1) / (upper._1 - lower._1))
        
        Color(interpolation(lower._2.red, upper._2.red),
          interpolation(lower._2.green, upper._2.green),
          interpolation(lower._2.blue, upper._2.blue)
        )
      }
    }
  }
  
  def batchPredictTemperatures(temperatures: Iterable[(Location, Double)],
                               locations: IndexedSeq[Location],
                               distanceCuttoff: Double = EarthRadius / DisplayWidth): Array[Double] = {
  
    final case class Accumulator(num: Double, denom: Double, locked: Boolean) {
      def update(n: Double, d: Double): Accumulator = this.copy(num + n, denom + d)
    }
    
    val tempAccumulators: Array[Accumulator] = Array.fill(locations.size)(Accumulator(0, 0, false))
    
    for (localizedTemp <- temperatures;
         locIndex <- 0 until locations.size;
         if (!tempAccumulators(locIndex).locked)) {
      val distance = greatCircleDistance(localizedTemp._1, locations(locIndex))
  
      if (distance <= distanceCuttoff) {
        tempAccumulators(locIndex) = Accumulator(localizedTemp._2, 1, true)
      } else {
        val weight = Math.pow(distance, -InterpolationPower)
        tempAccumulators(locIndex) =
          tempAccumulators(locIndex).update(weight * localizedTemp._2, weight)
      }
    }
  
    tempAccumulators.map(accum => accum.num / accum.denom)
  }
  
  val singlePassVisualize: Visualizer =
    (temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]) => {
      
      val sortedColors = colors.toSeq.sortBy(_._1)
      
      val locations: Vector[Location] =
        (for (y <- 0 until DisplayHeight; x <- 0 until DisplayWidth)
          yield spotToLocation(x, y)).toVector
      
      val temps = batchPredictTemperatures(temperatures, locations)
      val pixels = temps.map(t => sortedInterpolateColor(sortedColors, t))
          .map(c => Pixel(c.red, c.green, c.blue, 255))
      
      Image(DisplayWidth, DisplayHeight, pixels)
    }
  
  def addTwoTuples[T: Numeric](t1: (T, T), t2: (T, T)): (T, T) =
    (implicitly[Numeric[T]].plus(t1._1, t2._1), implicitly[Numeric[T]].plus(t1._2, t2._2))
  
  def parPredictTemperature(temperatures: ParIterable[(Location, Double)],
                            location: Location,
                            distanceCutoff: Double = 1.0): Double = {
    val distancedTemps =
      temperatures.map({ case (loc, temp) => (greatCircleDistance(loc, location), temp) })
    
    val closestLocation = distancedTemps.minBy(_._1)
    
    if (closestLocation._1 <= distanceCutoff) closestLocation._2
    else {
      val fraction = distancedTemps
        .map({ case (d, t) => (Math.pow(d, -InterpolationPower) * t, Math.pow(d, -InterpolationPower))})
        .aggregate((0.0, 0.0))(addTwoTuples[Double], addTwoTuples[Double])
      fraction._1 / fraction._2
    }
  }
  
  implicit def parVisualize(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {
    val pixelLocations =
      (for (y <- 0 until DisplayHeight; x <- 0 until DisplayWidth)
        yield spotToLocation(x, y))
  
    Image(DisplayWidth, DisplayHeight, parComputePixels(temperatures.par, pixelLocations.par, colors, EarthRadius / DisplayWidth))
  }
  
  def parComputePixels(temperatures: ParIterable[(Location, Double)],
                   locations: ParSeq[Location],
                   colors: Iterable[(Double, Color)],
                   distanceCutoff: Double,
                   alpha: Int = 255): Array[Pixel] = {
    
    val sortedColors = colors.toSeq.sortBy(_._1)
  
    val temps = timer.timed("parPredictTemperature") { locations.map(parPredictTemperature(temperatures, _, distanceCutoff)) }
    val pixels = temps
      .map(t => sortedInterpolateColor(sortedColors, t))
      .map(c => Pixel(c.red, c.green, c.blue, alpha))
  
    pixels.toArray
  }
  
  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Double)],
                colors: Iterable[(Double, Color)])
               (implicit ev: Visualizer): Image =
    ev(temperatures, colors)
  
  def sparkVisualize(temperatures: Dataset[(Location, Double)],
                     colors: Iterable[(Double, Color)]): Image = {
    val sortedColors = colors.toSeq.sortBy(_._1)
    
    val storedTemps = temperatures.cache
    
    val displayTemps =
      (for (y <- 0 until DisplayHeight; x <- 0 until DisplayWidth)
        yield spotToLocation(x, y))
        .par
        .map(pixelLocation => {
      val distances = storedTemps.map({ case (loc, temp) => (greatCircleDistance(loc, pixelLocation), temp) }).cache
      val closest = distances.reduce((t1: (Double, Double), t2: (Double, Double)) => if (t1._1 < t2._1) t1 else t2)
      if (closest._1 <= EarthRadius / DisplayWidth) {
        closest._2
      } else {
        val interpolated = distances
          .map({ case (distance, temp) => (Math.pow(distance, -InterpolationPower), Math.pow(distance, -InterpolationPower) * temp) })
          .reduce((t1: (Double, Double), t2: (Double, Double)) => (t1._1 + t2._1, t2._2 + t2._2))
        interpolated._1 / interpolated._2
      }
    })
  
    val pixels = displayTemps
      .map(t => sortedInterpolateColor(sortedColors, t))
      .map(c => Pixel(c.red, c.green, c.blue, 255))
      .toArray
  
    Image(DisplayWidth, DisplayHeight, pixels)
  }
  
  
  def main(args: Array[String]): Unit = {
    
    import scala.collection.JavaConversions.asScalaIterator
    
    val temps = Extraction.temperaturesForYear(2015).sample(true, 0.1)
    val classicTemps = temps.toLocalIterator.toIterable

    println(s"Got ${temps.count} temperatures")
    println("Visualizing...")
  
    println("Default Visualization")
    val image = timer.timed("visualize")
      { visualize(classicTemps, colorScale) }
  
    val targetFile = new File("./target/images/visualization-map.png")
    targetFile.getParentFile.mkdirs
    image.output(targetFile)
  
    println("Naive Visualization")
    val image1 = timer.timed("naiveVisualization")
      { visualize(classicTemps, colorScale)(naiveVisualize) }
    
    val targetFile1 = new File("./target/images/visualization-map.png")
    targetFile1.getParentFile.mkdirs
    image1.output(targetFile1)
  
    println("Improved Visualization")
    val image2 = timer.timed("improvedVisualization")
      { visualize(classicTemps, colorScale)(singlePassVisualize) }

    val targetFile2 = new File("./target/images/visualization-map-2.png")
    targetFile2.getParentFile.mkdirs
    image2.output(targetFile2)
  
//    println("Spark Visualization")
//    val image3 = timer.timed("sparkVisualization")
//      { sparkVisualize(temps, colorScale) }
//
//    val targetFile3 = new File("./target/images/visualization-map-3.png")
//    targetFile3.getParentFile.mkdirs
//    image3.output(targetFile3)
  
    println("Parallel Visualization")
    val image4 = timer.timed("parVisualization")
      { parVisualize(classicTemps, colorScale) }
    
    val targetFile4 = new File("./target/images/visualization-map-4.png")
    targetFile4.getParentFile.mkdirs
    image4.output(targetFile4)
    
    println(timer.toString)
  }
}

