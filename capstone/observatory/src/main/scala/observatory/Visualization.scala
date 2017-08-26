package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import Math._

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  
  private val EarthRadius: Double = 6371
  private val DisplayWidth: Int = 360
  private val DisplayHeight: Int = 180
  
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
  

  /** https://en.wikipedia.org/wiki/Great-circle_distance */
  def greatCircleDistance(p1: Location, p2: Location): Double =
    EarthRadius * acos(sin(p1.lat) * sin(p2.lat) +
      cos(p1.lat) * cos(p2.lat) * cos(abs(p1.lon - p2.lon)))
  
  /**
    * https://en.wikipedia.org/wiki/Inverse_distance_weighting
    *
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Double)], location: Location): Double = {
    val p = 2
    
    val distancedTemps: Iterable[(Double, Double)] =
      temperatures.map({ case (loc, temp) => (greatCircleDistance(loc, location), temp) })
    val closestLocation = distancedTemps.minBy(_._1)
    
    if (closestLocation._1 < 1) closestLocation._2
    else {
      val weightedTemps =
        distancedTemps.map({ case (d, t) => (Math.pow(d, -p), t)})
      
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
    
    def square(d: Double) = d * d
    
    def interpolate(t: Double)(low: Int, high: Int): Int =
      (low + (high - low) * t).toInt
    
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
    
  def spotToLocation(x: Int, y: Int): Location = Location(90 - y, x - 180)

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {
    
    def getPixelForSpot(x: Int, y: Int): Pixel = {
      val color = interpolateColor(colors, predictTemperature(temperatures, spotToLocation(x, y)))
      Pixel(color.red, color.green, color.blue, 255)
    }
    
    val pixels = for (x <- 0 until DisplayWidth;
          y <- 0 until DisplayWidth) yield getPixelForSpot(x, y)
    
    Image(DisplayWidth, DisplayHeight, pixels.toArray)
  }
}

