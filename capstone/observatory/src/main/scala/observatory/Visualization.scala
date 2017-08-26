package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import Math._
import java.io.File

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  
  // Constants
  
  type Visualizer = (Iterable[(Location, Double)], Iterable[(Double, Color)]) => Image
  
  protected val EarthRadius: Double = 6371
  protected val DisplayWidth: Int = 360
  protected val DisplayHeight: Int = 180
  protected val InterpolationPower: Double = 2
  
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
    EarthRadius * acos(sin(degreesToRadians(p1.lat)) * sin(degreesToRadians(p2.lat)) +
      cos(degreesToRadians(p1.lat)) * cos(degreesToRadians(p2.lat)) *
        cos(degreesToRadians(abs(p1.lon - p2.lon))))
  
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
    
    def square(d: Double) = d * d
    
    def interpolate(t: Double)(low: Int, high: Int): Int =
      (low + (high - low) * t).round.toInt
    
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
  
    def getPixelForSpot(x: Int, y: Int): Pixel = timer.timed("getPixelForSpot") {
      val temp = timer.timed("predictTemperature")
      { predictTemperature(temperatures, spotToLocation(x, y)) }
      val color = interpolateColor(sortedColors, temp)
      Pixel(color.red, color.green, color.blue, 255)
    }
  
    val pixels =
      for (y <- 0 until DisplayHeight; x <- 0 until DisplayWidth)
        yield getPixelForSpot(x, y)
  
    Image(DisplayWidth, DisplayHeight, pixels.toArray)
  }
  
  def efficientInterpolateColor(sortedPoints: Seq[(Double, Color)], value: Double): Color = {
    
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
  
  implicit val singlePassVisualize: Visualizer =
    (temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]) => {
  
      final case class Accumulator(num: Double, denom: Double) {
        def update(n: Double, d: Double): Accumulator = this.copy(num + n, denom + d)
      }
      
      val sortedColors = colors.toSeq.sortBy(_._1)
      
      def getPixelForSpot(x: Int, y: Int): Pixel = timer.timed("getPixelForSpot") {
        val temp = timer.timed("predictTemperature")
        { predictTemperature(temperatures, spotToLocation(x, y)) }
        val color = interpolateColor(sortedColors, temp)
        Pixel(color.red, color.green, color.blue, 255)
      }
      
      val tempAccumulators: Array[Accumulator] =
        Array.fill(DisplayHeight * DisplayWidth)(Accumulator(0,0))
      
      var pixelsToInterpolate: Set[(Int, Int)] =
        (for (y <- 0 until DisplayHeight; x <- 0 until DisplayWidth)
          yield (x,y)).toSet
      
      for (temp <- temperatures;
           spot <- pixelsToInterpolate) {
        val distance = greatCircleDistance(temp._1, spotToLocation(spot._1, spot._2))
        
        if (distance <= EarthRadius / DisplayWidth) {
          pixelsToInterpolate = pixelsToInterpolate - spot
          tempAccumulators(spot._1 + spot._2 * DisplayWidth) = Accumulator(temp._2, 1)
        } else {
          val weight = Math.pow(distance, -InterpolationPower)
          tempAccumulators(spot._1 + spot._2 * DisplayWidth) =
            tempAccumulators(spot._1 + spot._2 * DisplayWidth).update(weight * temp._2, weight)
        }
      }
      
      val temps = tempAccumulators.map(accum => accum.num / accum.denom)
      val pixels = temps.map(t => efficientInterpolateColor(sortedColors, t))
          .map(c => Pixel(c.red, c.green, c.blue, 255))
      
      Image(DisplayWidth, DisplayHeight, pixels)
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
  
  
  def main(args: Array[String]): Unit = {
    val temps = Extraction.temperaturesForYear(2015).take(200)
    
    println(s"Got ${temps.size} temperatures")
    
    println("Visualizing...")
    val image = timer.timed("naiveVisualization")
      { visualize(temps, colorScale)(naiveVisualize) }
    
    val targetFile = new File("./images/visualization-map.png")
    
    image.output(targetFile)
    
    val image2 = timer.timed("improvedVisualization")
      { visualize(temps, colorScale)(singlePassVisualize) }
  
    val targetFile2 = new File("./images/visualization-map-2.png")
    image2.output(targetFile2)
    
    println(timer.toString)
  }
}

