package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import Math._
import java.io.File

import scala.reflect.io.File

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {
  
  /**
    * http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Scala
    *
    * @param zoom Zoom level
    * @param x    X coordinate
    * @param y    Y coordinate
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(zoom: Int, x: Int, y: Int): Location =
    Location(
      toDegrees(atan(sinh(PI * (1.0 - 2.0 * y.toDouble / (1 << zoom))))),
      x.toDouble / (1 << zoom) * 360.0 - 180.0)
  
  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @param zoom         Zoom level
    * @param x            X coordinate
    * @param y            Y coordinate
    * @return A 256×256 image showing the contents of the tile defined by `x`, `y` and `zooms`
    */
  def tile(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)], zoom: Int, x: Int, y: Int): Image = {
    
    val tileZoomFactor = 8
    
    val sortedColors = colors.toSeq.sortBy(_._1)
    
    val pixelLocations =
      (for (yi <- 0 until 1 << tileZoomFactor; xi <- 0 until 1 << tileZoomFactor)
        yield tileLocation(zoom + tileZoomFactor, (x << tileZoomFactor) + xi, (y << tileZoomFactor) + yi)).par
    
    val parTemperatures = temperatures.par
    
    val pixels = timer.timed("parComputePixels") { Visualization.parComputePixels(parTemperatures,
      pixelLocations, colors, Visualization.EarthRadius / (1 << (zoom + tileZoomFactor + 1)), 127) }
    
    Image(1 << tileZoomFactor, 1 << tileZoomFactor, pixels).scale(1 << (8-tileZoomFactor))
  }
  
  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    *
    * @param yearlyData    Sequence of (year, data), where `data` is some data associated with
    *                      `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
                           yearlyData: Iterable[(Int, Data)],
                           generateImage: (Int, Int, Int, Int, Data) => Unit
                         ): Unit = {
    
    def processYear(year: Int, data: Data): Unit = {
      for (level <- 0 to 3;
           x <- 0 until (1 << level);
           y <- 0 until (1 << level))
        generateImage(year, level, x, y, data)
    }
  
    yearlyData.foreach(p => processYear(p._1, p._2))
  }
  
  def main(args: Array[String]): Unit = {
  
    import scala.collection.JavaConversions.asScalaIterator
  
    val year = 2015
    val yearTemps = Extraction.temperaturesForYear(year).sample(false, 0.01).toLocalIterator.toIterable
    
    println(s"Loaded ${yearTemps.size} temps")
    
    val fullData = Iterable((year, yearTemps))

    def generateImage(year: Int, zoom: Int, x: Int, y: Int, temperatures: Iterable[(Location, Double)]): Unit = {
      println(s"Generating image for (zoom=$zoom, x=$x, y=$y)")
      val image = timer.timed("renderTile") { tile(temperatures, Visualization.colorScale, zoom, x, y) }

      val file = new java.io.File(s"./target/temperatures/$year/$zoom/$x-$y.png")
      file.getParentFile.mkdirs()
      image.output(file)
    }
    generateTiles(fullData, generateImage)
  
    println(timer.toString)
  }
  
}