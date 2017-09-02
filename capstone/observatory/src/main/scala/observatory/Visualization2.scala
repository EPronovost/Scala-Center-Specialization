package observatory

import java.io.File

import com.sksamuel.scrimage.{Image, Pixel}


/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {
  
  final val deviationColorScale: Seq[(Double, Color)] = Seq[(Double, Color)](
    (7, Color(0, 0, 0)),
    (4, Color(255, 0, 0)),
    (2, Color(255, 255, 0)),
    (0, Color(255, 255, 255)),
    (-2, Color(0, 255, 255)),
    (-7, Color(0, 0, 255)))

  /**
    * @param x X coordinate between 0 and 1
    * @param y Y coordinate between 0 and 1
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    x: Double,
    y: Double,
    d00: Double,
    d01: Double,
    d10: Double,
    d11: Double
  ): Double = {
    assert(0 <= x && x <= 1, s"x=$x must be in [0, 1]")
    assert(0 <= y && y <= 1, s"y=$y must be in [0, 1]")
    d00 * (1-x) * (1-y) + d10 * x * (1-y) + d01 * (1-x) * y + d11 * x * y
  }

  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param zoom Zoom level of the tile to visualize
    * @param x X value of the tile to visualize
    * @param y Y value of the tile to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: (Int, Int) => Double,
    colors: Iterable[(Double, Color)],
    zoom: Int,
    x: Int,
    y: Int
  ): Image = {
    
    val sortedColors = colors.toSeq.sortBy(_._1)
  
    val locations =
      (for (yi <- 0 until 256; xi <- 0 until 256)
        yield Interaction.tileLocation(zoom + 8, (x << 8) + xi, (y << 8) + yi)).par
    
    
    def getPixel(loc: Location): Pixel = {
      val (xLow, yLow) = (loc.lat.floor.toInt, loc.lon.floor.toInt)
      val (xFraction, yFraction) = (loc.lat - xLow, loc.lon - yLow)
      val value = bilinearInterpolation(xFraction, yFraction,
        grid(xLow, yLow), grid(xLow, yLow + 1), grid(xLow + 1, yLow), grid(xLow + 1, yLow + 1))
      val color = Visualization.sortedInterpolateColor(sortedColors, value)
      Pixel(color.red, color.green, color.blue, 127)
    }
    
    val pixels = locations.map(getPixel)
    Image(256, 256, pixels.toArray)
  }
  
  def generateTiles(year: Int, imageType: LayerName, grid: (Int, Int) => Double): Unit = {
    
    val colorScale = if (imageType == LayerName.Temperatures) Visualization.colorScale else deviationColorScale;
    
    for (zoom <- 0 to 3; x <- 0 until (1 << zoom); y <- 0 until (1 << zoom)) {
      val file = new File(s"./target/${imageType.id}/$year/$zoom/$x/$y.png")
      if (!file.exists) {
        val image: Image = visualizeGrid(grid, colorScale, zoom, x, y)
        file.getParentFile.mkdirs
        image.output(file)
      }
    }
    
    println(s"Done generating tiles for year $year, layer ${imageType.id}")
  }
  
  def main(args: Array[String]): Unit = {
  
    for (year <- 1975 to 1980) generateTiles(year, LayerName.Temperatures, Manipulation.averageYears(year))
  
    val averages = Manipulation.averageYears(1975, 1989)
  
    for (year <- List(2010, 2011, 2015)) {
      val newTemps = Manipulation.averageYears(year)
      def deviations(x: Int, y: Int): Double = newTemps(x, y) - averages(x, y)
      generateTiles(year, LayerName.Deviations, deviations)
      generateTiles(year, LayerName.Temperatures, newTemps)
    }
  }

}
