package observatory

case class Location(lat: Double, lon: Double) {
  
  protected val degreesToRadians: Double => Double = _ * Math.PI / 180
  
  val lonRad = degreesToRadians(lon)
  val latRadSin = Math.sin(degreesToRadians(lat))
  val latRadCos = Math.cos(degreesToRadians(lat))
}

case class Color(red: Int, green: Int, blue: Int)

