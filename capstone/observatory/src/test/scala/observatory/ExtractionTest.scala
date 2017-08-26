package observatory

import java.time.LocalDate

import org.scalatest.FunSuite

trait ExtractionTest extends FunSuite {
  
  def printIterable(iter: Iterable[_]): Unit = iter.foreach(println(_))
  
  def compareIterables[A: Ordering](a: Iterable[A], b: Iterable[A]): Boolean =
    (a.toSeq.sorted zip b.toSeq.sorted).forall(t => t._1 == t._2)
  
  implicit object locationOrdering extends Ordering[Location] {
    override def compare(x: Location, y: Location): Int =
      if (x.lat == y.lat) (x.lon compare y.lon) else (x.lat compare y.lat)
  }
  
  implicit object localDateOrdering extends Ordering[LocalDate] {
    override def compare(x: LocalDate, y: LocalDate): Int = x.compareTo(y)
  }
  
  test("Assignment Example") {
    
    val stationsFile = "/testStations.csv"
    val tempsFile = "/testTemperatures.csv"
    
    val expectedAll = Seq(
      (LocalDate.of(2015, 8, 11), Location(37.35, -78.433), 27.3),
      (LocalDate.of(2015, 12, 6), Location(37.358, -78.438), 0.0),
      (LocalDate.of(2015, 1, 29), Location(37.358, -78.438), 2.0)
    )
    
    val expectedAverages = Seq(
      (Location(37.35, -78.433), 27.3),
      (Location(37.358, -78.438), 1.0)
    )
    
    val stations = Extraction.getStations(stationsFile)
    stations.show()
    
    val temperatures = Extraction.getTemperatures(tempsFile)
    temperatures.show()
    
    val joined = stations.join(temperatures, List("STN", "WBAN"))
    joined.show
    
    val tempsIterable = Extraction.locateTemperatures(2015, stationsFile, tempsFile)
    printIterable(tempsIterable)
    
    assert(compareIterables(tempsIterable, expectedAll))
    
    val averagesIterable = Extraction.locationYearlyAverageRecords(tempsIterable)
    printIterable(averagesIterable)
    
    assert(compareIterables(averagesIterable, expectedAverages))
  }
}