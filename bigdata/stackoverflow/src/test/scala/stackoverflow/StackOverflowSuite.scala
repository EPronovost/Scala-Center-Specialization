package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }


  test("clusters") {
    val vectors = StackOverflow.sc.parallelize(List(
      (450000, 39),
      (500000, 31),
      (150000,1),
      (150000,10),
      (500000, 55),
      (150000,2),
      (150000,22)))
    
    val means = Array((500000, 13), (150000,10))
    
    val results: Array[(String, Double, Int, Int)] = testObject.clusterResults(means, vectors)
    
    for (r <- results) println(r)
    
    assert(results contains ("Python", 100.0, 4, 6))
    assert(results contains ("Scala", 66.66666666666666, 3, 39))
  
  }
}
