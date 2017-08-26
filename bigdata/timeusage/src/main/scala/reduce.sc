import scala.collection.GenIterable

val a = (0 until 10).toList

trait Combiner[A] {
  def combine(x: A, y: A): A
}

implicit val StringCombiner = new Combiner[String] {
  override def combine(x: String, y: String) = s"($x$y)"
}

implicit val IntCombiner = new Combiner[Int] {
  override def combine(x: Int, y: Int) = x + y
}


def binaryReduce[A: Combiner](xs: List[A]): A = {
  def reducePairs[A](op: (A, A) => A)(xs: List[A]): List[A] = xs match {
    case x1 :: x2 :: rest => op(x1, x2) :: reducePairs(op)(rest)
    case _ => xs
  }
  
  xs match {
    case Nil => throw new NoSuchElementException
    case x :: Nil => x
    case _ => binaryReduce(reducePairs(implicitly[Combiner[A]].combine)(xs))
  }
}

binaryReduce(a)

binaryReduce(a.map(_.toString))

binaryReduce(a.map(_.toString))(new Combiner[String] {
  override def combine(x: String, y: String) = x + " " + y
})

implicit def toString[Int](x: Int): String = "Int(" + x.toString + ")"



//binaryReduce(a.map(_.toDouble))