package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  lazy val genHeap: Gen[H] = oneOf(const(empty),
    for {
      n <- arbitrary[Int]
      h <- oneOf(const(empty), genHeap)
    } yield insert(n, h)
  )
  
  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("insertTwoElements") = forAll {(a: Int, b: Int) =>
      val h = insert(a, insert(b, empty))
      findMin(h) == (if (a < b) a else b)
  }
  
  property("clearOut") = forAll {(a: Int) =>
      isEmpty(deleteMin(insert(a, empty)))
  }
  
  def listOfMins(h: H): List[A] = isEmpty(h) match {
    case true => List()
    case false => findMin(h) :: listOfMins(deleteMin(h))
  }
  
  def mergeLists(xs: List[A], ys: List[A]): List[A] = (xs, ys) match {
    case (List(), _) => ys
    case (_, List()) => xs
    case (x :: xRest, y :: yRest) =>
      if (x < y) x :: mergeLists(xRest, ys) else y :: mergeLists(xs, yRest)
  }
  
  def testOrdered(l: List[A]): Boolean = l match {
    case x :: y :: rest => (x <= y) && testOrdered(y :: rest)
    case _ => true
  }
  
  property("orderedMins") = forAll { (h: H) =>
    testOrdered(listOfMins(h))
  }
  
  property("orderedMinsOfMeld") = forAll { (h1: H, h2: H) =>
      testOrdered(listOfMins(meld(h1, h2)))
  }
  
  property("heapMelding") = forAll { (h1: H, h2: H) =>
      if (!isEmpty(h1) && !isEmpty(h2)) {
        val (min1, min2) = (findMin(h1), findMin(h2))
        findMin(meld(h1, h2)) == (if (min1 < min2) min1 else min2)
      } else {
        true
      }
  }
  
  property("meldEmtpy") = forAll { (h: H) =>
      if (isEmpty(h)) true
      else findMin(meld(h, empty)) == findMin(h)
  }
  
  property("meldSymmetry") = forAll { (h1: H, h2: H) =>
      listOfMins(meld(h1, h2)) sameElements listOfMins(meld(h2, h1))
  }
  
  property("reinsertMin") = forAll { (a1: A, a2: A, a3: A) =>
      val h = insert(a1, insert(a2, insert(a3, empty)))
      val min = findMin(h)
      val h2 = insert(min, deleteMin(h))
      findMin(h2) == min
  }
  
  property("mergeHasAll") = forAll { (h1: H, h2: H, h3: H) =>
      val merged = meld(meld(h1, h2), h3)
      listOfMins(merged) sameElements
          mergeLists(mergeLists(listOfMins(h1), listOfMins(h2)),
            listOfMins(h3))
  }
  
}
