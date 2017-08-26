package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    
    @tailrec
    def balancedAtIndex(index: Int, count: Int): Boolean = {
      if (index == chars.length) count == 0
      else chars(index) match {
        case '(' => balancedAtIndex(index + 1, count + 1)
        case ')' => if (count == 0) false else balancedAtIndex(index + 1, count - 1)
        case _ => balancedAtIndex(index + 1, count)
      }
    }
    
    balancedAtIndex(0,0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, numClosed: Int, numOpened: Int): (Int, Int) = {
      if (idx >= until) (numClosed, numOpened)
      else chars(idx) match {
        case '(' => traverse(idx + 1, until, numClosed, numOpened + 1)
        case ')' => {
          if (numOpened > 0) traverse(idx + 1, until, numClosed, numOpened - 1)
          else traverse(idx + 1, until, numClosed + 1, numOpened)
        }
        case _ => traverse(idx + 1, until, numClosed, numOpened)
      }
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from < threshold) traverse(from, until, 0, 0)
      else {
        val cutoff = from + (until - from) / 2
        val (leftHalf, rightHalf) = parallel(reduce(from, cutoff), reduce(cutoff, until))
        val mergeDelta = rightHalf._1 - leftHalf._2
        
        if (mergeDelta >= 0) (leftHalf._1 + mergeDelta, rightHalf._2)
        else (leftHalf._1, rightHalf._2 - mergeDelta)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
