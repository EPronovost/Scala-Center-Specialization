package recfun

import scala.annotation.tailrec

object Main {
  def main(args: Array[String]) {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(pascal(col, row) + " ")
      println()
    }
  }

  /**
   * Exercise 1
   */
    def pascal(c: Int, r: Int): Int = c match {
      case 0 | `r` => 1
      case x if 0 < x && x < r => pascal(c-1, r-1) + pascal(c, r-1)
      case _ => 0
    }
  
  /**
   * Exercise 2
   */
    def balance(chars: List[Char]): Boolean = {
        @tailrec
        def track_balance(chars: List[Char], openCount: Int): Boolean = chars match {
            case List() => openCount == 0
            case '(' :: tail => track_balance(tail, openCount + 1)
            case ')' :: tail => openCount >= 1 && track_balance(tail, openCount - 1)
            case _ :: tail => track_balance(tail, openCount)
        }
        track_balance(chars, 0)
    }
  
  /**
   * Exercise 3
   */
    def countChange(money: Int, coins: List[Int]): Int = {
        if (coins.isEmpty)
            0
        else countChange(money, coins.tail) + {
            if (coins.head > money)
                0
            else if (coins.head == money)
                1
            else
                countChange(money - coins.head, coins)
        }
    }
  }
