package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = Signal {
//    val (aVal, bVal, cVal) = (a(), b(), c())
//    bVal * bVal - 4 * aVal * cVal
    b() * b() - 4 * a() * c()
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = Signal {
    val (aVal, bVal, dVal) = (a(), b(), delta())
    
    if (dVal < 0) Set()
    else if (dVal == 0) Set(-bVal / (2 * aVal))
    else Set((-bVal + Math.sqrt(dVal)) / (2 * aVal), (-bVal - Math.sqrt(dVal)) / (2 * aVal))
  }
}
