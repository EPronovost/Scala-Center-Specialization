abstract class Nat extends Ordered[Nat] {
    def isZero: Boolean
    def predecessor: Nat
    def successor: Nat
    def + (that: Nat): Nat
    def - (that: Nat): Nat
    def * (that: Nat): Nat
    
    def getDots: String
    override def toString = "{" + getDots + "}"
}

object Zero extends Nat {
    def isZero = true
    def predecessor = throw new NoSuchElementException
    def successor = Succ(this)
    def + (that: Nat) = that
    def - (that: Nat) = throw new ArithmeticException
    def * (that: Nat) = Zero
    def compare(that: Nat) = that match {
        case Zero => 0
        case Succ(_) => -1
    }
    def getDots = ""
}
case class Succ(n: Nat) extends Nat {
    def isZero = false
    def predecessor = n
    def successor = Succ(this)
    def + (that: Nat) = Succ(n + that)
    def - (that: Nat) = that match {
        case Zero => this
        case Succ(t) => n - t
    }
    def * (that: Nat) = that + n * that
    def compare(that: Nat) = that match {
        case Zero => 1
        case Succ(t) => n.compare(t)
    }
    def getDots = {
        var d = n.getDots + "."
        if (d endsWith ".......") d = (d stripSuffix ".......") + "*"
        if (d endsWith "*******") d = (d stripSuffix "*******") + "@"
        d
    }
}

var a: Array[Nat] = Array.ofDim(100)
a(0) = Zero

for (i <- 1 until a.length) a(i) = Succ(a(i-1))

a(3) + a(6)

a(80) - a(25)

a(6) * a(7)

a(10) > a(4)

def allSuccessors(n: Nat): Stream[Nat] = Stream.cons(n, allSuccessors(Succ(n)))

val naturals = allSuccessors(Zero)