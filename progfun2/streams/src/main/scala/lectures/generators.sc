import scala.util.Random

trait Generator[+T] {
    self => // an alias for "this"
    
    def generate: T
    
    def map[S](f: T => S): Generator[S] = new Generator[S] {
        def generate = f(self.generate)
    }
    
    def flatMap[S](f: T => Generator[S]): Generator[S] = new Generator[S] {
        def generate = f(self.generate).generate
    }
    
    def filter(p: T => Boolean): Generator[T] = new Generator[T] {
        def generate = {
            var r = self.generate
            while (!p(r)) {
                r = self.generate
            }
            r
        }
    }
}

val integers = new Generator[Int] {
    val g = new Random()
    def generate = g.nextInt()
}

val booleans = for (x <- integers) yield x > 0

def pairs[T, U](t: Generator[T], u: Generator[U]) = t flatMap {
    x => u map {y => (x,y)}
}

def single[T](x: T): Generator[T] = new Generator[T] {
    def generate = x
}

def choose(low: Int, high: Int): Generator[Int] = {
    assert(low < high)
    for (x <- integers if low <= x && x < high) yield x
}

def oneOf[T](xs: T*): Generator[T] = {
    for (i <- choose(0, xs.length)) yield xs(i)
}


def lists: Generator[List[Int]] = for {
    isEmpty <- booleans
    list <- if (isEmpty) emptyLists else nonEmptyLists
} yield list

def emptyLists = single(Nil)

def nonEmptyLists = for {
    h <- integers
    tail <- lists
} yield h :: tail


trait Tree
case class Inner(left: Tree, right: Tree) extends Tree {
    override def toString = "{" + left.toString + " } {" + right.toString + "}"
}
case class Leaf(x: Int) extends Tree {
    override def toString = x.toString
}

def trees: Generator[Tree] = for {
    isLeaf <- booleans
    tree <- if (isLeaf) leafNode else innerNode
} yield tree

def leafNode = for {x <- integers} yield Leaf(x)

def innerNode = for {left <- trees; right <- trees} yield Inner(left, right)