class PriorityHeap[T](val maxSize: Int, val comp: (T, T) => Boolean) {
  
  var arr: Array[T] = Array.ofDim(maxSize)
  var size: Int = 0
  
  override def toString: String = arr.mkString("[ ", " ", " ]")
  
  def getParentIndex(index: Int): Int = (index - 1) / 2
  
  def getChildrenIndices(index: Int): (Int, Int) = (2 * index + 1, 2 * index + 2)
  
  def isEmpty: Boolean = arr.isEmpty
  
  def swap(index1: Int, index2: Int): Unit = {
    val tmp = arr(index1)
    arr(index1) = arr(index2)
    arr(index2) = tmp
  }
  
  def promoteElement(index: Int): Unit = {
    if (index == 0) return
    
    val parent = getParentIndex(index)
    if (comp(arr(index), arr(parent))) {
      swap(index, parent)
      promoteElement(parent)
    }
  }
  
  def demoteElement(index: Int): Unit = {
    val (leftChild, rightChild) = getChildrenIndices(index)
    if (rightChild < size) {
      val minChild = if (comp(arr(leftChild), arr(rightChild))) leftChild else rightChild
      if (comp(arr(minChild), arr(index))) {
        swap(minChild, index)
        demoteElement(minChild)
      }
    } else if (rightChild == size) {
      if (comp(arr(leftChild), arr(index))) {
        swap(leftChild, index)
      }
    }
  }
  
  def insert(x: T): Unit = {
    arr(size) = x
    size = size + 1
    promoteElement(size - 1)
  }
  
  def peakTop: T = arr(0)
  
  def pop: T = {
    swap(0, size - 1)
    size = size - 1
    demoteElement(0)
    arr(size)
  }
  
}

class HalvedList[T <: Comparable[T]](val maxSize: Int) {
  val lowerHeap: PriorityHeap[T] = new PriorityHeap(maxSize / 2 + 1, _.compareTo(_) == 1)
  val upperHeap: PriorityHeap[T] = new PriorityHeap(maxSize / 2 + 1, _.compareTo(_) == -1)
  
  def balanceHeaps: Unit = (upperHeap.size - lowerHeap.size) match {
    case x if (x > 1) => {
      lowerHeap.insert(upperHeap.pop)
      balanceHeaps
    }
    case x if (x < -1) => {
      upperHeap.insert(lowerHeap.pop)
      balanceHeaps
    }
    case _ => return
  }
  
  def insert(x: T): Unit = {
    if (!upperHeap.isEmpty && x.compareTo(upperHeap.peakTop) == 1) upperHeap.insert(x) else lowerHeap.insert(x)
    balanceHeaps
  }
  
  override def toString: String = lowerHeap.toString.reverse + " | " + upperHeap.toString
}

class IntHalvedList(val maxSize: Int) extends HalvedList[Int](maxSize) {
  def getMedian: Float = {
    if (lowerHeap.size == upperHeap.size)
      (lowerHeap.peakTop.toFloat + upperHeap.peakTop.toFloat) / 2f
    else if (lowerHeap.size > upperHeap.size)
      lowerHeap.peakTop.toFloat
    else
      upperHeap.peakTop.toFloat
  }
}

val a = new IntHalvedList(10)


for (i <- 1 to 10) {
  a.insert(i)
  println(a.getMedian)
}

