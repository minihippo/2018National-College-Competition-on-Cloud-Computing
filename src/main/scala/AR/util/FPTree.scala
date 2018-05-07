package AR.util

import AR.util.FPTree.Node

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * FP-Tree data structure used in FP-Growth.
  *
  * @tparam T item type
  */
class FPTree[T] extends Serializable {

  import FPTree._

  val root: Node[T] = new Node(null)

  //  private val summaries: mutable.Map[T, Parents[T]] = mutable.Map.empty

  /** Adds a transaction with count. */
  def add(t: Iterable[T], count: Long = 1L): this.type = {
    require(count > 0)
    var curr = root
    curr.count += count
    t.foreach { item =>
      //      val summary = summaries.getOrElseUpdate(item, new Parents)
      //      summary.count += count
      val child = curr.children.getOrElseUpdate(item, {
        val newNode = new Node(curr)
        newNode.item = item
        //        summary.nodes += (newNode, count)
        newNode
      })
      child.count += count
      curr = child
    }
    this
  }

  //  /** Gets a subtree with the suffix后缀. */
  //  private def project(suffix: T): FPTree[T] = {
  //    val tree = new FPTree[T]
  //    if (summaries.contains(suffix)) {
  //      val summary = summaries(suffix)
  //      summary.nodes.foreach { node =>
  //        var t = List.empty[T]
  //        var curr = node.parent
  //        while (!curr.isRoot) {
  //          t = curr.item :: t
  //          curr = curr.parent
  //        }
  //        tree.add(t, node.count)
  //      }
  //    }
  //    tree
  //  }

  //  /** Extracts all patterns with valid suffix and minimum count. */
  //  def extract(
  //               minCount: Long,
  //               validateSuffix: T => Boolean = _ => true): Iterator[(List[T], Long)] = {
  //    summaries.iterator.flatMap { case (item, summary) =>
  //      if (validateSuffix(item) && summary.count >= minCount) {
  //        Iterator.single((item :: Nil, summary.count)) ++
  //          project(item).extract(minCount).map { case (t, c) =>
  //            (item :: t, c)
  //          }
  //      } else {
  //        Iterator.empty
  //      }
  //    }
  //  }
}

object FPTree {

  /** Representing a node in an FP-Tree. */
  class Node[T](val parent: Node[T]) extends Serializable {
    var item: T = _
    var count: Long = 0L
    val children: mutable.Map[T, Node[T]] = mutable.Map.empty

    def isRoot: Boolean = parent == null
  }

}

class ReFPTree[T](val validateSuffix: T => Boolean = _ => true) extends Serializable {

  import ReFPTree._

  private val summaries: mutable.Map[T, Summary[T]] = mutable.Map.empty

  def generateTree(node: Node[T], parent: ReNode[T] = new ReNode(null)): this.type = {
    node.children.foreach { case (item, node) =>
      val curr = new ReNode(parent)
      curr.item = item
      if (validateSuffix(item)) {
        val summary = summaries.getOrElseUpdate(item, new Summary)
        summary.parents.append((parent, node.count))
        summary.count += node.count
      }
      generateTree(node, curr)
    }
    this
  }

  def combine(prefix: List[T], suffix: T): List[List[T]] = {
    var combination: List[List[T]] = List(List(suffix))
    prefix.foreach(item => {
      combination = combination ::: combination.map(list => item :: list)
    })
    combination
  }

  def project(reNode: ReNode[T]): List[T] = {
    var curr = reNode
    val path: ListBuffer[T] = ListBuffer.empty
    while(!curr.isRoot) {
      path += curr.item
      curr = curr.parent
    }
    path.toList
  }

  def extract(minCount: Long, item: T, summary: Summary[T]):Iterator[(List[T], Long)] = {
    val freqItemset: ListBuffer[(List[T], Long)] = ListBuffer.empty
    if (summary.parents.size == 1 && summary.count >= minCount) {
        val parent = summary.parents.head
        val prefix = project(parent._1)
        if (prefix.size != 0) {
            combine(prefix, item).map(list => freqItemset.append((list, summary.count)))
        } else {
          freqItemset.append((List(item), summary.count))
        }

    } else {

    }
    freqItemset.toIterator
  }

  def traverse(minCount: Long):Iterator[(List[T], Long)] = {
    summaries.iterator.flatMap { case (item, summary) =>
        if (validateSuffix(item)) {
          extract(minCount, item, summary)
        } else {
          Iterator.empty
        }
    }
  }
}

object ReFPTree {

  class ReNode[T](val parent: ReNode[T]) extends Serializable {
    var item: T = _

    def isRoot: Boolean = parent == null
  }

  /** Parents of an item in an FP-Tree. */
  private class Summary[T] extends Serializable {
    var count: Long = 0L
    val parents: ListBuffer[(ReNode[T], Long)] = ListBuffer.empty
  }

}