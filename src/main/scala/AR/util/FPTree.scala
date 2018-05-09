package AR.util

import javafx.scene.Parent

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

  def combine(prefix: List[T], suffix: List[T]): List[List[T]] = {
    var combination = List(suffix)
    prefix.foreach(item => {
      combination = combination ::: combination.map(list => item :: list)
    })
    combination
  }

  def project(reNode: ReNode[T]): List[T] = {
    var curr = reNode
    val path: ListBuffer[T] = ListBuffer.empty
    while (!curr.isRoot) {
      path += curr.item
      curr = curr.parent
    }
    path.toList
  }

  def extract(minCount: Long, suffix: List[T], count: Long,
              parents: mutable.Map[T, ListBuffer[(ReNode[T], Long)]]): ListBuffer[(List[T], Long)] = {
    val partFreqSet = ListBuffer.empty[(List[T], Long)]
    val deepNodeID = parents.keys.max
    val deepNodes = parents(deepNodeID)

    if (parents.keys.size == 1 && deepNodes.map(_._2).sum >= minCount) {
      partFreqSet ++= extractOnePath(minCount, suffix, count, deepNodes.head._1)
      return partFreqSet
    }

    val nSuffix = deepNodeID::suffix
    var nSuffix_count = 0
    var suffix_count = count
    val deepNodes_parents = mutable.Map.empty[T, ListBuffer[(ReNode[T], Long)]]
    parents -= deepNodeID
    deepNodes.foreach { case (node, count) =>
        if (!node.parent.isRoot) {
          val deep_nodes = deepNodes_parents.getOrElseUpdate(node.parent.item, ListBuffer.empty[(ReNode[T], Long)])
          deep_nodes.append((node.parent, count))
          val nodes = parents.getOrElseUpdate(node.parent.item, ListBuffer.empty[(ReNode[T], Long)])
          nodes.append((node.parent, count))
          nSuffix_count += count
        } else {
          suffix_count -= count
        }
    }
    if (nSuffix_count >= minCount && nSuffix_count != 0) {
      partFreqSet.append((nSuffix, nSuffix_count))
      partFreqSet ++= extract(minCount, nSuffix, nSuffix_count, deepNodes_parents)
    }

    if (suffix_count >= minCount && suffix_count != 0) {
      partFreqSet ++= extract(minCount, suffix, suffix_count, parents)
    }
    partFreqSet
  }

  def extractOnePath(minCount: Long, suffix: List[T],
                     count: Long, parent: ReNode[T]): ListBuffer[(List[T], Long)] = {
    val partFreqSet = ListBuffer.empty[(List[T], Long)]
    val prefix = project(parent)
    if (prefix.size != 0) {
      combine(prefix, suffix).map(list => partFreqSet.append((list, count)))
    } else {
      partFreqSet.append((suffix, count))
    }
    partFreqSet
  }

  def traverse(minCount: Long): Iterator[(List[T], Long)] = {
    summaries.iterator.flatMap { case (item, summary) =>
      if (validateSuffix(item)) {
        val freqItemset = ListBuffer.empty[(List[T], Long)]
        if (summary.parents.size == 1 && summary.count >= minCount) {
          freqItemset ++= extractOnePath(minCount, List(item), summary.count, summary.parents.head._1)
        } else if (summary.parents.size > 1 && summary.count >= minCount) {
          freqItemset.append((List(item), summary.count))
          val parents = mutable.Map.empty[T, ListBuffer[(ReNode[T], Long)]]
          var suffix_count = summary.count
          summary.parents.foreach { case (node, count) =>
            if (!node.isRoot) {
              val nodes = parents.getOrElseUpdate(node.item, ListBuffer.empty[(ReNode[T], Long)])
              nodes.append((node, count))
            } else {
              suffix_count -= count
            }
          }

          if (suffix_count >= minCount) {
            freqItemset ++= extract(minCount, List(item), suffix_count, parents)
          }

        }
        freqItemset.toIterator
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