package AR.util

import java.net.URI
import java.nio.file.Path

import FPNewDef._
import java.{util => ju}

import org.apache.spark.{HashPartitioner, Partitioner, SparkContext, SparkException}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import java.util.Arrays

import AR.conf.Conf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.fs.FileSystem

/**
  * Model trained.
  *
  * @param freqItemsets frequent itemset, which is an RDD of `FreqItemset`
  */
class FPModel(
               val freqItemsets: RDD[FreqItemset]) extends Serializable {
  /**
    * Generates association rules for [[freqItemsets]].
    *
    * @param confidence minimal confidence of the rules produced
    */
  def generateAssociationRules(confidence: Double): RDD[AssociationRules.RuleNewDef] = {
    val associationRules = new AssociationRules(confidence)
    associationRules.run(freqItemsets)
  }
}

/**
  * A parallel FP-growth algorithm to mine frequent itemsets. The algorithm is described in
  * <a href="http://dx.doi.org/10.1145/1454008.1454027">Li et al., PFP: Parallel FP-Growth for Query
  * Recommendation</a>. PFP distributes computation in such a way that each worker executes an
  * independent group of mining tasks. The FP-Growth algorithm is described in
  * <a href="http://dx.doi.org/10.1145/335191.335372">Han et al., Mining frequent patterns without
  * candidate generation</a>.
  *
  * @param minSupport    the minimal support level of the frequent pattern, any pattern that appears
  *                      more than (minSupport * size-of-the-dataset) times will be output
  * @param numPartitions number of partitions used by parallel FP-growth
  * @see <a href="http://en.wikipedia.org/wiki/Association_rule_learning">
  *      Association rule learning (Wikipedia)</a>
  *
  */
class FPNewDef private(
                        private var minSupport: Double,
                        private var numPartitions: Int) extends Serializable {

  /**
    * Constructs a default instance with default parameters {minSupport: `0.3`, numPartitions: same
    * as the input data}.
    *
    */
  def this() = this(0.3, -1)

  /**
    * Sets the minimal support level (default: `0.3`).
    *
    */
  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0.0 && minSupport <= 1.0,
      s"Minimal support level must be in range [0, 1] but got ${minSupport}")
    this.minSupport = minSupport
    this
  }

  /**
    * Sets the number of partitions used by parallel FP-growth (default: same as input data).
    *
    */
  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"Number of partitions must be positive but got ${numPartitions}")
    this.numPartitions = numPartitions
    this
  }

  /**
    * Computes an FP-Growth model that contains frequent itemsets.
    *
    * @param data input data set, each element contains a transaction
    * @return an FPGrowthModel
    *
    */
  def run(data: RDD[Array[Int]]): FPModel = {
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItems = genFreqItems(data, minCount, partitioner)
    val freqItemsets = genFreqItemsets(data, minCount, freqItems, partitioner)
    new FPModel(freqItemsets)
  }

  /**
    * Generates frequent items by filtering the input data using minimal support level.
    *
    * @param minCount    minimum count for frequent itemsets
    * @param partitioner partitioner used to distribute items
    * @return array of frequent pattern ordered by their frequencies
    */
  private def genFreqItems(
                            data: RDD[Array[Int]],
                            minCount: Long,
                            partitioner: Partitioner): Array[Int] = {
    data.flatMap { t =>
      val uniq = t.toSet
      if (t.length != uniq.size) {
        throw new SparkException(s"Items in a transaction must be unique but got ${t.toSeq}.")
      }
      t
    }.map(v => (v, 1L))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()
      .sortBy(-_._2)
      .map(_._1)
  }

  /**
    * Generate frequent itemsets by building FP-Trees, the extraction is done on each partition.
    *
    * @param data        transactions
    * @param minCount    minimum count for frequent itemsets
    * @param freqItems   frequent items
    * @param partitioner partitioner used to distribute transactions
    * @return an RDD of (frequent itemset, count)
    */

  //  private def genFreqItemsets(
  //                                               data: RDD[Array[String]],
  //                                               minCount: Long,
  //                                               freqItems: Array[String],
  //                                               partitioner: Partitioner): RDD[FreqItemset] = {
  //    val itemToRank = freqItems.zipWithIndex.toMap
  //    data.flatMap { transaction =>
  //      genCondTransactions(transaction, itemToRank, partitioner)
  //    }.aggregateByKey(new FPTree[Int], partitioner.numPartitions)(
  //      (tree, transaction) => tree.add(transaction, 1L),
  //      (tree1, tree2) => tree1.merge(tree2))
  //      .flatMap { case (part, tree) =>
  //        tree.extract(minCount, x => partitioner.getPartition(x) == part)
  //      }.map { case (ranks, count) =>
  //      new FreqItemset(ranks.map(i => freqItems(i)).toArray, count)
  //    }
  //  }

  //private def genFreqItemsets(
  //                                             data: RDD[Array[Item]],
  //                                             minCount: Long,
  //                                             freqItems: Array[Item],
  //                                             partitioner: Partitioner): RDD[FreqItemset[Item]] = {
  //  val itemToRank = freqItems.zipWithIndex.toMap
  //  data.flatMap { transaction =>
  //    genCondTransactions(transaction, itemToRank, partitioner)
  //  }.map(itemset => (new WrapArray(itemset), 1L))
  //    .combineByKey(x => x,(x:Long,y:Long) => x+y, (x:Long,y:Long) => x+y, partitioner, true)
  //    .map(pair => (partitioner.getPartition(pair._1.array.last), (pair._1.array, pair._2)))
  //    .aggregateByKey(new FPTree[Int], partitioner.numPartitions)(
  //      (tree, transaction) => tree.add(transaction._1, transaction._2),
  //      (tree1, tree2) => tree1.merge(tree2))
  //    .flatMap { case (part, tree) =>
  //      tree.extract(minCount, x => partitioner.getPartition(x) == part)
  //    }.map { case (ranks, count) =>
  //    new FreqItemset(ranks.map(i => freqItems(i)).toArray, count)
  //  }
  //}
  //
  private def genFreqItemsets(
                               data: RDD[Array[Int]],
                               minCount: Long,
                               freqItems: Array[Int],
                               partitioner: Partitioner): RDD[FreqItemset] = {
    val itemToRank = freqItems.zipWithIndex.toMap
    val temp = data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank, partitioner)
    }.mapPartitions(iter => {
      val pair = mutable.Map.empty[WrapArray, Long]
      while (iter.hasNext) {
        val arr = new WrapArray(iter.next())
        val value = pair.get(arr)
        if (value == None) {
          pair(arr) = 1L
        } else {
          pair(arr) = value.get + 1L
        }
      }
      pair.iterator
      //      Iterator(pair.flatMap(f => f))
    }).map(turple => (partitioner.getPartition(turple._1.array.last), (turple._1.array, turple._2)))
      .repartitionAndSortWithinPartitions(partitioner).mapPartitions(iter => {
      val coArr = new ArrayBuffer[(Int, (Array[Int], Long))]()
      var pair = mutable.Map.empty[WrapArray, Long]
      var pre = partitioner.numPartitions + 1
      while (iter.hasNext) {
        val turple = iter.next()
        if (pre > partitioner.numPartitions) {
          pre = turple._1
        } else if (pre != turple._1) {
          pair.map(t => coArr.append((pre, (t._1.array, t._2))))
          pair = mutable.Map.empty[WrapArray, Long]
          pre = turple._1
        }

        val arr = new WrapArray(turple._2._1)
        val value = pair.get(arr)
        if (value == None) {
          pair(arr) = turple._2._2
        } else {
          pair(arr) = turple._2._2 + value.get
        }
      }
      pair.map(t => coArr.append((pre, (t._1.array, t._2))))
      coArr.iterator
    }).mapPartitions { iter =>
//      println("-------------------")
      if (iter.hasNext) {
        val res = new ArrayBuffer[(Int, FPTree)]()
        var pre = iter.next()
        var fPTree = new FPTree()

//        if (pre._2._1.contains(5)) {
//          println(pre._2._1.toList)
//        }

        fPTree.add(pre._2._1, pre._2._2)
        while (iter.hasNext) {
          val cur = iter.next()
          if (cur._1 == pre._1) { //add cur to fPTree
            fPTree.add(cur._2._1, cur._2._2)
          } else {
            res += ((pre._1, fPTree))
            fPTree = new FPTree()
            pre = cur
            fPTree.add(pre._2._1, pre._2._2)
          }
//
//          if (cur._2._1.contains(5)) {
//            println(cur._2._1.toList)
//          }

        }
        res += ((pre._1, fPTree))
        res.toArray.toIterator
      } else {
        Iterator()
      }
    }
    //already generate fp-tree
    temp.count()
    val gen = temp.flatMap { case (part, tree) =>
      tree.extract(minCount, x => partitioner.getPartition(x) == part)
    }
    //already generate frequentItemSet

//    println("=====")
//    println(freqItems.zipWithIndex.toMap)
//    println("=====")

    gen.count()
    gen.map { case (ranks, count) =>
      new FreqItemset(ranks.map(i => freqItems(i)).toArray, count)
    }
  }

  //    private def genFreqItemsets(
  //                                 data: RDD[Array[Int]],
  //                                 minCount: Long,
  //                                 freqItems: Array[Int],
  //                                 partitioner: Partitioner): RDD[FreqItemset] = {
  //      val itemToRank = freqItems.zipWithIndex.toMap
  //      val temp = data.flatMap { transaction =>
  //        genCondTransactions(transaction, itemToRank, partitioner)
  //      }.map(itemset => (new WrapArray(itemset), 1L))
  //        .combineByKey(x => x,(x:Long,y:Long) => x+y, (x:Long,y:Long) => x+y, partitioner, true)
  //        .map(pair => (partitioner.getPartition(pair._1.array.last), (pair._1.array, pair._2)))
  //        .repartitionAndSortWithinPartitions(partitioner).mapPartitions{iter=>
  //        if(iter.hasNext){
  //          val res = new ArrayBuffer[(Int, FPTree[Int])]()
  //          var pre = iter.next()
  //          var fPTree = new FPTree[Int]()
  //          fPTree.add(pre._2._1, pre._2._2)
  //          while(iter.hasNext){
  //            val cur = iter.next()
  //            if(cur._1 == pre._1){ //add cur to fPTree
  //              fPTree.add(cur._2._1, cur._2._2)
  //            }else{
  //              res += ((pre._1, fPTree))
  //              fPTree = new FPTree[Int]()
  //              pre = cur
  //              fPTree.add(pre._2._1, pre._2._2)
  //            }
  //          }
  //          res += ((pre._1, fPTree))
  //          res.toArray.toIterator
  //        }else{
  //          Iterator()
  //        }
  //      }
  //      //already generate fp-tree
  //      temp.count()
  //      val gen = temp.flatMap { case (part, tree) =>
  //        tree.extract(minCount, x => partitioner.getPartition(x) == part)
  //      }
  //      //already generate frequentItemSet
  //      gen.count()
  //      gen.map { case (ranks, count) =>
  //        new FreqItemset(ranks.map(i => freqItems(i)).toArray, count)
  //      }
  //    }


  /**
    * Generates conditional transactions.
    *
    * @param transaction a transaction
    * @param itemToRank  map from item to their rank
    * @param partitioner partitioner used to distribute transactions
    * @return a map of (target partition, conditional transaction)
    */
  //private def genCondTransactions(
  //                                                 transaction: Array[String],
  //                                                 itemToRank: Map[String, Int],
  //                                                 partitioner: Partitioner): mutable.Map[Int, Array[Int]] = {
  //  val output = mutable.Map.empty[Int, Array[Int]]
  //  // Filter the basket by frequent items pattern and sort their ranks.
  //  val filtered = transaction.flatMap(itemToRank.get)
  //  ju.Arrays.sort(filtered)
  //  val n = filtered.length
  //  var i = n - 1
  //  while (i >= 0) {
  //    val item = filtered(i)
  //    val part = partitioner.getPartition(item)
  //    if (!output.contains(part)) {
  //      output(part) = filtered.slice(0, i + 1)
  //    }
  //    i -= 1
  //  }
  //  output
  //}

  private def genCondTransactions(
                                   transaction: Array[Int],
                                   itemToRank: Map[Int, Int],
                                   partitioner: Partitioner): mutable.ArrayBuffer[Array[Int]] = {
    val output = mutable.ArrayBuffer.empty[Array[Int]]
    val group = mutable.Set.empty[Int]

    // Filter the basket by frequent items pattern and sort their ranks.
    val filtered = transaction.flatMap(itemToRank.get)
    ju.Arrays.sort(filtered)
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)
      if (!group.contains(part)) {
        output.append(filtered.slice(0, i + 1))
        group.add(part)

//        val a = filtered.slice(0, i + 1).toList
//        if (a.contains(5)) {
//          println(a)
//        }

      }
      i -= 1
    }
    output
  }

  //
  //private def genCondTransactions(
  //                                 transaction: Array[String],
  //                                 itemToRank: Map[String, Int],
  //                                 partitioner: Partitioner): mutable.ArrayBuffer[List[Int]] = {
  //  val output = mutable.ArrayBuffer.empty[List[Int]]
  //  val group = mutable.Set.empty[Int]
  //
  //  // Filter the basket by frequent items pattern and sort their ranks.
  //  val filtered = transaction.flatMap(itemToRank.get)
  //  ju.Arrays.sort(filtered)
  //  val n = filtered.length
  //  var i = n - 1
  //  while (i >= 0) {
  //    val item = filtered(i)
  //    val part = partitioner.getPartition(item)
  //    if (!group.contains(part)) {
  //      output.append(filtered.slice(0, i + 1).toList)
  //      group.add(part)
  //    }
  //    i -= 1
  //  }
  //  output
  //}

}

object FPNewDef {

  /**
    * Frequent itemset.
    *
    * @param items items in this itemset. Java users should call `FreqItemset.javaItems` instead.
    * @param freq  frequency
    *
    */
  class FreqItemset(
                     val items: Array[Int],
                     val freq: Long) extends Serializable {

    //    /**
    //      * Returns items in a Java List.
    //      *
    //      */
    //    def javaItems: java.util.List[Item] = {
    //      items.toList.asJava
    //    }

    override def toString: String = {
      s"${items.mkString("{", ",", "}")}: $freq"
    }
  }

  class WrapArray(val array: Array[Int])

  //    extends  Serializable {
  //    override def hashCode(): Int = Arrays.hashCode(this.array)
  //
  //    def canEqual(a: Any) = a.isInstanceOf[WrapArray]
  //
  //    override def equals(obj: scala.Any): Boolean = {
  //      obj match {
  //        case obj: WrapArray => obj.canEqual() && Arrays.equals(obj.array, this.array)
  //        case _ => false
  //      }
  //    }
  //
  //    override def toString: String = super.toString
  //  }
  //

}
