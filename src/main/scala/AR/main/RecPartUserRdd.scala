package AR.main

import AR.conf.Conf
import AR.util.AssociationRules.RuleNewDef
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet
import scala.reflect.ClassTag
import scala.util.control.Breaks

object RecPartUserRdd {
  class RuleToCompare(confidence_i: Double, rec_i: String) extends Comparable[RuleToCompare]{
    var confidence: Double = confidence_i
    var rec: String = rec_i
    override def compareTo(o: RuleToCompare): Int = {
      if (this.confidence == o.confidence) {
        o.rec compare this.rec
      } else {
        this.confidence compare o.confidence
      }
    }
  }

  class MyOrdering[T] extends Ordering[T] {
    def compare(x: T, y: T): Int = math.random compare math.random
  }

  class MyOrdering2[T] extends Ordering[T] {
    def compare(x: T, y: T): Int = {
      (x, y) match {
        case (null, null) => 0
        case (iX: (HashSet[Int], Long), iY: (HashSet[Int], Long)) => iX._1.size compare iY._1.size
      }
    }
  }

  //  class MyOrdering3[T] extends Ordering[T] {
  //    def compare(x: T, y: T): Int = {
  //      (x, y) match {
  //        case (null, null) => 0
  //        case (iX: (Double, String), iY: (Double, String)) => {
  //          if (iX._1 == iY._1) {
  //            iY._2 compare iX._2
  //          } else {
  //            iX._1 compare iY._1
  //          }
  //        }
  //      }
  //    }
  //  }

  def run(myConf: Conf, conf: SparkConf): Unit = {

    val partitionNum = myConf.numPartitionCD
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.memory.fraction", myConf.spark_memory_fraction)
    conf.set("spark.memory.storageFraction", myConf.spark_memory_storage_Fraction)
    conf.set("spark.shuffle.spill.compress", myConf.spark_shuffle_spill_compress)
    conf.set("spark.memory.offHeap.enable", myConf.spark_memory_offHeap_enable)
    conf.set("spark.memory.offHeap.size", myConf.spark_memory_offHeap_size)
    conf.set("spark.executor.memory", myConf.spark_executor_memory_CD)
    conf.set("spark.driver.cores", myConf.spark_driver_cores)
    conf.set("spark.driver.memory", myConf.spark_driver_memory)
    conf.set("spark.executor.instances", myConf.spark_executor_instances)
    conf.set("spark.cores.max", myConf.spark_cores_max_CD)
    conf.set("spark.executor.cores", myConf.spark_executor_cores_CD)
    conf.set("spark.default.parallelism", myConf.spark_parallelism_CD)

    val sc = new SparkContext(conf)
    val clazz = classOf[(Double, String)]
    val ct = ClassTag(clazz.getDeclaringClass)
    val associationRulesRdd = sc.objectFile[RuleNewDef](myConf.tempFilePath)
      .sortBy(rule => new RuleToCompare(rule.confidence, rule.consequent(0).toString), ascending = false, partitionNum)

    // broadcast rules
    val associationRules = sc.broadcast(associationRulesRdd.collect())

    // read user items rdd
    //    val userDataRdd = sc.textFile(myConf.inputFilePath + "/U.dat", 168).map(items =>
    //      items.trim.split(' ').map(s => s.toInt).sorted
    //    ).map(items => HashSet(items: _*)
    //    ).zipWithIndex().repartition(168)(new MyOrdering2[(HashSet[Int], Long)])

    //fragmentation
    val userDataRdd = sc.textFile(myConf.inputFilePath + "/U.dat", partitionNum).map(items =>
      items.trim.split(' ').map(s => s.toInt).sorted
    ).map(items => HashSet(items: _*)
    ).zipWithIndex()
    //      .cache()

    //new def mappartition
    def calFunc(iter: Iterator[(HashSet[Int], Long)]): Iterator[(Long, Int)] = {
      var res = List[(Long, Int)]()
      while (iter.hasNext) {
        val user = iter.next
        var rec_item = 0
        val loop = new Breaks
        loop.breakable {
          for (i <- associationRules.value.indices) {
            var flag = true
            val end = associationRules.value(i).consequent(0)
            //判断规则是否符合条件
            if (user._1.size >= associationRules.value(i).antecedent.length && !user._1.contains(end)) {
              val iterate = associationRules.value(i).antecedent.iterator
              val loop_rule = new Breaks
              loop_rule.breakable {
                while (iterate.hasNext) {
                  if (!user._1.contains(iterate.next())) {
                    flag = false
                    loop_rule.break()
                  }
                }
              }
              if (flag) {
                rec_item = end
                loop.break()
              }
            }
          }
        }
        res.::=(user._2, rec_item)
      }
      res.iterator
    }

    val userRdd_25 = userDataRdd.filter(item => item._1.size <= 25)
      .repartition(partitionNum)(new MyOrdering2[(HashSet[Int], Long)])
      .mapPartitions(calFunc)
      .cache()

    val userRdd_30 = userDataRdd.filter(item => item._1.size > 25 && item._1.size <= 50)
      .repartition(partitionNum)(new MyOrdering2[(HashSet[Int], Long)])
      .mapPartitions(calFunc)
      .cache()

    val userRdd_60 = userDataRdd.filter(item => item._1.size > 50 && item._1.size <= 100)
      .repartition(partitionNum)(new MyOrdering2[(HashSet[Int], Long)])
      .mapPartitions(calFunc)
      .cache()

    val userRdd_100 = userDataRdd.filter(item => item._1.size > 100 && item._1.size <= 500)
      .repartition(partitionNum)(new MyOrdering2[(HashSet[Int], Long)])
      .mapPartitions(calFunc)
      .cache()

    val userRdd_1195 = userDataRdd.filter(item => item._1.size > 500)
      .repartition(partitionNum)(new MyOrdering2[(HashSet[Int], Long)])
      .mapPartitions(calFunc)
      .cache()

    (userRdd_25 ++ userRdd_30 ++ userRdd_60 ++ userRdd_100 ++ userRdd_1195).sortByKey()
      .map(userRec => {
        userRec._2
      })
      .saveAsTextFile(myConf.outputFilePath + "/Rec")


  }
}