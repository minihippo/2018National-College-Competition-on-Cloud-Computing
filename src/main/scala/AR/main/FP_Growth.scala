package AR.main

import AR.conf.Conf
import AR.util.AssociationRules.RuleNewDef
import AR.util.{FPModel, FPNewDef}
import AR.conf.Conf
import AR.util.AssociationRules.RuleNewDef
//import pasa.nju.ARMining.util.AssociationRules.ConSeq
import FPNewDef.FreqItemset
import FPNewDef.WrapArray

//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
//import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel, FPModel, FPNewDef}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks

object FP_Growth {

  def total(myConf: Conf, conf: SparkConf): Unit = {

    val partitionNum = myConf.numPartitionAB //336
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.memory.fraction", myConf.spark_memory_fraction)
    conf.set("spark.memory.storageFraction", myConf.spark_memory_storage_Fraction)
    conf.set("spark.shuffle.spill.compress", myConf.spark_shuffle_spill_compress)
    conf.set("spark.memory.offHeap.enable", myConf.spark_memory_offHeap_enable)
    conf.set("spark.memory.offHeap.size", myConf.spark_memory_offHeap_size)
    conf.set("spark.executor.memory", myConf.spark_executor_memory_AB)
    conf.set("spark.driver.cores", myConf.spark_driver_cores)
    conf.set("spark.driver.memory", myConf.spark_driver_memory)
    conf.set("spark.executor.instances", myConf.spark_executor_instances)
    conf.set("spark.cores.max", myConf.spark_cores_max_AB)
    conf.set("spark.executor.cores", myConf.spark_executor_cores_AB)
    conf.registerKryoClasses(Array(classOf[FreqItemset],
      classOf[RuleNewDef]))
    val sc = new SparkContext(conf)

    val data = sc.textFile(myConf.inputFilePath + "/D.dat", partitionNum)
    val transactions = data.map(s => s.trim.split(' ').map(f => f.toInt))

    val fp = new FPNewDef() //FPGrowth(）
      .setMinSupport(0.092) // 0.092
      .setNumPartitions(partitionNum)
    val fpgrowth = fp.run(transactions)
    fpgrowth.freqItemsets.persist(StorageLevel.MEMORY_AND_DISK_SER)
    genFreSortBy(myConf.outputFilePath + "/Freq", fpgrowth)
//    genRules(myConf.tempFilePath, fpgrowth)
    sc.stop()
  }

  /**
    * Sort frequentItemSet by RDD.SortBy
    * @param outPath  save frequentItemSet file path
    * @param model generated FPGrowthModel
    * test pass...
    */
  def genFreSortBy(outPath: String, model: FPModel) = {
    val freqUnsort = model.freqItemsets//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val freqSort = freqUnsort.map(itemset => s"${itemset.items.mkString(" ")}: ${itemset.freq}").sortBy(f => f)
    println("frequentItemSet count:",freqSort.count())
    //save
    freqSort.saveAsTextFile(outPath)
  }


  /**
    * generate rules sorted and save the result
    * @param outPath   save rule as objectfile file path
    * @param model  generated FPGrowthModel
    * test pass...
    */
  def genRules(outPath: String, model: FPModel) = {
    val assRules = model.generateAssociationRules(0.8)//.filter(rule => rule.antecedent.length < 1196)
    //    println("-------------"+assRules.count())
    //sort
//    val assRulesSort = assRules.map(rule =>
//      new RuleNewDef(rule.antecedent.sorted, rule.consequent, rule.confidence))
    println("Associated Rules count", assRules.count())
    //save
    assRules.saveAsObjectFile(outPath)
  }

  //pass...
//  def freqSaveTextOnly(args: Array[String]): Unit = {
//    assert(args.length >= 2, "Input args is incorrect")
//    val partitionNum = 1176 //504
//    val conf = new SparkConf().setAppName("freqSaveTextOnly")//.setMaster("local")
//    val sc = new SparkContext(conf)
//
//    val data = sc.textFile(args(0), partitionNum)//.zipWithIndex().filter(f => f._2 < 10).map(f => f._1)
//    val transactions = data.map(s => s.trim.split(' '))
//
//    val fp = new FPGrowth()
//      .setMinSupport(0.092)     // 0.092
//      .setNumPartitions(partitionNum)  // 504
//    val fpgrowth = fp.run(transactions)
//    fpgrowth.freqItemsets.saveAsTextFile(args(1))
//  }
//
//  //pass...
//  def freqSaveObjectOnly(args: Array[String]): Unit = {
//    assert(args.length >= 3, "Input args is incorrect")
//    val partitionNum = 504 //504
//    val conf = new SparkConf().setAppName("freqSaveObjectOnly").
//      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//.setMaster("local")
//    conf.registerKryoClasses(Array(classOf[FreqItemset[String]],
//      classOf[RuleNewDef[String]], classOf[ConSeq]))
//    val sc = new SparkContext(conf)
//
//    val data = sc.textFile(args(0), partitionNum)
//    val transactions = data.map(s => s.trim.split(' '))
//
//    val fp = new FPGrowth()
//      .setMinSupport(0.092)     // 0.092
//      .setNumPartitions(partitionNum)  // 504
//    val fpgrowth = fp.run(transactions)
//    saveFreqObject(args(2), fpgrowth)
//  }
//
//  //pass...
//  def freqSortByAndSave(args: Array[String]): Unit = {
//    assert(args.length >= 2, "Input args is incorrect")
//    val partitionNum = 1008 //504
//    val conf = new SparkConf().setAppName("freqSortByAndSave")//.setMaster("local")
//    val sc = new SparkContext(conf)
//
//    val data = sc.textFile(args(0), partitionNum)
//    val transactions = data.map(s => s.trim.split(' '))
//
//    val fp = new FPGrowth()
//      .setMinSupport(0.092)     // 0.092
//      .setNumPartitions(partitionNum)  // 504
//    val fpgrowth = fp.run(transactions)
////    genFreSortBy(args(1), fpgrowth)
//    // save object file
////    saveFreqObject(args(2), fpgrowth)
//  }
//
//  //pass...
//  def freqSortParAndSave(args: Array[String]): Unit = {
//    assert(args.length >= 2, "Input args is incorrect")
//    val partitionNum = 1008//504
//    val conf = new SparkConf().setAppName("freqSortParAndSave")//.setMaster("local")
//    val sc = new SparkContext(conf)
//
//    val data = sc.textFile(args(0), partitionNum)
//    val transactions = data.map(s => s.trim.split(' '))
//
//    val fp = new FPGrowth()
//      .setMinSupport(0.092)     // 0.092
//      .setNumPartitions(partitionNum)  // 504
//    val fpgrowth = fp.run(transactions)
//    genFreSortPar(args(1), fpgrowth, partitionNum)
//    // save objectfile
////    saveFreqObject(args(2), fpgrowth)
//  }
//
//  //pass...
//  def freqSaveAndSortBy(args: Array[String]): Unit = {
//    assert(args.length >= 3, "Input args is incorrect")
//    val partitionNum = 1008 //504
//    val conf = new SparkConf().setAppName("freqSaveAndSortBy")//.setMaster("local")
//    val sc = new SparkContext(conf)
//
//    val data = sc.textFile(args(0), partitionNum)
//    val transactions = data.map(s => s.trim.split(' '))
//
//    val fp = new FPGrowth()
//      .setMinSupport(0.092)     // 0.092
//      .setNumPartitions(partitionNum)  // 504
//    val fpgrowth = fp.run(transactions)
//    saveFreqObject(args(2), fpgrowth)
//    freSortBy(args(2), args(1), sc)
//  }
//
//  //pass...
//  def freqSaveAndSortPar(args: Array[String]): Unit = {
//    assert(args.length >= 3, "Input args is incorrect")
//    val partitionNum = 1008//504
//    val conf = new SparkConf().setAppName("freqSaveAndSortPar")//.setMaster("local")
//    val sc = new SparkContext(conf)
//
//    val data = sc.textFile(args(0), partitionNum)
//    val transactions = data.map(s => s.trim.split(' '))
//
//    val fp = new FPGrowth()
//      .setMinSupport(0.092)     // 0.092
//      .setNumPartitions(partitionNum)  // 504
//    val fpgrowth = fp.run(transactions)
//    saveFreqObject(args(2), fpgrowth)
//    freSortPar(args(2), args(1), sc, partitionNum)
//  }
//
//  def ruleReadModel(args: Array[String]): Unit = {
//    assert(args.length >= 4, "Input args is incorrect")
//    val partitionNum = 504//504
//    val conf = new SparkConf().setAppName("ruleReadModel")//.setMaster("local")
//    val sc = new SparkContext(conf)
//
//    val model = readFreqtoModel(args(2), sc)
////    genRules(args(3), model)
//  }
//
//  def ruleGenDirect(args: Array[String]): Unit = {
//    assert(args.length >= 4, "Input args is incorrect")
//    val partitionNum = 504//504
//    val conf = new SparkConf().setAppName("ruleGenDirect")//.setMaster("local")
//    val sc = new SparkContext(conf)
//
//    val data = sc.textFile(args(0), partitionNum)
//    val transactions = data.map(s => s.trim.split(' '))
//
//    val fp = new FPGrowth()
//      .setMinSupport(0.092)     // 0.092
//      .setNumPartitions(partitionNum)  // 504
//    val fpgrowth = fp.run(transactions)
////    genRules(args(3), fpgrowth)
//  }
//
//  def total(args: Array[String]): Unit = {
//    assert(args.length >= 4, "Input args is incorrect")
//    val partitionNum = 336 //1680
//    val conf = new SparkConf().setAppName("totalFPNew").
//      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//.setMaster("local")
//    conf.registerKryoClasses(Array(classOf[FreqItemset[String]],
//      classOf[RuleNewDef[String]], classOf[ConSeq]))
//    val sc = new SparkContext(conf)
//
//    val data = sc.textFile(args(0), partitionNum)
//    val transactions = data.map(s => s.trim.split(' '))
//
//    val fp = new FPNewDef()//FPGrowth()
//      .setMinSupport(0.092)     // 0.092
//      .setNumPartitions(partitionNum)  // 504
//    val fpgrowth = fp.run(transactions)
//    fpgrowth.freqItemsets.persist(StorageLevel.MEMORY_AND_DISK_SER)
////    fpgrowth.freqItemsets.count()
////    saveFreqObject(args(2), fpgrowth)
//    genFreSortBy(args(1), fpgrowth)
////    genFreSortPar(args(1), fpgrowth, partitionNum)
//    genRules(args(3), fpgrowth)
//  }
//
//
//  def test(args: Array[String]) = {
//    val partitionNum = 10
//    val conf = new SparkConf().setAppName("FPGrowth").setMaster("local")
//    val sc = new SparkContext(conf)
//
//    val data = sc.textFile(args(0), partitionNum)
//    //.zipWithIndex().filter(f => f._2 < 10).map(f => f._1)
//    val transactions = data.map(s => s.trim.split(' '))
//
//    val fp = new FPGrowth()
//      .setMinSupport(0.2) // 0.092
//      .setNumPartitions(partitionNum) // 840
//    val fpgrowth = fp.run(transactions)
//    genFreSortPar(args(3), fpgrowth, partitionNum)
//    saveFreqObject(args(1), fpgrowth)
//    freSortPar(args(1), args(2),sc, partitionNum)
//  }
//
//  /**
//    *
//    * @param outPath
//    * @param model
//    * test pass...
//    */
//  def saveFreqObject(outPath: String, model: FPGrowthModel[String]): Unit = {
//    model.freqItemsets.saveAsObjectFile(outPath)
//  }
//
//  /**
//    *
//    * @param inPath
//    * @param sc
//    * @return
//    * test pass...
//    */
//  def readFreqtoModel(inPath: String, sc: SparkContext): FPGrowthModel[String] = {
//    val data = sc.objectFile[FreqItemset[String]](inPath)
//    val model = new FPGrowthModel[String](data)
//    model
//  }
//
//  /**
//    *
//    * @param inPath
//    * @param outPath
//    * @param sc
//    * test pass...
//    */
//  def freSortBy(inPath: String, outPath: String, sc: SparkContext) = {
//    val freq = sc.objectFile[FreqItemset[String]](inPath)
//    val freqUnsort = freq.map(itemset => (itemset.items(0),
//                                           s"${itemset.items.mkString(" ")}"))
//    val freqSort = freqUnsort.sortBy(f => f._1).map(f => f._2)
//    //save
//    freqSort.saveAsTextFile(outPath)
//  }
//
//  /**
//    * Sort frequentItemSet by RDD.partition
//    * @param outPath    save sorted frequentItemSet file path
//    * @param model generated FPGrowthModel
//    * @param partitionNum
//    * test pass...
//    */
//  def genFreSortPar(outPath: String, model: FPGrowthModel[String], partitionNum: Int) = {
//    val freqUnsort = model.freqItemsets.map(itemset => (itemset.items(0),
//      s"${itemset.items.mkString(" ")}"))
//    val freqSort = freqUnsort.repartitionAndSortWithinPartitions(new RangePartitioner(partitionNum,
//       freqUnsort)).map(f => f._2)
//    //save
//    freqSort.saveAsTextFile(outPath)
//  }
//
//  /**
//    *
//    * @param inPath
//    * @param outPath
//    * @param sc
//    * @param partitionNum
//    * test pass...
//    */
//  def freSortPar(inPath: String, outPath: String, sc: SparkContext, partitionNum: Int) = {
//    val freq = sc.objectFile[FreqItemset[String]](inPath)
//    val freqUnsort = freq.map(itemset => (itemset.items(0),
//      s"${itemset.items.mkString(" ")}"))
//    val freqSort = freqUnsort.repartitionAndSortWithinPartitions(new RangePartitioner(partitionNum,
//      freqUnsort)).map(f => f._2)
//    //save
//    freqSort.saveAsTextFile(outPath)
//  }
//
//  /**
//    * read rules from a objectfile path
//    * @param inPath read rules from file path
//    * @param sc   SparkContext
//    * @tparam Item String default
//    * @return      RDD[RuleNewDef] rules
//    * test pass...
//    */
//  def getRules[Item: ClassTag](inPath: String, sc: SparkContext) = {
//    val rules = sc.objectFile[RuleNewDef[Item]](inPath)
//    rules
//  }
//
//  /**
//    * spark provided save model function
//    * @param outPath save model path
//    * @param model   getted model
//    * @param sc      sparkContext
//    * test pass...
//    */
//  def saveModel(outPath: String, model: FPGrowthModel[FreqItemset[String]], sc: SparkContext): Unit = {
//    assert(outPath == None, "save model path is incorrect")
//    model.save(sc, outPath)
//  }
//
//  /**
//    * spark provided load model function
//    * @param inPath read model path
//    * @param sc     sparkContext
//    * @return       FPGrowthModel[_]
//    * test pass...
//    */
//  def loadModel(inPath: String, sc: SparkContext): FPGrowthModel[_] = {
//    assert(inPath == None, "load model path is incorrect")
//    val fpgrowth = FPGrowthModel.load(sc, inPath)
//    fpgrowth
//  }
//
//  def canRulesLessFilter(canRules: Set[ArrayBuffer[String]],
//                         rulesExist: mutable.HashMap[Array[String], ArrayBuffer[ConSeq]]) = {
//    val ans = new ArrayBuffer[ConSeq]()
//    for (canRule <- canRules) {
//      if (rulesExist.contains(canRule.toArray)) {
//        val loop = new Breaks
//        loop.breakable(
//          for (conSeq <- rulesExist.get(canRule.toArray).get) {
//            if (!canRule.contains(conSeq.consequent)) {
//              ans.append(conSeq)
//              loop.break
//            } // if
//          }   // for
//        )  // loop
//      }    // if
//    }      // for
//    ans
//  }
//
//  def freqAndRules(args: Array[String]) = {
//    assert(args.length >= 3, "Input args is incorrect")
//    val partitionNum = 1 //504
//    val conf = new SparkConf().setAppName("FPSave").setMaster("local")
//    val sc = new SparkContext(conf)
//
//    val data = sc.textFile(args(0), partitionNum)//.zipWithIndex().filter(f => f._2 < 10).map(f => f._1)
//    val transactions = data.map(s => s.trim.split(' '))
//
//    val fp = new FPGrowth()
//      .setMinSupport(0.092)     // 0.092
//      .setNumPartitions(partitionNum)  // 504
//    val fpgrowth = fp.run(transactions)
//    //    genFreSortBy(args(1), fpgrowth)
//    //    genFreSortPar(args(1), fpgrowth, partitionNum)
//    //    fpgrowth.save(sc, args(2))
//    // Unsorted frequentItemSets
//    //    val freqUnSort = fpgrowth.freqItemsets.map(itemset => s"${itemset.items.mkString(" ")}")
//    //    freqUnSort.saveAsTextFile(args(1))
//    //    println("-----------"+freqUnSort.count())
//    // Sorted frequentItemSets
//    //    val freqSort = freqUnSort.sortBy(itemset => itemset.split(" ")(0))
//    //    val freqSortTemp = freqUnSort.map(itemset => (itemset.split(" ")(0), itemset))
//    //    val freqSortWithKey = freqUnSort.map(itemset => (itemset.split(" ")(0),1))
//    //    val freqSort = freqSortTemp.repartitionAndSortWithinPartitions(new RangePartitioner(partitionNum,
//    //      freqSortWithKey)).map(f => f._2)
//    //     storage the frequentItemSets
//    fpgrowth.freqItemsets.saveAsTextFile(args(2))
//    //    freqSort.saveAsTextFile(args(1))
//
//    //    genRules(args(3), fpgrowth)
//    //    val assRules = fpgrowth.generateAssociationRules(0.9).filter(rule => rule.antecedent.length < 1195)
//    //    println("-------------"+assRules.count())
//    //    val assRulesSort = assRules.map(rule =>
//    //      new RuleNewDef(rule.antecedent.sorted, rule.consequent, rule.confidence))
//    //    //save rules
//    ////    val saveRules = assRulesSort.map(rule => s"[${rule.antecedent.mkString(",")}] " +
//    ////      s"=>${rule.consequent},${rule.confidence}")
//    //    assRules.saveAsTextFile(args(3))
//    //    saveRules.saveAsTextFile(args(2))
//    //    assRulesSort
//  }

}




