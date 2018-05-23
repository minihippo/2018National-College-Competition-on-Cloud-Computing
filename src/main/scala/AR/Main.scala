package AR

import org.apache.spark.SparkConf
import AR.conf.Conf
import AR.main.FP_Growth
import AR.main.RecPartUserRdd

object Main {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    assert(args.length >= 2, "Usage: JavaFPGrowthExample <input-file> <output-file> <spark.cores.max (optional)> <spark.executor.cores (optional)>")
    val otherArgs = for(i <-0 until args.length if i >= 2) yield args(i)
    val myConf = Conf.getConfWithoutInputAndOutput(otherArgs.toArray)
    println("args:" + myConf.toString())

    val conf = new SparkConf().setAppName(myConf.appName)//.setMaster("local")
    myConf.inputFilePath = args(0)
    myConf.outputFilePath = args(1)
//    myConf.tempFilePath = args(2)
    FP_Growth.total(myConf, conf)
    val end = System.currentTimeMillis()
    println("total time: " + (end - start) / 1000 + "s")
//
//    val conf = new SparkConf().setAppName(myConf.appName)//.setMaster("local")
//    RecPartUserRdd.run(myConf, conf)
//    println("total time: " + (System.currentTimeMillis() - end) / 1000 + "s")

  }

}

