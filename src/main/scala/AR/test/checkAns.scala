package AR.test

import org.apache.spark.{SparkConf, SparkContext}

object checkAns {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserDistribution").setMaster("local")
    val sc = new SparkContext(conf)
    val freq = sc.textFile("./data/Freq/*").collect()
    val freqm = sc.textFile("./data/FreqM/*").collect()

    if (freq.length != freqm.length) {
      println("len incorrect")
      return
    }

    for (i <- 0.until(freq.length)) {
      if (freq(i) != freqm(i)) {
        println("incorrect", i)
        return
      }
    }

    sc.stop()
  }
}
