package AR.test

import org.apache.spark.{SparkConf, SparkContext}

object searchCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserDistribution").setMaster("local")
    val sc = new SparkContext(conf)
    val userdata = sc.textFile("./data/D_100.dat").map(s => s.trim.split(" "))
    val count = userdata.filter(s => s.contains("493")&& !s.contains("49")&& s.contains("122")&& s.contains("8")&& s.contains("121"))
   println(count.count())
    sc.stop()
  }
}
