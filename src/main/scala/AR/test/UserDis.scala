package AR.test

import org.apache.spark.{SparkConf, SparkContext}

object UserDis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserDistribution").setMaster("local")
    val sc = new SparkContext(conf)
    val userdata = sc.textFile(args(0)).map(s => s.trim.split(" ").length)
    val count = userdata.count()
    val count_10 = userdata.filter(s => s <= 10).count()
//    val count_50 = userdata.filter(s => s <= 50 && s > 10).count()
    val count_100 = userdata.filter(s => s <= 100 && s > 10).count()
    val count_500 = userdata.filter(s => s <= 500 && s > 100).count()
    val count_800 = userdata.filter(s => s <=800 && s > 500).count()
    val count_1000 = userdata.filter(s => s <=1000 && s > 800).count()
    val count_1195 = userdata.filter(s => s <=1195 && s > 1000).count()
    println("count    :", count)
    println("count10  :", count_10)
//    println("count50:", count_50)
    println("count100 :", count_100)
    println("count500 :", count_500)
    println("count800 :", count_800)
    println("count1000:", count_1000)
    println("count1195:", count_1195)
    sc.stop()
  }
}

//(count    :,330244)
//(count10  :,11252)
//(count100 :,181856)
//(count500 :,127076)
//(count800 :,8078)
//(count1000:,1765)
//(count1195:,217)
