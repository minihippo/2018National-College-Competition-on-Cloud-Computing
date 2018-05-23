package AR.conf

/**
  * System config
  * example:
  */
case class Conf() {

  var inputFilePath: String = ""
  var outputFilePath: String = ""
  var tempFilePath: String = ""

  var appName: String = "Test"
  //  var appName: String = "FP_Growth"

  var numPartitionAB: Int = 336  //336
  //336 // partition number used in AB stage
  var numPartitionCD: Int = 168 //168 //partition number used in CD stage

  //spark configuration
  var spark_memory_fraction = "0.7"
  var spark_memory_storage_Fraction = "0.3"
  var spark_shuffle_spill_compress = "true"
  var spark_memory_offHeap_enable = "true"
  var spark_memory_offHeap_size = "5g"

  var spark_executor_cores_AB = "2"
  var spark_cores_max_AB = "42"

  var spark_executor_cores_CD = "8"
  var spark_cores_max_CD = "168"
  var spark_parallelism_CD = "672"

  var spark_executor_instances = "21"
  var spark_driver_cores = "24"
  var spark_driver_memory = "20g"
  var spark_executor_memory_AB = "20g"
  var spark_executor_memory_CD = "20g"


  override def toString() = {
    "Config:\r\n\t" +
      "inputFilePath: " + this.inputFilePath + "\r\n\t" +
      "outputFilePath: " + this.outputFilePath + "\r\n\t" +
      "appName: " + this.appName + "\r\n\t" +
      "numPartitionAB: " + this.numPartitionAB + "\r\n\t" +
      "numPartitionCD: " + this.numPartitionCD + "\r\n\t" +
      "spark_memory_fraction: " + this.spark_memory_fraction + "\r\n\t" +
      "spark_memory_storage_Fraction: " + this.spark_memory_storage_Fraction + "\r\n\t" +
      "spark_shuffle_spill_compress: " + this.spark_shuffle_spill_compress + "\r\n\t" +
      "spark_memory_offHeap_enable: " + this.spark_memory_offHeap_enable + "\r\n\t" +
      "spark_memory_offHeap_size: " + this.spark_memory_offHeap_size + "\r\n\t" +
      "spark_executor_cores_AB: " + this.spark_executor_cores_AB + "\r\n\t" +
      "spark_cores_max_AB: " + this.spark_cores_max_AB + "\r\n\t" +
      "spark_executor_cores_CD: " + this.spark_executor_cores_CD + "\r\n\t" +
      "spark_cores_max_CD: " + this.spark_cores_max_CD + "\r\n\t" +
      "spark_executor_instances: " + this.spark_executor_instances + "\r\n\t" +
      "spark_driver_cores: " + this.spark_driver_cores + "\r\n\t" +
      "spark_driver_memory: " + this.spark_driver_memory + "\r\n\t" +
      "spark_executor_memory_AB: " + this.spark_executor_memory_AB+ "\r\n\t" +
      "spark_executor_memory_CD: " + this.spark_executor_memory_CD+ "\r\n\t" +
      "spark_default_parallelism " + this.spark_parallelism_CD
  }
}

object Conf {

  def getConf(param: Array[String]): Conf = {
    val conf = new Conf()
    var flag = 0
    (0 to param.length / 2 - 1).foreach { i =>
      param(2 * i) match {
        case "--inputFilePath" => {
          conf.inputFilePath = param(2 * i + 1)
          flag += 1
        }
        case "--outputFilePath" => {
          conf.outputFilePath = param(2 * i + 1)
          flag += 1
        }
        case "--appName" => conf.appName = param(2 * i + 1)
        case "--numPartitionAB" => conf.numPartitionAB = param(2 * i + 1).toInt
        case "--numPartitionCD" => conf.numPartitionCD = param(2 * i + 1).toInt
        case "--spark_memory_fraction" => conf.spark_memory_fraction = param(2 * i + 1)
        case "--spark_memory_storage_Fraction" => conf.spark_memory_storage_Fraction = param(2 * i + 1)
        case "--spark_shuffle_spill_compress" => conf.spark_shuffle_spill_compress = param(2 * i + 1)
        case "--spark_memory_offHeap_enable" => conf.spark_memory_offHeap_enable = param(2 * i + 1)
        case "--spark_memory_offHeap_size" => conf.spark_memory_offHeap_size = param(2 * i + 1)
        case "--spark_executor_cores_AB" => conf.spark_executor_cores_AB = param(2 * i + 1)
        case "--spark_cores_max_AB" => conf.spark_cores_max_AB = param(2 * i + 1)
        case "--spark_executor_cores_CD" => conf.spark_executor_cores_CD = param(2 * i + 1)
        case "--spark_cores_max_CD" => conf.spark_cores_max_CD = param(2 * i + 1)
        case "--spark_executor_instances" => conf.spark_executor_instances = param(2 * i + 1)
        case "--spark_driver_cores" => conf.spark_driver_cores = param(2 * i + 1)
        case "--spark_driver_memory" => conf.spark_driver_memory = param(2 * i + 1)
        case "--spark_executor_memory_AB" => conf.spark_executor_memory_AB = param(2 * i + 1)
        case "--spark_executor_memory_CD" => conf.spark_executor_memory_CD = param(2 * i + 1)
        case "--spark_default_parallelism" => conf.spark_parallelism_CD = param(2 * i + 1)
        case _ => {
          println(s"No Such Conf:${param(2 * i)}")
          assert(false, "No Such Conf")
        }
      }
    }
    assert(flag - 2 == 0, "Maybe you forget to set the inputFilePath or outputFilePath")
    conf
  }

  def getConfWithoutInputAndOutput(param: Array[String]): Conf = {
    val conf = new Conf
    (0 to param.length / 2 - 1).foreach { i =>
      param(2 * i) match {
        case "--appName" => conf.appName = param(2 * i + 1)
        case "--numPartitionAB" => conf.numPartitionAB = param(2 * i + 1).toInt
        case "--numPartitionCD" => conf.numPartitionCD = param(2 * i + 1).toInt
        case "--spark_memory_fraction" => conf.spark_memory_fraction = param(2 * i + 1)
        case "--spark_memory_storage_Fraction" => conf.spark_memory_storage_Fraction = param(2 * i + 1)
        case "--spark_shuffle_spill_compress" => conf.spark_shuffle_spill_compress = param(2 * i + 1)
        case "--spark_memory_offHeap_enable" => conf.spark_memory_offHeap_enable = param(2 * i + 1)
        case "--spark_memory_offHeap_size" => conf.spark_memory_offHeap_size = param(2 * i + 1)
        case "--spark_executor_cores_AB" => conf.spark_executor_cores_AB = param(2 * i + 1)
        case "--spark_cores_max_AB" => conf.spark_cores_max_AB = param(2 * i + 1)
        case "--spark_executor_cores_CD" => conf.spark_executor_cores_CD = param(2 * i + 1)
        case "--spark_cores_max_CD" => conf.spark_cores_max_CD = param(2 * i + 1)
        case "--spark_executor_instances" => conf.spark_executor_instances = param(2 * i + 1)
        case "--spark_driver_cores" => conf.spark_driver_cores = param(2 * i + 1)
        case "--spark_driver_memory" => conf.spark_driver_memory = param(2 * i + 1)
        case "--spark_executor_memory_AB" => conf.spark_executor_memory_AB = param(2 * i + 1)
        case "--spark_executor_memory_CD" => conf.spark_executor_memory_CD = param(2 * i + 1)
        case "--spark_default_parallelism" => conf.spark_parallelism_CD = param(2 * i + 1)
        case _ => {
          println(s"No Such Conf:${param(2 * i)}")
          assert(false, "No Such Conf")
        }
      }
    }
    conf
  }
}
