import org.apache.spark.sql.SparkSession



object Mltest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local[*]").enableHiveSupport().getOrCreate()

    val sqlDF=spark.sql("SELECT * FROM edw_user_info_cnt")

    sqlDF.show()
  }

}
