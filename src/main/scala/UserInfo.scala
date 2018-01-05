
import org.apache.spark.ml.PipelineModel
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession

import org.apache.spark._



object UserInfo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount").set("spark.driver.allowMultipleContexts","true")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("192.168.64.133", 9999)
    val words = lines.flatMap(_.split(" "))

    words.foreachRDD { rdd =>

      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val wordsDataFrame = rdd.toDF("word")

      // Create a temporary view
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on DataFrame using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word as text, count(*) as id from words group by word")
     // wordCountsDataFrame.show()


/*      val schema = StructType(
        Seq(
          StructField("id", IntegerType, true)
          , StructField("text", StringType, true)
        )
      )*/


      val modelPath = "D:\\sparkMl"
      val model = PipelineModel.load(modelPath)

      val prediction = model.transform(wordCountsDataFrame)

      val selected = prediction.select("id", "text", "probability", "prediction")
      selected.collect().foreach(
        x => print(x.getDouble(3))
      )

    }


    ssc.start()             // Start the computation
    ssc.awaitTermination()
    

  }




}
