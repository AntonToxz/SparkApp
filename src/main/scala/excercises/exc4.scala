package excercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array, col, collect_list, count, slice}


object exc4 extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("SparkByExample")
    .getOrCreate()

  import spark.implicits._

  val input = spark.range(50).withColumn("key", $"id" % 5)
  input.show

  input.groupBy("key").agg(collect_list("id").as("all"))
    .withColumn("only_first_three",slice(col("all"),1,3))
    .show(false)

}
