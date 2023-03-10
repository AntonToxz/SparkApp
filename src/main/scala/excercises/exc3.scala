package excercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count}


object exc3 extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("SparkByExample")
    .getOrCreate()

  import spark.implicits._

  val input = Seq(
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
    ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001", 2),
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
    ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001", 2),
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
    ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001", 2),
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
    ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001", 2),
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
    ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001", 2),
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
    ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001", 2),
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
    ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2),
    ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2),
    ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2),
    ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2),
    ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2),
    ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2),
    ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2)).toDF("column0", "column1", "column2", "label")
  input.show

  val window = Window.partitionBy(col("column2"))

  input.withColumn("count",count("column2").over(window)).show()

}
