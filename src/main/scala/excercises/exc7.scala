package excercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, first}

object exc7 extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("SparkByExample")
    .getOrCreate()

  import spark.implicits._

  val input = spark.read.option("multiline", "true").json("src/main/resources/input/exc7_input.json").toDF()
  input.show(false)
  input.printSchema()

  input.withColumn("open_close_hours", explode(col("hours")))
    .show()

}
