package excercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, first}


object exc2 extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("SparkByExample")
    .getOrCreate()

  import spark.implicits._

  val input = Seq(
    (1, "MV1"),
    (1, "MV2"),
    (2, "VPV"),
    (2, "Others")).toDF("id", "value")
  input.show

  input.dropDuplicates("id").show()

}
