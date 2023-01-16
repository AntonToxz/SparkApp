package excercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, first}

object exc6 extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("SparkByExample")
    .getOrCreate()

  import spark.implicits._

  val input = Seq(
    ("100","John", Some(35),None),
    ("100","John", None,Some("Georgia")),
    ("101","Mike", Some(25),None),
    ("101","Mike", None,Some("New York")),
    ("103","Mary", Some(22),None),
    ("103","Mary", None,Some("Texas")),
    ("104","Smith", Some(25),None),
    ("105","Jake", None,Some("Florida"))).toDF("id", "name", "age", "city")
   input.show

  input.groupBy("id").agg(first("name",true),first("age",true),first("city",true))
    .show(false)

}
