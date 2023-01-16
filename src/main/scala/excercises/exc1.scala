package excercises

import org.apache.spark.sql.functions.{col, exp, expr}
import org.apache.spark.sql.{SparkSession, functions}


object exc1 extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("SparkByExample")
    .getOrCreate()

  import spark.implicits._

  val dept = Seq(
    ("50000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")
  dept.show

  dept.createOrReplaceTempView("table")
  spark.sql("""SELECT VALUES, Delimiter, filter(split(VALUES,concat("\\",Delimiter)),x->x!='') as split_value FROM table""").show(false)


}
