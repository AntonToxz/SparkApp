import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split}

object GlueApp extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.access.key", "")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", "")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.endpoint", "")

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val df1 = spark.read.text("")
  val df2 = df1.filter(row => !(row.mkString("").isEmpty && row.length>0))

  val df3 = df2.withColumn("word",explode(split(col("value")," ")))
    .groupBy("word")
    .count()
    .sort(col("count").desc)
    .repartition(1)

  df3.show(10, false)

  df3.write
    .mode("overwrite")
    .csv("")

}
