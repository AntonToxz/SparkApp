import org.apache.curator.shaded.com.google.common.hash.Hashing
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{avg, broadcast, col, countDistinct, explode, md5, rank, row_number, sha2, split, sum, udf, when}

import java.nio.charset.StandardCharsets

object test extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("SparkByExample")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val productsTable = spark.read.parquet("./src/main/resources/input/products_parquet")
  val salesTable = spark.read.parquet("./src/main/resources/input/sales_parquet")
  val sellersTable = spark.read.parquet("./src/main/resources/input/sellers_parquet")

  exerciseFour(productsTable, salesTable, sellersTable)


  def exerciseWarmUpOne(productsTable: DataFrame, salesTable: DataFrame, sellersTable: DataFrame) = {
    println("Num of products - " + productsTable.count())
    println("Num of sales - " + salesTable.count())
    println("Num of sellers - " + sellersTable.count())

    salesTable.agg(countDistinct(col("product_id"))).show()

    salesTable
      .filter(col("product_id") =!= 0)
      .groupBy("product_id").count()
      .orderBy(col("count").desc)
      .show(3)
  }

  def exerciseWarmUpTwo(productsTable: DataFrame, salesTable: DataFrame, sellersTable: DataFrame) = {
    salesTable.groupBy(col("date"))
      .agg(countDistinct(col("product_id")))
      .orderBy("date")
      .show(10)

    salesTable
      .dropDuplicates("product_id", "date")
      .groupBy("date")
      .count()
      .orderBy("date")
      .show(10)

    //    val changedSales = salesTable
    //      .filter(col("product_id") =!= 0)
    //      .limit(100)
    //      .orderBy("date", "order_id")
    //      .withColumn("product_id", when(col("product_id") === "19609336", "43439802").otherwise(col("product_id")))
  }

  def exerciseOne(productsTable: DataFrame, salesTable: DataFrame, sellersTable: DataFrame) = {
    salesTable
      .filter(col("product_id") =!= 0)
      .limit(100)
      .join(productsTable, "product_id")
      .agg(avg(col("num_pieces_sold") * col("price")))
      .show()
  }

  def exerciseTwo(productsTable: DataFrame, salesTable: DataFrame, sellersTable: DataFrame) = {
    //BROADCAST!!!!!!!!!!
    salesTable
      .filter(col("product_id") =!= 0)
      .groupBy("date", "seller_id")
      .agg(sum(col("num_pieces_sold")).as("sum_sold"))
      .join(broadcast(sellersTable), "seller_id")
      .groupBy("seller_id")
      .agg(avg(col("sum_sold") / col("daily_target")))
      .show()
  }

  def exerciseThree(productsTable: DataFrame, salesTable: DataFrame, sellersTable: DataFrame) = {
    val window1 = Window.partitionBy("product_id").orderBy(col("num_pieces_sold").desc)
    val window2 = Window.partitionBy("product_id").orderBy(col("num_pieces_sold").asc)

    val filteredSales = salesTable.filter(col("product_id") =!= 0)

    filteredSales
      .withColumn("rank", row_number().over(window1))
      .orderBy("product_id")
      .filter(col("rank") === 2)
      .show(10)

    filteredSales
      .withColumn("rank", row_number().over(window2))
      .orderBy("product_id")
      .filter(col("rank") === 1)
      .show(10)
  }

  def exerciseFour(productsTable: DataFrame, salesTable: DataFrame, sellersTable: DataFrame) = {
    val myUdf = udf(hashEvenIterMd5(_,_))

    salesTable
      .withColumn("hashed_bill", myUdf(col("order_id"), col("bill_raw_text")))
      .show(false)
  }

  def hashEvenIterMd5(id: Int, text: String) = {
    if (id % 2 == 0) {
      val iterations = text.count(_ == 'a')
      var result = text
      for (i <- 1 to iterations) {
        result = Hashing.md5().hashString(text, StandardCharsets.UTF_8).toString
      }
      result
    }
    else {
      Hashing.sha256().hashString(text, StandardCharsets.UTF_8).toString
    }
  }
}



