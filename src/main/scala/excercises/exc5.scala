package excercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, explode, slice}

object exc5 extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("SparkByExample")
    .getOrCreate()

  import spark.implicits._

  case class MovieRatings(movieName: String, rating: Double)
  case class MovieCritics(name: String, movieRatings: Seq[MovieRatings])
  val movies_critics = Seq(
    MovieCritics("Manuel", Seq(MovieRatings("Logan", 1.5), MovieRatings("Zoolander", 3), MovieRatings("John Wick", 2.5))),
    MovieCritics("John", Seq(MovieRatings("Logan", 2), MovieRatings("Zoolander", 3.5), MovieRatings("John Wick", 3))))
  val ratings = movies_critics.toDF
  ratings.show(truncate = false)

  ratings.withColumn("movie_ratings",explode(col("movieRatings")))
    .withColumn("key",col("movie_ratings").getItem("movieName"))
    .withColumn("value",col("movie_ratings").getItem("rating"))
    .groupBy("name").pivot("key").avg("value")
    .show(false)

}
