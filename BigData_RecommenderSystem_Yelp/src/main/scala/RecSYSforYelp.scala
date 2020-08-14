import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object RecSYSforYelp {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // arguments input
    val category = args(0)
    val city = args(1)
    val state = args(2)

    // datasets input
    val spark = SparkSession.builder.master("local[*]").appName("Business Recommendation").getOrCreate
    val business = spark.read.option("multiLine", value = false).option("mode", "PERMISSIVE").json("business.json")

    // category filter
    val categories_to_array = business.withColumn("categories", split(business("categories"), ", "))
    val filter_by_category = categories_to_array.select("*").where(array_contains(categories_to_array("categories"), category))

    // city filter
    val filter_by_city = filter_by_category.where(filter_by_category.col("city") === city)
    filter_by_city.show()

    // city filter
    val filter_by_state = filter_by_city.where(filter_by_category.col("state") === state)
  }
}
