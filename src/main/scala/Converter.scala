import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import org.apache.log4j.{Level, Logger}

object Converter {

  val csvFile = "/home/bob/Documents/thesis/data/Taxi_Trips.csv"

  def main(args: Array[String]) {

    // Set log level to error to avoid abundant info logs
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val codec = "uncompressed"

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("hello spark")
      .getOrCreate()
    import spark.implicits._

    //write it as uncompressed file
    spark.conf.set("spark.sql.parquet.compression.codec", codec)

    var df = spark.read
      .options(Map("header" -> "true"))
      .csv(csvFile)
      .repartition(1) //we want to have everything in 1 file
      .limit(10 * 1000000)

    // Replace illegal whitespaces in column names
    for (i <- df.schema.names) {
      df = df.withColumnRenamed(i, i.replace(" ", "_"))
    }

    df.write
      .parquet("Taxi_Trips_10M.parquet")

    println("Written to parquet file")

    spark.stop()
  }
}
