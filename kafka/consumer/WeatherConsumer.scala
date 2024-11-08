import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WeatherConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WeatherConsumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val weatherDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "weather_data")
      .load()
      .select(from_json(col("value").cast("string"), 
        """
          |{
          |  "city_name": "string",
          |  "temperature": "double",
          |  "humidity": "integer",
          |  "pressure": "integer",
          |  "timestamp": "string"
          |}
        """.stripMargin).alias("data"))
      .select("data.*")

    val query = weatherDF.writeStream
      .foreachBatch { (batchDF, batchId) =>
        batchDF.write
          .format("jdbc")
          .option("url", "jdbc:postgresql://postgres:5432/weather_db")
          .option("dbtable", "weather_data")
          .option("user", "admin")
          .option("password", "admin123")
          .mode("append")
          .save()
      }
      .start()

    query.awaitTermination()
  }
}