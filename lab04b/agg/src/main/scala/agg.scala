import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object agg {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName(name = "Pykhtin.Lab04b")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()


    val checkpointDirectory = "hdfs:///user/mikhail.pykhtin/streamingCheckpoints"
    val viewFS = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val viewPath = new Path(checkpointDirectory)
    if (viewFS.exists(viewPath))
      viewFS.delete(viewPath, true)
    println("commit log has been cleaned")

    val schema = StructType(
      List(
        StructField("event_type", StringType, nullable = true),
        StructField("category", StringType, nullable = true),
        StructField("item_id", StringType, nullable = true),
        StructField("item_price", IntegerType, nullable = true),
        StructField("uid", StringType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      )
    )

    val kafka_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", "mikhail_pykhtin")
      .option("startingOffsets", """earliest""")
      //.option("maxOffsetsPerTrigger", "5")
      .option("checkpointLocation", "lab4b/checkpoint_in")
      .load

    val parsed_df = kafka_df.select(col("value").cast(StringType).as("json"))
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")
      .withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))


    val stream_data = parsed_df
      .withWatermark("timestamp", "2 hours")
      .groupBy(window(col("timestamp"), "1 hours", "1 hours").as("window_tf"))
      .agg(
        sum(when(col("event_type") === lit("buy"), col("item_price"))).as("revenue"),
        count(when(col("uid").isNotNull, col("uid"))).as("visitors"),
        count(when(col("event_type") === lit("buy"), col("event_type"))).as("purchases"),
        avg(when(col("event_type") === lit("buy"), col("item_price"))).as("aov")
      )
      .select(col("window_tf").getField("start").cast("long").as("start_ts"),
        col("window_tf").getField("end").cast("long").as("end_ts"),
        col("revenue"), col("visitors"),
        col("purchases"), col("aov"))
      .toJSON

    val stream_kafka = stream_data
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("topic", "mikhail_pykhtin_lab04b_out")
      .option("checkpointLocation", "hdfs:///user/mikhail.pykhtin/streamingCheckpoints")
      .outputMode("update")
      .start()
      .awaitTermination()


    spark.stop()
  }
}
