
import java.util.TimeZone
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQueryListener, Trigger}
import org.apache.spark.sql.{SparkSession, functions => f}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object agg {

  private val batchTimeSeconds = 5
  private val checkpointDirectory = "hdfs:///user/mikhail.pykhtin/streamingCheckpoints"
  private val kafkaBrokers = "spark-master-1:6667"
  private val topicNameIn = "mikhail_pykhtin"
  private val topicNameOut = "mikhail_pykhtin_lab04b_out"

  private val eventScheme = StructType(
    Array(
      StructField("event_type", StringType),
      StructField("category", StringType),
      StructField("item_id", StringType),
      StructField("item_price", LongType),
      StructField("uid", StringType),
      StructField("timestamp", LongType)
    )
  )

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Pykhtin.Lab04b")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val viewFS = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    
    val viewPath = new Path(checkpointDirectory)
    if (viewFS.exists(viewPath))
      viewFS.delete(viewPath, true)
    println("commit log has been cleaned")

    val kafka_df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", topicNameIn)
      .load()


    val agg_df = kafka_df
      .select(f.from_json(f.col("value").cast("string"), eventScheme).as("json"))
      .select("json.*")
      .withColumn("eventTime", f.from_unixtime(f.col("timestamp") / 1000))
      .groupBy(f.window(f.col("eventTime"), "1 hour").as("ts"))
      .agg(
        f.sum(f.when(f.col("uid").isNotNull, 1)).as("visitors"),
        f.sum(f.when(f.col("event_type") === "buy", f.col("item_price"))).as("revenue"),
        f.sum(f.when(f.col("event_type") === "buy", 1)).as("purchases")
      )
      .select(
        f.unix_timestamp(f.col("ts").getField("start")).as("start_ts"),
        f.unix_timestamp(f.col("ts").getField("end")).as("end_ts"),
        f.col("revenue"),
        f.col("visitors"),
        f.col("purchases"),
        (f.col("revenue") / f.col("purchases")).as("aov")
      )
    val kafkaOutput = agg_df
      .select(f.to_json(f.struct(f.col("*"))).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", topicNameOut)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(s"$batchTimeSeconds seconds"))
      .option("checkpointLocation", checkpointDirectory)
      .start()

    println(kafkaOutput.lastProgress)
    println(kafkaOutput.status)

    spark.streams.awaitAnyTermination()

  }
}
