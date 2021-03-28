import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object filter {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(name = "Pykhtin.Lab04a").getOrCreate()

    val topic_name = spark.conf.get("spark.filter.topic_name")
    val offset = spark.conf.get("spark.filter.offset")
    val output_dir_prefix = spark.conf.get("spark.filter.output_dir_prefix")

    val offsetNum = if (offset == "earliest") "earliest" else s"""{"$topic_name":{"0":$offset}}"""

    val schema = StructType(
      List(
        StructField("event_type", StringType, nullable = true),
        StructField("category", StringType,  nullable = true),
        StructField("item_id", StringType,  nullable = true),
        StructField("item_price", IntegerType,  nullable = true),
        StructField("uid", StringType,  nullable = true),
        StructField("timestamp", LongType,  nullable = true)
      )
    )


    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> topic_name,
      "startingOffsets" -> offsetNum,
      "maxOffsetsPerTrigger" -> "5"
    )

    val raw_kafka = spark.read.format("kafka").options(kafkaParams).load

    val parsedSdf = raw_kafka.select(col("value").cast(StringType).as("json"))
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")
      .withColumn("date", to_date(((col("timestamp")/1000)-10800)
        .cast("timestamp"), "yyyyMMDD").cast("string"))
      .withColumn("date", regexp_replace(col("date"), lit("-"), lit("")))

    val buys = parsedSdf.filter(col("event_type") === "buy")
    val views = parsedSdf.filter(col("event_type") === "view")

    views.write.partitionBy("date").json(output_dir_prefix + "/view")
    buys.write.partitionBy("date").json(output_dir_prefix + "/buy")
    spark.stop()

  }
}