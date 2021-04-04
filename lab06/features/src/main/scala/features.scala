import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.linalg.{Vector => MLVector}

import org.apache.spark.sql.functions._

object features {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(name = "Pykhtin.Lab06").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val webLogs = spark.read.json("/labs/laba03/weblogs.json")

    val explodedWebLogs = webLogs.select(col("uid"), explode(col("visits")).as("visits"))
    val colTimeStamp = (((col("visits").getItem("timestamp")/1000)-10800).cast("timestamp").as("datetime"))
    val colUrl = (callUDF("parse_url", col("visits").getItem("url"), lit("HOST")).as("url"))
    val colStampToWeekDay = (date_format(colTimeStamp, "E").as("WeekDay"))
    val colStampToHour = (date_format(colTimeStamp, "H").as("HourDay"))
    val webVisits = explodedWebLogs.select(col("uid"), colUrl, colStampToWeekDay, colStampToHour)
      .withColumn("domain", regexp_replace(col("url"), "www.", ""))
    val webVisitsList = webVisits.groupBy("uid").agg(collect_list("domain").as("domain"))

    val colStrHourDay = (concat(lit("web_hour_"), col("HourDay").cast("String")).as("HourStrDay"))
    val colStrWeekDay = (concat(lit("web_day_"), lower(col("WeekDay"))).as("WeekStrDay"))

    val vocabTop1000 = webVisits.groupBy("domain").count()
      .filter(col("domain").isNotNull)
      .sort(desc("count")).limit(1000)

    val HourBins = udf((hour: Integer) => {
      hour match {
        case hour if 9 to 17 contains hour => "web_fraction_work_hours"
        case hour if 18 to 23 contains hour => "web_fraction_evening_hours"
        case _ => null
      }
    })

    val DayHourBins = webVisits
      .join(vocabTop1000, Seq("domain"), "inner")
      .select(
        col("uid"),
        col("domain"),
        col("HourDay"),
        colStrHourDay,
        colStrWeekDay
      )

    val HourDayPivot = DayHourBins.groupBy("uid")
      .pivot("HourStrDay").count()
    val WeekDayPivot = DayHourBins.groupBy("uid")
      .pivot("WeekStrDay").count()
    val WorkingHourPivot = DayHourBins.groupBy("uid")
      .pivot(HourBins(col("HourDay"))).count()
      .drop("null")

    val vocabList = vocabTop1000.sort("domain")
      .agg(collect_list("domain").as("domain"))

    val vectorizer = new CountVectorizer()
      .setInputCol("domain")
      .setOutputCol("vectorized")

    val toDense = udf((v:MLVector) => v.toDense)
    val domainVectors = vectorizer.fit(vocabList)
      .transform(webVisitsList)
      .select(
        col("uid"),
        toDense(col("vectorized")).as("domain_features")
      )

    val userItems = spark.read.parquet("users-items/20200429")

    val finalMatrix = domainVectors
      .join(HourDayPivot, Seq("uid"), "inner")
      .join(WeekDayPivot, Seq("uid"), "inner")
      .join(WorkingHourPivot, Seq("uid"), "inner")
      .join(userItems, Seq("uid"), "inner")
      .na.fill(0)

    finalMatrix.write.parquet("/user/mikhail.pykhtin/features")

  }
}