import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.sql.cassandra._
import org.elasticsearch.spark


object data_mart extends App {

  var spark = SparkSession.builder().appName(name="lab03").getOrCreate()

  var jsonLogs = "hdfs:///labs/laba03/weblogs.json"
  var user = "mikhail_pykhtin"
  var pass = "9mYHPVhT"

  //Cassandrsa -  csndrDf
  spark.conf.set("spark.cassandra.connection.host", "10.0.0.5")
  spark.conf.set("spark.cassandra.connection.port", "9042")
  spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
  spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

  var csndrOpt = Map("table" -> "clients", "keyspace" -> "labdata")

  var csndrDf = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(csndrOpt)
    .load
    .withColumn("age_cat", when(col("age") <= 24, "18-24")
      .when(col("age") <= 34, "25-34")
      .when(col("age") <= 44, "35-44")
      .when(col("age") <= 54, "45-54")
      .otherwise(">=55")
    )
    .drop("age")

  //Elastic - es
  var esOpt = Map(
    "es.nodes" -> "10.0.0.5:9200",
    "es.batch.write.refresh" -> "false",
    "es.nodes.wan.only" -> "true",
    "es.net.http.auth.user" -> "mikhail.pykhtin",
    "es.net.http.auth.pass" -> "9mYHPVhT"
  )

  var esDf = spark.read
    .format("es")
    .options(esOpt)
    .load("visits*")
    .withColumn("category", concat(lit("shop_"), lower(regexp_replace(col("category"), "-|/s", "_"))))

  var esDfPvt = esDf.groupBy(col("uid").alias("uid_es")).pivot(col("category")).count()

  //json
  var jsDf = spark.read.json("hdfs:///labs/laba03/weblogs.json")
    .select(col("uid"), explode(col("visits")("url")).alias("url"))
    .withColumn("domain", regexp_replace(callUDF("parse_url", col("url"), lit("HOST")), "^www.", ""))
    .drop("url")

  var jsDfPvt = jsDf.join(pgDf, Seq("domain"), "inner")
    .groupBy(col("uid").alias("uid_js")).pivot(col("category")).count()

  //PG
  var pgUrl = s"jdbc:postgresql://10.0.0.5:5432/labdata?user=${user}&password=${pass}"

  var pgDf = spark
    .read
    .format("jdbc")
    .option("url", pgUrl)
    .option("driver", "org.postgresql.Driver")
    .option("labdata", "domain_cats")
    .option("query", "select * from public.domain_cats")
    .load()
    .withColumn("category", concat(lit("web_"), lower(regexp_replace(col("category"), "-|/s", "_"))))

  var finalDf = csndrDf.join(broadcast(esDfPvt), csndrDf("uid") === esDfPvt("uid_es"), "left")
    .join(broadcast(jsDfPvt), csndrDf("uid") === jsDfPvt("uid_js"), "left")
    .drop("uid_es", "uid_js")

  //финальный датафрейм
  var pgFinalUrl = s"jdbc:postgresql://10.0.0.5:5432/${user}?user=${user}&password=${pass}"

  finalDf.write
    .format("jdbc")
    .option("url", pgFinalUrl)
    .option("dbtable", "clients")
    .option("user", user)
    .option("password", pass)
    .option("driver", "org.postgresql.Driver")
    .option("truncate", "true")
    .mode("overwrite")
    .save()
  spark.close()
}
