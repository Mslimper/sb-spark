import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import java.net.URL
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.broadcast

import scala.util.{Failure, Success, Try}

object data_mart extends App {
  val spark = SparkSession
    .builder()
    .appName("Laba03")
    .master("yarn")
    .getOrCreate()

  val dfJson:Dataset[Row] = spark.read.json("/labs/laba03/weblogs.json")
  val dfJsonCount = dfJson.count

  val dfHdfs = dfJson.select(col("uid"), explode(col("visits")).alias("visits")).na.drop

  val dfHdfsCount = dfHdfs.count
  dfHdfs.explain
  dfHdfs.printSchema
  dfHdfs.show(5, 200, true)

  val esOptions = Map("es.nodes" -> "10.0.0.5:9200",
    "es.batch.write.refresh" -> "false",
    "es.nodes.wan.only" -> "true",
    "es.net.http.auth.user" -> "mikhail.pykhtin",
    "es.net.http.auth.pass" -> "9mYHPVhT")

  val dfElasticsearch = spark.read
    .format("org.elasticsearch.spark.sql")
    .options(esOptions)
    .load("visits*")
    .na.drop
    .withColumn("category_shop", concat(lit("shop_"),
      regexp_replace(lower(col("category")), "\\s|-", "_")))
    .select(col("uid"),
      col("event_type"),
      col("category_shop"),
      col("item_id"),
      col("item_price"),
      col("timestamp"))

  dfElasticsearch.explain
  dfElasticsearch.printSchema
  dfElasticsearch.show(1, 100, true)

  val dfElasticsearchPivot = dfElasticsearch.groupBy("uid").pivot("category_shop").count
  dfElasticsearchPivot.show(5,200, true)

  spark.conf.set("spark.cassandra.connection.host", "10.0.0.5")
  spark.conf.set("spark.cassandra.connection.port", "9042")
  spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
  spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

  val tableOpts = Map("table" -> "clients",
    "keyspace" -> "labdata")

  val dfCassandra = spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(tableOpts)
    .load
    .na.drop
    .withColumn("age_cat",
      when(col("age") >= 18 && col("age") <= 24,"18-24")
        .when(col("age") >= 25 && col("age") <= 34,"25-34")
        .when(col("age") >= 35 && col("age") <= 44,"35-44")
        .when(col("age") >= 45 && col("age") <= 54,"45-54")
        .when(col("age") >= 55,">=55")
        .otherwise("Unknown"))
    .filter(col("age_cat")!=="Unknown")
    .select(col("uid"), col("gender"), col("age_cat"))

  dfCassandra.explain
  dfCassandra.printSchema
  dfCassandra.show(5)

  val jdbcUrl = "jdbc:postgresql://10.0.0.5:5432/labdata?user=mikhail_pykhtin&password=9mYHPVhT"
  val dfPostgresql = spark
    .read
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("dbtable", "domain_cats")
    .option("driver", "org.postgresql.Driver")
    .load
    .na.drop
    .withColumn("category_web", concat(lit("web_"),
      regexp_replace(lower(col("category")), "\\s|-", "_")))
    .select(col("domain"), col("category_web"))

  dfPostgresql.explain
  dfPostgresql.printSchema
  dfPostgresql.show(5)

  def decodeFunc = udf(
    (url_test:String) => {
      def getDomaiName(url:String) = Try {
        new URL(java.net.URLDecoder.decode(url, "UTF-8")).getHost
      }  match {
        case Success(valid) => {
          if (url.toString.startsWith("http")) {
            valid.replaceAll("""^www(.*?)\.""","")
          }
          else {"bad"}
        }
        case Failure(e) => {
          if (url.toString.startsWith("http")) {
            url.replaceAll("""^(?:http?:\/\/)""","").split("/")(0)
          }
          else {"bad"}
        }
      }
      getDomaiName(url_test)
    }
  )

  val result = dfHdfs.join(broadcast(dfPostgresql), dfPostgresql("domain") === decodeFunc(dfHdfs("visits.url")), "inner")
    .filter(decodeFunc(dfHdfs("visits.url"))!=="bad")
    .select(col("uid"), col("domain"), col("category_web"))
  result.explain
  result.printSchema
  result.show(5,100,true)

  val resultPivotCategoryWeb = result.groupBy("uid").pivot("category_web").count
  resultPivotCategoryWeb.show(5,200,true)

  val dfHdfsDistinct = dfHdfs.select(col("uid")).distinct

  val resultAll = dfHdfsDistinct
    .join(broadcast(dfCassandra), Seq("uid"), "left")
    .join(broadcast(dfElasticsearchPivot), Seq("uid"), "left")
    .join(broadcast(resultPivotCategoryWeb), Seq("uid"), "left")
  resultAll.explain
  resultAll.printSchema
  resultAll.show(5, 200, true)

  resultAll.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.5:5432/mikhail_pykhtin?user=mikhail_pykhtin&password=9mYHPVhT")
    .option("dbtable", "clients")
    .option("driver", "org.postgresql.Driver")
    .option("truncate", true)
    .mode("overwrite")
    .save

  spark.close()
}