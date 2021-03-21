import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._


object data_mart {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(name = "Pykhtin.Lab03").getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)


    val normString = udf((colVal: String) => {
      colVal.replaceAll("[.-]", "_").toLowerCase()
    })
    
    //hdfs

    val hdfs_df = spark.read.json("/labs/laba03/weblogs.json")

    //elastic
    val elastic_df = spark.read.format("org.elasticsearch.spark.sql")
      .option("es.nodes", "10.0.1.9")
      .option("es.port", "9200")
      .option("es.batch.write.refresh", "false")
      .option("es.nodes.wan.only", "true")
      .option("es.net.http.auth.user", "mikhail.pykhtin")
      .option("es.net.http.auth.pass", "9mYHPVhT")
      .load("visits*")

    //cassandra

    spark.conf.set("spark.cassandra.connection.host", "10.0.1.9")
    spark.conf.set("spark.cassandra.connection.port", "9042")
    spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
    spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

    val tableOpts = Map("table" -> "clients","keyspace" -> "labdata")

    val AgeCat = udf((age: Integer) => {
      age match {
        case age if 18 to 24 contains age => "18-24"
        case age if 25 to 34 contains age => "25-34"
        case age if 35 to 44 contains age => "35-44"
        case age if 45 to 54 contains age => "45-54"
        case age if age >= 55 => ">=55"
        case _ => "other"
      }
    })

    val cassandra_df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(tableOpts)
      .load()


    val cassandra_df_cat = cassandra_df.withColumn("age_cat", AgeCat(col("age")))
      .select(col("uid"), col("age_cat"), col("gender"))


    //Postgre

    val domain_cats = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.1.9:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "mikhail_pykhtin")
      .option("password", "9mYHPVhT")
      .option("driver", "org.postgresql.Driver")
      .load()

    val domain_cats_norm = broadcast(domain_cats.withColumn("category", concat(lit("web_"), normString(col("category")))))

    val hdfs_df_explode = hdfs_df.select(col("uid"), explode(col("visits")).as("visits"))

    val url_extract = hdfs_df_explode.select(col("uid"), col("visits").getItem("url").as("url"))

    val web_visit = url_extract
      .withColumn("url", regexp_replace(col("url"), "http://http://", "http://"))
      .withColumn("url", regexp_replace(col("url"), "www.", ""))
      .withColumn("url", lower(trim(regexp_replace(col("url"), "\\s+", " "))))
      .withColumn("domain", callUDF("parse_url", col("url"), lit("HOST")))
      .select("uid", "domain")

    val web_visit_join = web_visit.join(domain_cats_norm, Seq("domain"), "inner").select("uid", "category")

    val web_visit_pivot = web_visit_join.groupBy("uid").pivot("category").count()

    val shop_visit = elastic_df.filter(col("uid").isNotNull)
      .withColumn("category", concat(lit("shop_"), normString(col("category"))))
      .select("uid", "category")

    val shop_visit_pivot = shop_visit.groupBy("uid").pivot("category").count()

    val final_lab_3 = cassandra_df_cat.join(web_visit_pivot, Seq("uid"), "left")
      .join(shop_visit_pivot, Seq("uid"), "left").na.fill(0)

    final_lab_3.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.1.9:5432/mikhail_pykhtin")
      .option("dbtable", "clients")
      .option("user", "mikhail_pykhtin")
      .option("password", "9mYHPVhT")
      .option("driver", "org.postgresql.Driver")
      .save()
  }
}
