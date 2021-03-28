import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._

object users_items {
  def main(args: Array[String]): Unit = {

    val normString = (colVal: Column) => {
      lower(regexp_replace(trim(colVal), "[^a-zA-Z0-9\\s]", "_"))
    }

    val dfToMatrix = (df: DataFrame, groupColNm: String, pivotColNm: String,  Prefix: String) => {
      df.withColumn(pivotColNm, normString(col(pivotColNm)))
        .withColumn(pivotColNm, concat(lit(Prefix), col(pivotColNm)))
        .groupBy(groupColNm).pivot(pivotColNm).count()
    }

    def expandCols(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit(null).as(x)
      })
    }

    val spark = SparkSession.builder().appName(name = "Khanin.Lab05").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val confOutputDir = spark.conf.get("spark.users_items.output_dir", "/user/mikhail.pykhtin/users-items")
    val confInputDir = spark.conf.get("spark.users_items.input_dir", "/user/mikhail.pykhtin/visits")
    val confUpdate = if (spark.conf.get("spark.users_items.update") == "1") 1 else 0

    println(s"Update param is $confUpdate")
    println(s"confInputDir param is $confInputDir")
    println(s"confOutputDir param is $confOutputDir")

    val views = spark.read.json(confInputDir + "/view/")
    val buys = spark.read.json(confInputDir + "/buy/")

    val uidDates = views.select("uid", "date").union(buys.select("uid", "date"))

    val maxInputDate= uidDates.agg(max("date")).collect()(0)(0).toString

    val users = uidDates.select("uid").distinct

    val buyMatrix = dfToMatrix(buys.withColumn("item_id", normString(col("item_id"))), "uid", "item_id", "buy_")
    val viewsMatrix = dfToMatrix(views.withColumn("item_id", normString(col("item_id"))), "uid", "item_id", "view_")

    val fs = if(confOutputDir.contains("file:/")) FileSystem.getLocal(new Configuration()) else FileSystem.get(new Configuration())
    if (!fs.exists(new Path(confOutputDir)))
      fs.mkdirs(new Path(confOutputDir))
    val status = fs.listStatus(new Path(confOutputDir))

    val input_matrix = users.join(buyMatrix, Seq("uid"), "left").join(viewsMatrix, Seq("uid"), "left").na.fill(0)

    val digitsMatch = "(\\d{8})".r

    val fileList = status
      .map(x=> digitsMatch.findFirstIn(x.getPath.toString).getOrElse(None))
      .filter(_ != None).map(_.toString)

    def maxDate(date1: String, date2: String): String = if (date1 > date2) date1 else date2



    val maxOutDate = fileList.reduceOption(maxDate).getOrElse("0")
    println(s"maxInputDate param is $maxInputDate")
    println(s"MaxOutData param is $maxOutDate")

    if (confUpdate != 1)
    {
      println(s"confUpdate != 0 param is used")
      input_matrix.write.parquet(confOutputDir+"/"+maxInputDate)
      println(s"Matrix has been written")
    }
    else
    {
      println(s"confUpdate == 1 param is used")
      val previousMatrix = spark.read.parquet(confOutputDir+"/"+maxOutDate)
      val newCols = input_matrix.columns.toSet
      val prevCols = previousMatrix.columns.toSet
      val total = newCols ++ prevCols

      val newMatrix = previousMatrix
        .select(expandCols(prevCols, total):_*)
        .union(input_matrix.select(expandCols(newCols, total):_*))
        .na.fill(0)

      val newOutputDir = if (maxOutDate != "0") confOutputDir+"/" + maxDate(maxOutDate, maxInputDate) else confOutputDir+"/"+maxInputDate
      println(s"MaxOutData param is $newOutputDir")
      if (fs.exists(new Path(newOutputDir)))
        fs.delete(new Path(newOutputDir), true)

      newMatrix.write.parquet(newOutputDir)

      println(s"Matrix has been written")
    }
    spark.stop()


  }

}
