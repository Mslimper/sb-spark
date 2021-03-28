name := "filter"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-sql" % "2.4.5" % Provided ,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5" % Provided

)