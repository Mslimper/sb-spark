name := "data_mart"

version := "1.0"

scalaVersion := "2.11.12"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.7"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.12" % "3.0.0"

libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "7.11.2"

libraryDependencies += "org.postgresql" % "postgresql" % "42.2.19"
