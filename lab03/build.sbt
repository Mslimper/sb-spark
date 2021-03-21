name := "data_mart"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

libraryDependencies += "org.postgresql" % "postgresql" % "42.2.12"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.6.2"