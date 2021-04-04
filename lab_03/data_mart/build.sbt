name := "data_mart"

version := "1.0"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3"

// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-20
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.11.2"

// https://mvnrepository.com/artifact/org.postgresql/postgresql
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.19"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"