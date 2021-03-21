name := "data_mart"
version := "0.1"
scalaVersion := "2.11.12"
libraryDependencies ++= Seq(  "org.apache.spark" %% "spark-sql" % "2.4.7" % Provided,  "org.apache.spark" %%  "spark-core" % "2.4.7" % Provided,  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.7.0",  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3",  "org.postgresql" % "postgresql" % "42.2.19")
mainClass := Some("data_mart")
