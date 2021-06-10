name := "DataETL"

version := "0.1"

scalaVersion := "2.12.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.+"
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "0.8.1"



