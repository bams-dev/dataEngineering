name := "evaluation-spark-m2"
version := "1.0"
scalaVersion := "2.12.17"

val sparkVersion = "3.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "org.slf4j"         % "slf4j-simple" % "1.7.36"
)

fork := true
run / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)
