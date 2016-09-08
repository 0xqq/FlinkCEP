lazy val commonSettings = Seq(
  organization := "com.datareply.FlinkCEP",
  version := "0.1",
  scalaVersion := "2.11.8")


resolvers ++= Seq(
  "clojars.org" at "http://clojars.org/repo",
  "apache snapshots" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.sonatypeRepo("public")
)

val pusher = "com.pusher" % "pusher-java-client" % "1.1.3"
val kafka  = "org.apache.kafka" % "kafka-clients" % "0.9.0.1"
val avro = "org.apache.avro" % "avro" % "1.8.0"
val sprayjson = "io.spray" %%  "spray-json" % "1.3.2"

val flinkKafka = "org.apache.flink" % "flink-connector-kafka-0.9_2.11" % "1.1.1"
val flinkStream = "org.apache.flink" % "flink-streaming-scala_2.11" % "1.1.1"
val flinkCep = "org.apache.flink" % "flink-cep-scala_2.11" % "1.1.1"

val influxClient = "com.paulgoldbaum" % "scala-influxdb-client_2.11" % "0.5.0"

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies += pusher,
    libraryDependencies += kafka,
    libraryDependencies += avro,
    libraryDependencies += sprayjson,
    libraryDependencies += flinkStream,
    libraryDependencies += flinkKafka,
    libraryDependencies += flinkCep,
    libraryDependencies += influxClient
  )
