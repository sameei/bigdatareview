
name := "kfch"
version := "0.1"
scalaVersion := "2.12.8"

fork := true

libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka-clients" % "2.7.0",
    "org.apache.spark" %% "spark-sql" % "3.0.1" % Provided,
    "org.apache.spark" %% "spark-streaming" % "3.0.1" % Provided,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1",
    "org.scalatest" %% "scalatest" % "3.2.3" % Test,
    "org.javatuples" % "javatuples" % "1.2",
    "com.github.scopt" %% "scopt" % "4.0.0",
    "org.slf4j" % "slf4j-api" % "1.7.30",
    // "ch.qos.logback" % "logback-classic" % "1.2.3",
)

libraryDependencies ++= Seq(
    // https://stackoverflow.com/questions/22200130/how-do-i-run-junit-4-11-test-cases-with-sbt/28051194#28051194
    "junit" % "junit" % "4.13.1" % Test,
    "com.novocode" % "junit-interface" % "0.11" % Test exclude("junit", "junit-dep")
)

// excludeDependencies += "log4j" % "log4j"
// some depencies are directly relying on it