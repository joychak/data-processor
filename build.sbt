
val commonSettings = Seq(
  organization := "com.datalogs",
  scalaVersion := "2.11.8",
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-target:jvm-1.8"),
  javacOptions  ++= Seq("-source", "1.8", "-target", "1.8"),

  // Added for ScalaTest
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test",

  test in assembly := {},

  parallelExecution in Test := false
)


lazy val sparkprocessorsettings = commonSettings ++ Seq(
  libraryDependencies ++= Seq(
    "org.rogach" %% "scallop" % "1.0.0",
    "joda-time" % "joda-time" % "2.9.3",
    "org.joda" % "joda-convert" % "1.8",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.4.4",
    "org.apache.avro" % "avro-mapred" % "1.8.0" exclude("org.mortbay.jetty", "servlet-api"),
    "org.apache.parquet" % "parquet-avro" % "1.8.0",
    "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-hive" % "2.1.0" % "provided",
    "com.databricks" %% "spark-xml" % "0.3.3"
  )
)

lazy val projsparkprocessorcore = Project(id = "spark-processor-core", base = file ("spark-processor-core"))
  .settings(sparkprocessorsettings)
  .settings(
    name := "spark-processor-core",
    artifact in (Compile, assembly) := {
      val art = (artifact in (Compile, assembly)).value
      art.copy(`classifier` = Some("assembly"))
    },
    addArtifact(artifact in (Compile, assembly), assembly),
    publishArtifact := false
  )

lazy val projsparkprocessordataset = Project(id = "spark-processor-dataset", base = file ("spark-processor-dataset"))
  .settings(sparkprocessorsettings)
  .dependsOn(projsparkprocessorcore)
  .settings(
    name := "spark-processor-dataset",
//    sbtavro.SbtAvro.projectSettings ++ Seq(
//      version in AvroConfig := "1.8.2",
//      javaSource in AvroConfig := sourceDirectory.value / "generated" / "avro",
//      stringType in AvroConfig := "String"
//    ),
    artifact in (Compile, assembly) := {
      val art = (artifact in (Compile, assembly)).value
      art.copy(`classifier` = Some("assembly"))
    },
    addArtifact(artifact in (Compile, assembly), assembly),
    publishArtifact := true
  )



////////////////////////////// Root project //////////////////////////////////////////
lazy val processor = project.in(file ("."))
  .settings(commonSettings)
  .settings(
    test := { },
    publish := { },
    publishLocal := { }
  )
  .aggregate(projsparkprocessorcore, projsparkprocessordataset)