
name := """meetup"""

EclipseKeys.withSource := true

version := "1.0"

scalaVersion := "2.11.2"


ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

jarName in assembly := "meetup.jar"

assemblyMergeStrategy in assembly := {
  case "application.conf" => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

libraryDependencies ++= Seq(
  "com.typesafe.akka"  %% "akka-actor"       % "2.3.9",
  "com.typesafe.akka"  %% "akka-slf4j"       % "2.3.9",
  "org.apache.spark" % "spark-streaming_2.11" % "1.3.0",
  "org.apache.spark" % "spark-mllib_2.11" % "1.3.0",
  "org.json4s" %% "json4s-jackson" % "3.2.10",
  "joda-time" % "joda-time" % "2.3",
  "org.joda" % "joda-convert" % "1.7",
  "com.ning" % "async-http-client" % "1.9.10",
  "org.json4s" %% "json4s-native" % "3.2.11",
  "org.yaml" % "snakeyaml" % "1.15",
  "org.slf4j" % "slf4j-log4j12" % "1.7.10"
)


resolvers ++= Seq(
  "Sonatype.org Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype.org Releases" at "http://oss.sonatype.org/service/local/staging/deploy/maven2",
  "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
  "artifactory" at "http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"
)


scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-Xlint",
  "-Ywarn-dead-code",
  "-language:_",
  "-target:jvm-1.7",
  "-encoding", "UTF-8"
)

javaOptions in run += "-Xmx8G"


parallelExecution in Test := false

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")
