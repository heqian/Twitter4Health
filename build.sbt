name := "Twitter4Health"

version := "0.1"

scalaVersion := "2.10.0-RC1"

scalacOptions += "-deprecation"

libraryDependencies ++= Seq(
	"org.twitter4j" % "twitter4j-core" % "2.2.6",
	"org.twitter4j" % "twitter4j-async" % "2.2.6",
	"org.twitter4j" % "twitter4j-stream" % "2.2.6",
	"com.typesafe" % "slick_2.10.0-RC1" % "0.11.2",
	"org.slf4j" % "slf4j-nop" % "1.6.4",
	"com.h2database" % "h2" % "1.3.166"
)
