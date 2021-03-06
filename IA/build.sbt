name := "IA"

version := "0.1"

scalaVersion := "2.13.8"

//libraryDependencies += "org.apache.jena" % "jena-core" % "4.3.2"

resolvers ++= Seq(Resolver.sonatypeRepo("public"), "Confluent Maven Repo" at "https://packages.confluent.io/maven/")

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.36"

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.36"

libraryDependencies += "com.github.javafaker" % "javafaker" % "1.0.2"

libraryDependencies += "org.apache.jena" % "apache-jena-libs" % "4.3.2"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.1.0"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "3.1.0"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.13.1"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.1"

libraryDependencies += "com.twitter" % "bijection-avro_2.13" % "0.9.7"