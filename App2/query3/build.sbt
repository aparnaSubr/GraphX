name := "graphx-query3"
version := "1.0"
scalaVersion := "2.11.7"
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.0.0",
                       "org.apache.spark" %% "spark-sql" % "2.0.0",
                       "org.apache.spark" %% "spark-graphx" % "2.0.0", 
                       "org.apache.spark" %% "spark-mllib" % "2.0.0",
                       "org.scalaj" %% "scalaj-collection" % "1.6")
