name := "SparkNotebooks"

version := "0.1"

scalaVersion := "2.12.13"

resolvers += "SynapseML" at "https://mmlspark.azureedge.net/maven"
libraryDependencies += "com.microsoft.azure" %% "synapseml" % "0.9.4"
