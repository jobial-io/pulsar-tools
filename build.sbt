/*
 * Copyright (c) 2020 Jobial OÜ. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
name := "pulsar-tools"

ThisBuild / organization := "io.jobial"
ThisBuild / scalaVersion := "2.13.8"
ThisBuild / crossScalaVersions := Seq("2.11.12", "2.12.15", "2.13.8")
ThisBuild / version := "0.0.9"
ThisBuild / scalacOptions += "-target:jvm-1.8"
ThisBuild / javacOptions ++= Seq("-source", "11", "-target", "11")
ThisBuild / Test / packageBin / publishArtifact := true
ThisBuild / Test / packageSrc / publishArtifact := true
ThisBuild / Test / packageDoc / publishArtifact := true
ThisBuild / resolvers += "Mulesoft" at "https://repository.mulesoft.org/nexus/content/repositories/public/"
Compile / packageBin / mappings ~= {
  _.filter(!_._1.getName.endsWith("logback.xml"))
}

import sbt.Keys.{description, libraryDependencies, publishConfiguration}
import sbt.addCompilerPlugin
import xerial.sbt.Sonatype._

lazy val commonSettings = Seq(
  publishConfiguration := publishConfiguration.value.withOverwrite(true),
  publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true),
  publishM2Configuration := publishM2Configuration.value.withOverwrite(true),
  publishTo := publishTo.value.orElse(sonatypePublishToBundle.value),
  sonatypeProjectHosting := Some(GitHubHosting("jobial-io", "pulsar-tools", "orbang@jobial.io")),
  organizationName := "Jobial OÜ",
  licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  description := "Pulsar monitoring and admin tools",
  addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  scalacOptions ++= (if (scalaBinaryVersion.value != "2.13") Seq("-Ypartial-unification") else Seq())
)

lazy val SclapVersion = "1.3.6"
lazy val ScaseVersion = "1.2.8"
lazy val PulsarVersion = "2.10.3"
lazy val LogbackVersion = "1.2.3"

lazy val root: Project = project
  .in(file("."))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "io.jobial" %% "scase-pulsar" % ScaseVersion,
      "io.jobial" %% "scase-core" % ScaseVersion % "compile->compile;test->test",
      "io.jobial" %% "scase-tibco-rv" % ScaseVersion,
      "io.jobial" %% "sclap" % SclapVersion,
      "org.apache.pulsar" % "pulsar-client-admin" % PulsarVersion,
      "ch.qos.logback" % "logback-classic" % LogbackVersion
    ),
    Compile / unmanagedJars ++= Seq(file(sys.env.get("TIBCO_RV_ROOT").getOrElse(sys.props("tibco.rv.root")) + "/lib/tibrvj.jar"))
  )

