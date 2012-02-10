name := "slashem"

version := "0.6.2"

organization := "com.foursquare"

crossScalaVersions := Seq("2.8.1", "2.9.0", "2.9.0-1", "2.9.1", "2.8.0")

libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val specsVersion = scalaVersion match {
    case "2.8.0" => "1.6.5"
    case "2.9.1" => "1.6.9"
    case _       => "1.6.8"
  }
  val scalaCheckVersion = scalaVersion match {
    case "2.8.0" => "1.8"
    case "2.8.1" => "1.8"
    case _ => "1.9"
  }
  val liftVersion = scalaVersion match {
    case "2.9.1" => "2.4-M4"
    case _       => "2.4-M2"
  }
  Seq(
    "net.liftweb"             %% "lift-mongodb-record" % liftVersion  % "compile",
    "junit"                    % "junit"               % "4.5"        % "test",
    "com.novocode"             % "junit-interface"     % "0.6"        % "test",
    "ch.qos.logback"           % "logback-classic"     % "0.9.26"     % "provided",
    "org.scala-tools.testing" %% "specs"               % specsVersion % "test",
    "org.scala-lang"           % "scala-compiler"      % scalaVersion % "test",
    "org.elasticsearch"        % "elasticsearch"  % "0.18.5" % "compile",
    "com.twitter"             % "finagle-core"        % "1.8.0" % "compile",
    "org.codehaus.jackson" % "jackson-mapper-asl" % "1.8.0" % "compile",
    "org.scala-tools.testing" %% "scalacheck"         % scalaCheckVersion   % "test"    withSources(),
    "com.twitter"             % "finagle"             % "1.8.0"  % "compile"           intransitive(),
    "com.twitter"             % "finagle-core"        % "1.8.0" % "compile",
    "org.scalaj"              %% "scalaj-collection" % "1.2"
  )
}

publishTo <<= (version) { v =>
  val nexus = "https://oss.sonatype.org/"
  if (v.endsWith("-SNAPSHOT"))
    Some("snapshots" at nexus+"content/repositories/snapshots/")
  else
    Some("releases" at nexus+"service/local/staging/deploy/maven2")
}

resolvers += "Bryan J Swift Repository" at "http://repos.bryanjswift.com/maven2/"

resolvers += "twitter maven repo" at "http://maven.twttr.com/"

resolvers += "codehaus maven repo" at "http://repository.codehaus.org/"

resolvers += "sonatype maven repo" at "http://oss.sonatype.org/content/repositories/releases/"

resolvers <++= (version) { v =>
  if (v.endsWith("-SNAPSHOT"))
    Seq(ScalaToolsSnapshots)
  else
    Seq()
}

scalacOptions ++= Seq("-deprecation", "-unchecked")

testFrameworks += new TestFramework("com.novocode.junit.JUnitFrameworkNoMarker")


credentials ++= {
  val sonaType = ("Sonatype Nexus Repository Manager", "oss.sonatype.org")
  def loadMavenCredentials(file: java.io.File) : Seq[Credentials] = {
    xml.XML.loadFile(file) \ "servers" \ "server" map (s => {
      val host = (s \ "id").text
      val realm = if (host == sonaType._2) sonaType._1 else "Unknown"
      Credentials(realm, host, (s \ "username").text, (s \ "password").text)
    })
  }
  val ivyCredentials   = Path.userHome / ".ivy2" / ".credentials"
  val mavenCredentials = Path.userHome / ".m2"   / "settings.xml"
  (ivyCredentials.asFile, mavenCredentials.asFile) match {
    case (ivy, _) if ivy.canRead => Credentials(ivy) :: Nil
    case (_, mvn) if mvn.canRead => loadMavenCredentials(mvn)
    case _ => Nil
  }
}

publishMavenStyle := true

pomIncludeRepository := { x => false }

pomExtra := (
<url>https://github.com/foursquare/slashem</url>
<licenses>
  <license>
    <name>Apache 2</name>
    <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
    <distribution>repo</distribution>
    <comments>A business-friendly OSS license</comments>
  </license>
</licenses>
<scm>
 <url>git@github.com/foursquare/slashem.git</url>
 <connection>scm:git:git@github.com/foursquare/slashem.git</connection>
</scm>
<developers>
 <developer>
 <id>holdenkarau></id>
 <name>Holden Karau</name>
 <email>holden@foursquare.com</email>
 </developer>
 <developer>
 <id>jonshea</id>
 <name>Jon Shea</name>
 <email>jonshea@foursquare.com</email>
 </developer>
 <developer>
 <name>Govind Kabra</name>
 <email>govind@foursquare.com</email>
 </developer>
 <developer>
 <name>Adam Alix</name>
 <email>aalix@foursquare.com</email>
 </developer>
</developers>
)