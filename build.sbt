name := "slashem"

version := "0.15.4"

organization := "com.foursquare"

scalaVersion := "2.9.1"

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
    case "2.9.1" => "2.4"
    case _       => "2.4-M2"
  }
  Seq(
    "net.liftweb"             %% "lift-record" % liftVersion  % "compile",
    "org.mongodb"              % "mongo-java-driver"    % "[2.6.5,)"   % "compile",
    "junit"                    % "junit"                % "[4.8.2,)"   % "test",
    "com.novocode"             % "junit-interface"      % "[0.7,)"     % "test" ,
    "org.scala-tools.testing" %% "specs"                % specsVersion % "test",
    "org.elasticsearch"        % "elasticsearch"  % "0.19.4" % "compile" exclude("log4j", "log4j") exclude("com.sun.jmx","jmxri") exclude("com.sun.jdmk","jmxtools") exclude("com.codahale","jerkson_2.8.1") exclude("com.codahale","jerkson") exclude("com.twitter","streamyj_2.8.1") exclude("org.codehaus.jackson" , "jackson-mapper-asl") exclude("org.codehas.jackson" , "jackson-core-asl"),
    "org.codehaus.jackson"     % "jackson-mapper-asl" % "1.8.8",
    "org.codehaus.jackson"     % "jackson-core-asl" % "1.8.8",
    "org.scala-tools.testing" %% "scalacheck"         % scalaCheckVersion   % "test",
    "com.twitter"              % "finagle-core"        % "5.3.23" % "compile" exclude("thrift", "libthrift"),
    "com.twitter"              % "finagle-http"        % "5.3.23" % "compile" exclude("thrift", "libthrift"),
    "com.twitter"              % "util-core"           % "5.3.14" % "compile",
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

resolvers ++= Seq(
  "Bryan J Swift Repository" at "http://repos.bryanjswift.com/maven2/",
  "twitter maven repo" at "http://maven.twttr.com/",
  "codehaus maven repo" at "http://repository.codehaus.org/",
  "sonatype maven repo" at "http://oss.sonatype.org/content/repositories/releases/"
)


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

ivyXML := (
<dependencies>
 <exclude module="jmxtools"/>
 <exclude module="jmxri"/>
</dependencies>
)
