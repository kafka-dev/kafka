import sbt._

class KafkaProject(info: ProjectInfo) extends DefaultProject(info) with IdeaProject {
  override def repositories = Set(ScalaToolsSnapshots, "JBoss Maven 2 Repository" at "http://repository.jboss.com/maven2", "Oracle Maven 2 Repository" at "http://download.oracle.com/maven")

  val log4j = "log4j" % "log4j" % "1.2.14"
  val jopt = "jopt-simple" % "jopt-simple" + "3.2"
  
  val cglib = "cglib" % "cglib" % "2.1_3" % "test"
  val objenesis = "org.objenesis" % "objenesis" % "1.0" % "test"
  val easymock = "org.easymock" % "easymock" % "3.0" % "test"
  val asm = "asm" % "asm" % "3.3" % "test" 
  val junit = "junit" % "junit" % "4.1" % "test"
  val junitInterface = "com.novocode" % "junit-interface" % "0.4" % "test"
}