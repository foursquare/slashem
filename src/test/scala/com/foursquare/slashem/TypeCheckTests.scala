package com.foursquare.slashem

import com.foursquare.slashem._

import org.bson.types.ObjectId
import org.junit.Test
import org.junit._

import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary.arbitrary

import org.specs.SpecsMatchers
import org.specs.matcher.ScalaCheckMatchers


class TypeCheckTests extends SpecsMatchers with ScalaCheckMatchers {
  @Test
  def thingsThatShouldntCompile {
    val compiler = new Compiler
    def check(code: String, shouldTypeCheck: Boolean = false): Unit = {
      compiler.typeCheck(code) aka "'%s' compiles!".format(code) must_== shouldTypeCheck
    }

    //Make sure our basic does compile
    check("""SVenueTest where (_.metall any) useQueryType("edismax") orderDesc(_.decayedPopularity1)""", shouldTypeCheck = true)
    //Make sure trying to limit a limit query doesn't work
    check("""SVenueTest where (_.metall any) useQueryType("edismax") orderDesc(_.decayedPopularity1) limit(1) limit(10)""")
    //Conflicting order statements
    check("""SVenueTest where (_.metall any) useQueryType("edismax") orderDesc(_.decayedPopularity1)  orderDesc(_.meta_categories) """)
    check("""SEventTest where (_.default contains "hixxy") useQueryType("edismax") filter(_.tags inRadius(geoLat, geoLong, 1))""")
    check("""
    import org.joda.time.{DateTime, DateTimeZone}
          val d1 = new DateTime(2011, 5, 1, 0, 0, 0, 0, DateTimeZone.UTC)
          val d2 = new DateTime(2011, 5, 2, 0, 0, 0, 0, DateTimeZone.UTC)
          SEventTest where (_.name inRange(d1, d2))""")



    check("""
    case class TestPirate(state: Option[String])
    SVenueTest where (_.name eqs "test") selectCase(_.name,((x: Option[String]) => TestPirate(x)))
              """,shouldTypeCheck=true)
    check("""
    case class TestPirate(state: Option[String])
    SVenueTest where (_.name eqs "test") selectCase(_.name,((x: Option[String]) => TestPirate(x))) selectCase(_.name,((x: Option[String]) => TestPirate(x)))
              """,shouldTypeCheck=false)

 }

  //Stolen from Rogue
  class Compiler {
    import java.io.{PrintWriter, Writer}
    import scala.io.Source
    import scala.tools.nsc.{Interpreter, InterpreterResults, Settings}

    class NullWriter extends Writer {
      override def close() = ()
      override def flush() = ()
      override def write(arr: Array[Char], x: Int, y: Int): Unit = ()
    }

    private val settings = new Settings
    val loader = manifest[SVenueTest].erasure.getClassLoader
    settings.classpath.value = Source.fromURL(loader.getResource("app.class.path")).mkString
    settings.bootclasspath.append(Source.fromURL(loader.getResource("boot.class.path")).mkString)
    settings.deprecation.value = true // enable detailed deprecation warnings
    settings.unchecked.value = true // enable detailed unchecked warnings

    // This is deprecated in 2.9.x, but we need to use it for compatibility with 2.8.x
    private val interpreter =
      new Interpreter(
        settings,
        /*
         * It's a good idea to comment out this second parameter when adding or modifying
         * tests that shouldn't compile, to make sure that the tests don't compile for the
         * right reason.
         *
         */
        new PrintWriter(new NullWriter())
      )

    interpreter.interpret("""import com.foursquare.slashem._""")

    def typeCheck(code: String): Boolean = {
      interpreter.interpret(code) match {
        case InterpreterResults.Success => true
        case InterpreterResults.Error => false
        case InterpreterResults.Incomplete => throw new Exception("Incomplete code snippet")
      }
    }
  }

}
