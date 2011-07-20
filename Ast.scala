package com.foursquare.solr
import com.foursquare.lib.Solr

object Ast {

  // A 'Clause' is something of the form 'field:(query)'
  case class Clause[T](fieldName: String, query: Query[T]) {
    def extend(): String = {
      val q = query match {
        case Group(x) => query
        case _ => Group(query)
      }
      //If a field does not have a name then do not attempt to specify it
      fieldName match {
        case "" => q.extend
        case x => x + ":" + q.extend
      }
    }
  }

  case class WeightedField(fieldName: String, boost: Double = 1) {
    def extend(): String = {
      boost match {
        case 1 => fieldName
        case x => fieldName+"^"+x
      }
    }
  }

  case class PhraseWeightedField(fieldName: String, boost: Double = 1, pf: Boolean, pf2: Boolean, pf3: Boolean) {
    def extend(): String = {
      boost match {
        case 1 => fieldName
        case x => fieldName+"^"+x
      }
    }
  }

  // TODO: Implement Range (maybe)
  abstract class Query[T]() {
    def extend: String
    def and(c: Query[T]): Query[T] = And(this, c)
    def or(c: Query[T]): Query[T] = Or(this, c)
    def boost(b : Float): Query[T] = Boost(this,b)
  }

  case class Phrase[T](query: T, escaped: Boolean = true) extends Query[T] {
    def extend = {'"' + Solr.escape(query.toString) + '"'}
  }

  case class Range[T](q1: T,q2: T) extends Query[T] {
    def extend = {'['+Solr.escape(q1.toString)+" to "+ Solr.escape(q2.toString) +']'}
  }

  case class UnescapedPhrase[T](query: T) extends Query[T] {
    def extend = query.toString
  }

  case class BagOfWords[T](query: T) extends Query[T] {
    def extend = Solr.escape(query.toString)
  }

  case class Group[T](items: Query[T]) extends Query[T] {
    def extend = {"(%s)".format(items.extend)}
  }


  // Override the constructor for Group so that it gobbles nested Groups, ie
  // Group(Group(foo))) => Group(foo)
  // Fails to compile with 'method apply is defined twice'
/*  object Group {
    def apply[T](items: Query[T]) = {
      new Group(Phrase("asdf"))}
      items match {
        case x: Group[_] => items
        case x => new Group(x)
      }
    }
  } */


  case class And[T](q1: Query[T], q2: Query[T]) extends Query[T] {
    def extend = "%s AND %s".format(q1.extend, q2.extend)
  }

  case class Or[T](q1: Query[T], q2: Query[T]) extends Query[T] {
    def extend = "%s OR %s".format(q1.extend, q2.extend)
  }

  case class Plus[T](q: Query[T]) extends Query[T] {
    def extend = "+" + q.extend
  }

  case class Minus[T](q: Query[T]) extends Query[T] {
    def extend = "-" + q.extend
  }

  case class Splat[T]() extends Query {
    def extend = "*"
  }

  case class Boost[T](q: Query[T], boost: Float) extends Query[T] {
    def extend = q.extend + "^" + boost.toString
  }
}
