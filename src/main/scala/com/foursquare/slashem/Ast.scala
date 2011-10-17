package com.foursquare.slashem
import org.elasticsearch.index.query.{QueryBuilder => ElasticQueryBuilder,
                                      FilterBuilder => ElasticFilterBuilder,
                                      QueryFilterBuilder,
                                      QueryStringQueryBuilder,
                                      BoolQueryBuilder,
                                      BoostingQueryBuilder,
                                      AndFilterBuilder,
                                      OrFilterBuilder,
                                      RangeQueryBuilder,
                                      RangeFilterBuilder};

import org.elasticsearch.index.query.QueryBuilders._;

object Ast {

  // ripped from lucene source code QueryParser.java
  def escape(q: String) = {
    val sb = new StringBuilder()
    q.trim.foreach{c =>
      // These characters are part of the query syntax and must be escaped
      if (c == '\\' || c == '+' || c == '-' || c == '!' || c == '(' || c == ')' || c == ':'
        || c == '^' || c == '[' || c == ']' || c == '\"' || c == '{' || c == '}' || c == '~'
        || c == '*' || c == '?' || c == '|' || c == '&') {
        sb.append('\\')
      }
      sb.append(c)
    }
    sb.toString
  }

  def quote(q: String) = "\"" + q + "\""


  abstract class AbstractClause {
    def extend: String
    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder
    //By default we can just use the QueryFilterBuilder and the query extender
    def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      new QueryFilterBuilder(this.elasticExtend(qf))
    }
  }

  //You can use a OrClause to join two clauses
  case class OrClause(s1: AbstractClause, s2: AbstractClause) extends AbstractClause {
    def extend(): String = {
      "("+s1.extend+") OR ("+s2.extend+")"
    }
    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder = {
      val q = new BoolQueryBuilder()
      List(s1,s2).map(_.elasticExtend(qf)).map(q.should(_))
      q
    }
    //By default we can just use the QueryFilterBuilder and the query extender
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      new OrFilterBuilder(s1.elasticFilter(qf),s2.elasticFilter(qf))
    }
  }
  case class AndClause(s1: AbstractClause, s2: AbstractClause) extends AbstractClause {
    def extend(): String = {
      "("+s1.extend+") AND ("+s2.extend+")"
    }
    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder = {
      val q = new BoolQueryBuilder()
      List(s1,s2).map(_.elasticExtend(qf)).map(q.must(_))
      q
    }
    //By default we can just use the QueryFilterBuilder and the query extender
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      new AndFilterBuilder(s1.elasticFilter(qf),s2.elasticFilter(qf))
    }
  }


  // A 'Clause' is something of the form 'field:(query)'
  // @param plus Set true for regular required or false to negate.
  case class Clause[T](fieldName: String, query: Query[T], plus: Boolean = true) extends AbstractClause {
    def extend(): String = {
      val q = query match {
        case Group(x) => query
        case Splat() => query
        case _ => Group(query)
      }
      //If a field does not have a name then do not attempt to specify it
      val qstr = fieldName match {
        case "" => q.extend
        case x => x + ":" + q.extend
      }
      plus match {
        case true => qstr
        case false => "-"+qstr
      }
    }

    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder = {
      //If its on a specific field support that
      val fields = fieldName match {
        case "" => qf
        case _ => List(WeightedField(fieldName))
      }
      query.elasticExtend(fields)
    }
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      //If its on a specific field support that
      val fields = fieldName match {
        case "" => qf
        case _ => List(WeightedField(fieldName))
      }
      query.elasticFilter(fields)
    }

  }

  //A field with a query weight
  case class WeightedField(fieldName: String, boost: Double = 1) {
    def extend(): String = {
      boost match {
        case 1.0 => fieldName
        case x => fieldName+"^"+x
      }
    }
  }

  //A phrase weighted field. Results in a document scoring bost
  //pf => traditional phrase query
  //pf2 => in edismax type queries two word shingle matches
  //pf3 => in edismax type queries three word shingle matches
  case class PhraseWeightedField(fieldName: String, boost: Double = 1, pf: Boolean, pf2: Boolean, pf3: Boolean) {
    def extend(): String = {
      boost match {
        case 1 => fieldName
        case x => fieldName+"^"+x
      }
    }
  }

  abstract class Query[T]() {
    def extend: String
    def and(c: Query[T]): Query[T] = And(this, c)
    def or(c: Query[T]): Query[T] = Or(this, c)
    def boost(b: Float): Query[T] = Boost(this, b)
    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder
    def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      new QueryFilterBuilder(this.elasticExtend(qf))
    }

  }

  case class Empty[T]() extends Query[T] {
    def extend = "\"\""
    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder = {
      val q = new QueryStringQueryBuilder(this.extend)
      qf.map(f => q.field(f.fieldName,f.boost.toFloat))
      q
    }
  }

  case class Phrase[T](query: T, escaped: Boolean = true) extends Query[T] {
    def extend = {'"' + escape(query.toString) + '"'}
    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder = {
      val q = new QueryStringQueryBuilder(this.extend)
      qf.map(f => q.field(f.fieldName,f.boost.toFloat))
      q
    }
  }

  case class Range[T](q1: Query[T],q2: Query[T]) extends Query[T] {
    def extend = {'['+q1.extend+" TO "+ q2.extend +']'}
    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder = {
      val q = new RangeQueryBuilder(qf.head.fieldName)
      q.from(q1)
      q.to(q2)
    }
    //By default we can just use the QueryFilterBuilder and the query extender
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      val q = new RangeFilterBuilder(qf.head.fieldName)
      q.from(q1)
      q.to(q2)
    }
  }


  //We take a look at the list of fields being queried
  //and the list of phrase boost fields and generate
  //corresponding phrase boost queries.
  case class BagOfWords[T](query: T) extends Query[T] {
    def extend = escape(query.toString)
    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder = {
      val q = new QueryStringQueryBuilder(this.extend)
      qf.map(f => q.field(f.fieldName,f.boost.toFloat))
      q
    }
  }

  case class Group[T](items: Query[T]) extends Query[T] {
    def extend = {"(%s)".format(items.extend)}
    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder = {
      items.elasticExtend(qf)
    }
    //By default we can just use the QueryFilterBuilder and the query extender
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      items.elasticFilter(qf)
    }
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
    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder = {
      val q = new BoolQueryBuilder()
      List(q1,q2).map(_.elasticExtend(qf)).map(q.must(_))
      q
    }
    //By default we can just use the QueryFilterBuilder and the query extender
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      new AndFilterBuilder(q1.elasticFilter(qf),q2.elasticFilter(qf))
    }
  }

  case class Or[T](q1: Query[T], q2: Query[T]) extends Query[T] {
    def extend = "%s OR %s".format(q1.extend, q2.extend)
    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder = {
      val q = new BoolQueryBuilder()
      List(q1,q2).map(_.elasticExtend(qf)).map(q.should(_))
      q
    }
    //By default we can just use the QueryFilterBuilder and the query extender
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      new OrFilterBuilder(q1.elasticFilter(qf),q2.elasticFilter(qf))
    }
  }

  case class Splat[T]() extends Query[T] {
    def extend = "*"
    //Is there a better way to do this?
    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder = {
      val q = new QueryStringQueryBuilder(this.extend)
      qf.map(f => q.field(f.fieldName,f.boost.toFloat))
      q
    }
  }

  case class Boost[T](q: Query[T], boost: Float) extends Query[T] {
    def extend = q.extend + "^" + boost.toString
    def elasticExtend(qf: List[WeightedField]): ElasticQueryBuilder = {
      val boostedQuery = new BoostingQueryBuilder()
      if (boost > 0) {
        boostedQuery.positive(q.elasticExtend(qf))
      } else {
        boostedQuery.negative(q.elasticExtend(qf))
      }
      boostedQuery.boost(boost.abs)
      boostedQuery
    }
  }
}
