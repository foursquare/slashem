// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.slashem

import org.elasticsearch.index.query.{QueryBuilder => ElasticQueryBuilder,
                                      FilterBuilder => ElasticFilterBuilder,
                                      QueryFilterBuilder,
                                      QueryStringQueryBuilder,
                                      BoolQueryBuilder,
                                      BoolFilterBuilder,
                                      BoostingQueryBuilder,
                                      AndFilterBuilder,
                                      OrFilterBuilder,
                                      RangeQueryBuilder,
                                      RangeFilterBuilder};

import org.elasticsearch.index.query.QueryBuilders._;

object Ast {

  val escapePattern = """\b(OR|AND|or|and)\b""".r

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
      //Added (not part of QueryParser.java)
      if (c != '’') {
        sb.append(c)
      }
    }
    //Added (not part of QueryParser.java)
    escapePattern.replaceAllIn(sb.toString,m => "\"" + m.group(0) + "\"")
  }

  def quote(q: String) = "\"" + q + "\""


  abstract class AbstractClause {
    def extend: String
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder
    //By default we can just use the QueryFilterBuilder and the query extender
    def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      new QueryFilterBuilder(this.elasticExtend(qf, Nil))
    }
    def or(clauses: AbstractClause*) = {
      OrClause(this::clauses.toList)
    }
    def and(clauses: AbstractClause*) = {
      AndClause(this::clauses.toList)
    }
  }

  //You can use a OrClause to join two clauses
  case class OrClause(clauses: List[AbstractClause]) extends AbstractClause {
    def extend(): String = {
      clauses.map(c => "(" + c.extend + ")").mkString(" OR ")
    }
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder = {
      val q = new BoolQueryBuilder()
      clauses.map(_.elasticExtend(qf, pf)).map(q.should(_))
      q
    }
    //By default we can just use the QueryFilterBuilder and the query extender
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      new OrFilterBuilder(clauses.map(_.elasticFilter(qf)):_*)
    }
  }
  case class AndClause(clauses: List[AbstractClause]) extends AbstractClause {
    def extend(): String = {
      clauses.map(c => "(" + c.extend + ")").mkString(" AND ")
    }
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder = {
      val q = new BoolQueryBuilder()
      clauses.map(_.elasticExtend(qf, pf)).map(q.must(_))
      q
    }
    //By default we can just use the QueryFilterBuilder and the query extender
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      val filters = clauses.map(_.elasticFilter(qf))
      new AndFilterBuilder(filters:_*)
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

    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder = {
      //If its on a specific field support that
      val fields = fieldName match {
        case "" => qf
        case _ => List(WeightedField(fieldName))
      }
      val baseQuery = query.elasticExtend(fields, pf)
      plus match {
        case true => baseQuery
        case false => (new BoolQueryBuilder()).mustNot(baseQuery)
      }
    }
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      //If its on a specific field support that
      val fields = fieldName match {
        case "" => qf
        case _ => List(WeightedField(fieldName))
      }
      val baseQuery = query.elasticFilter(fields)
      plus match {
        case true => baseQuery
        case false => (new BoolFilterBuilder()).mustNot(baseQuery)
      }
    }

  }

  case class Field(fieldName: String) extends ScoreBoost {
    def extend(): String = {
      fieldName
    }
    def elasticExtend(): String = {
      "(doc['" + fieldName + "'].value)"
    }
  }

  //A field with a query weight
  case class WeightedField(fieldName: String, boost: Double = 1) extends ScoreBoost {
    def extend(): String = {
      boost match {
        case 1.0 => fieldName
        case x => fieldName+"^"+x
      }
    }
    def elasticExtend(): String = {
      boost match {
        case 1.0 => "(doc['"+fieldName+"'].value)"
        case _ => "(doc['"+fieldName+"'].value *"+boost+")"
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
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder
    def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      new QueryFilterBuilder(this.elasticExtend(qf, Nil))
    }

  }

  //Name doesn't have to be a field name for Solr
  //it could be "lat,lng". However for ES it must be
  //a field
  abstract class ScoreBoost {
    def extend(): String
    def elasticExtend(): String
  }

  case class GeoDist(name: String, lat: Double, lng: Double, distType: String = "") extends ScoreBoost {
    def extend = { distType match {
      case "square" => "sqedist(%s,%s,%s)".format(lat,lng,name)
      case _ => "dist(2,%s,%s,%s)".format(lat,lng,name)
    }}
    def elasticExtend = {
      val distanceInKm = "doc['%s'].distanceInKm(%s,%s)".format(name,lat,lng)
      distType match {
        case "square" => "pow(%s,2.0)".format(distanceInKm)
        case _ => distanceInKm
      }
    }
  }
  case class Recip(query: ScoreBoost, x: Int, y: Int, z: Int) extends ScoreBoost{
    def extend = "recip(%s,%d,%d,%d)".format(query.extend,x,y,z)
    def elasticExtend = "%d.0*pow(((%d.0*(%s))+%d.0),-1.0)".format(y,x,query.elasticExtend,z)
  }

  case class Empty[T]() extends Query[T] {
    def extend = "\"\""
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder = {
      val q = new QueryStringQueryBuilder(this.extend)
      qf.map(f => q.field(f.fieldName,f.boost.toFloat))
      q
    }
  }

  case class Phrase[T](query: T, escaped: Boolean = true) extends Query[T] {
    def extend = {'"' + escape(query.toString) + '"'}
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder = {
      val q = new QueryStringQueryBuilder(this.extend)
      qf.map(f => q.field(f.fieldName,f.boost.toFloat))
      q
    }
  }

  case class PhrasePrefix[T](query: T, escaped: Boolean = true) extends Query[T] {
    def extend = {'"' + escape(query.toString) + '*' + '"'}
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder = {
      val q = disMaxQuery()
      q.tieBreaker(1)

      qf.map(f => {
        val basePhrase = textPhraseQuery(f.fieldName,this.extend)
        val phraseQuery = f.boost match {
          case 1 => basePhrase
          case _ => basePhrase.boost(f.boost.toFloat)
        }
        q.add(phraseQuery)
      }
           )
      q
    }
  }


  case class Range[T](q1: Query[T],q2: Query[T]) extends Query[T] {
    def extend = {'['+q1.extend+" TO "+ q2.extend +']'}
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder = {
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
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder = {
      val normalq = new QueryStringQueryBuilder(this.extend)
      qf.map(f => normalq.field(f.fieldName,f.boost.toFloat))
      val qfnames = qf.map(_.fieldName).toSet
      val queriesToGen = pf.filter(qfnames contains _.fieldName)
        queriesToGen match {
          case Nil => normalq
          case _ => {
            val q = disMaxQuery()
            q.tieBreaker(1)
            q.add(normalq)
            queriesToGen.map(f => {
              val basePhrase = textPhraseQuery(f.fieldName,this.extend)
              val phraseQuery = f.boost match {
                case 1 => basePhrase
                case _ => basePhrase.boost(f.boost.toFloat)
              }
              q.add(phraseQuery)
            }
                           )
            q
          }
        }
    }
  }

  case class Group[T](items: Query[T]) extends Query[T] {
    def extend = {"(%s)".format(items.extend)}
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder = {
      items.elasticExtend(qf, pf)
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
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder = {
      val q = new BoolQueryBuilder()
      List(q1,q2).map(_.elasticExtend(qf, pf)).map(q.must(_))
      q
    }
    //By default we can just use the QueryFilterBuilder and the query extender
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      new AndFilterBuilder(q1.elasticFilter(qf),q2.elasticFilter(qf))
    }
  }

  case class Or[T](q1: Query[T], q2: Query[T]) extends Query[T] {
    def extend = "%s OR %s".format(q1.extend, q2.extend)
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder = {
      val q = new BoolQueryBuilder()
      List(q1,q2).map(_.elasticExtend(qf, pf)).map(q.should(_))
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
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder = {
      val q = new QueryStringQueryBuilder(this.extend)
      qf.map(f => q.field(f.fieldName,f.boost.toFloat))
      q
    }
  }

  case class Boost[T](q: Query[T], boost: Float) extends Query[T] {
    def extend = q.extend + "^" + boost.toString
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField]): ElasticQueryBuilder = {
      val boostedQuery = new BoostingQueryBuilder()
      if (boost > 0) {
        boostedQuery.positive(q.elasticExtend(qf, pf))
      } else {
        boostedQuery.negative(q.elasticExtend(qf, pf))
      }
      boostedQuery.boost(boost.abs)
      boostedQuery
    }
  }
}
