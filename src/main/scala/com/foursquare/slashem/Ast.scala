// Copyright 2011-2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.slashem

import org.elasticsearch.index.query.{FilterBuilder => ElasticFilterBuilder,
                                      FilterBuilders => EFilterBuilders,
                                      QueryBuilder => ElasticQueryBuilder,
                                      QueryBuilders => EQueryBuilders,
                                      QueryStringQueryBuilder}

/**
 * Abstract Syntax Tree used to represent queries.
 */
object Ast {

  val escapePattern = """\b(OR|AND|or|and)\b""".r

  /**
   * Ripped from lucene source code QueryParser.java
   * http://bit.ly/AzKzV3
   *
   * @param q Query string to sanitize
   */
  def escape(q: String): String = {
    val sb = new StringBuilder()
    q.trim.foreach{c: Char =>
      // These characters are part of the query syntax and must be escaped
      if (c == '\\' || c == '+' || c == '-' || c == '!' || c == '(' || c == ')' || c == ':'
        || c == '^' || c == '[' || c == ']' || c == '\"' || c == '{' || c == '}' || c == '~'
        || c == '*' || c == '?' || c == '|' || c == '&') {
        sb.append('\\')
      }
      // Added (not part of QueryParser.java)
      if (c != '\u2019') {
        sb.append(c)
      }
    }
    // Added (not part of QueryParser.java)
    escapePattern.replaceAllIn(sb.toString, m => {"\"" + m.group(0) + "\""})
  }

  /**
   * Return the input string in an escaped quotes
   *
   * @param q Query string to quote
   */
  def quote(q: String): String = "\"" + q + "\""

  /**
   * AbstractClause represents query clauses.
   */
  abstract class AbstractClause {
    /**
     * Returns the clause's Solr query format string representation
     */
    def extend(): String
    /**
     * Extend the query into its elastic search format
     *
     * @param qf List of @see WeightedField
     * @param pf List of @see PhraseWeightedField
     * @param mm Option[String] representing the minimum match percentage
     * @return ElasticQueryBuilder representing the clause
     */
    def elasticExtend(qf: List[WeightedField],
                      pf: List[PhraseWeightedField],
                      mm: Option[String]): ElasticQueryBuilder

    /**
     * Creates a filter from a list of weighted fields.
     *
     * @param qf List of weighted fields used to build a filter list.
     * @return ElasticFilterBuilder
     */
    def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      // By default we can just use the QueryFilterBuilder and the query extender
      EFilterBuilders.queryFilter(this.elasticExtend(qf, Nil, None))
    }

    /**
     * Generate an OrClause from a list of clauses
     *
     * @param clauses a list of abstract clauses to OR together
     * @return OrClause inputs ORed together
     */
    def or(clauses: AbstractClause*): OrClause = {
      OrClause(this::clauses.toList)
    }

    /**
     * Generate an AndClause from a list of clauses
     *
     * @param clauses a list of abstract clauses to AND together
     * @return AbstractClause* ANDed together @see AndClause
     */
    def and(clauses: AbstractClause*): AndClause = {
      AndClause(this::clauses.toList)
    }
  }

  /**
   * Case class representing a list of clauses ORed together
   *
   * You can use a OrClause() to join two or more clauses with an OR
   */
  case class OrClause(clauses: List[AbstractClause]) extends AbstractClause {
    /** @inheritdoc */
    def extend(): String = {
      clauses.map(c => "(" + c.extend + ")").mkString(" OR ")
    }

    /** @inheritdoc */
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField],
                      mm: Option[String]): ElasticQueryBuilder = {
      val q = EQueryBuilders.boolQuery
      clauses.map(c => c.elasticExtend(qf, pf, mm)).map(eqb => q.should(eqb))
      q
    }
    /**
     * @inheritdoc
     * By default we can just use the QueryFilterBuilder and the query extender
     */
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      EFilterBuilders.orFilter(clauses.map(_.elasticFilter(qf)):_*)
    }
  }

  /**
   * Case class representing a list of clauses ANDed together
   */
  case class AndClause(clauses: List[AbstractClause]) extends AbstractClause {
    /** @inheritdoc */
    def extend(): String = {
      clauses.map(c => "(" + c.extend() + ")").mkString(" AND ")
    }
    /** @inheritdoc */
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField],
                      mm: Option[String]): ElasticQueryBuilder = {
      val query = EQueryBuilders.boolQuery
      // Extend the clauses and add them to the ElasticQueryBuilder
      clauses.map(ac => ac.elasticExtend(qf, pf, mm)).map(eqb => query.must(eqb))
      query
    }

    /**
     * @inheritdoc
     * By default we can just use the QueryFilterBuilder and the query extender
     */
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      val filters = clauses.map(_.elasticFilter(qf))
      EFilterBuilders.andFilter(filters:_*)
    }
  }

  /**
   * A 'Clause' is something of the form 'field:(query)'
   * @param plus Defaults to true, used to negate queries (by setting to false).
   */
  case class Clause[T](fieldName: String, query: Query[T], plus: Boolean = true) extends AbstractClause {
    /** @inheritdoc */
    def extend(): String = {
      val (q,boost) = query match {
        case Group(x) => (query,1)
        case Splat() => (query,1)
        case Boost(Group(x),b) => (Group(x),b)
        case Boost(x,b) => (Group(x),b)
        case _ => (Group(query),1)
      }
      // If a field does not have a name then do not attempt to specify it
      val qstr = fieldName match {
        case "" => q.extend()
        case x => x + ":" + q.extend()
      }
      val booleanQuery = plus match {
        case true => qstr
        //This is added as a work around for the lack of support of
        //pure negative queries (even though its partially supported
        //now it turns out they don't work so well when nested)
        case false => "(*:* -"+qstr+")"
      }
      //Boost as approraite
      boost match {
        case 1.0 => booleanQuery
        case x => booleanQuery+"^"+x
      }
    }

    /** @inheritdoc */
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField],
                      mm: Option[String]): ElasticQueryBuilder = {
      // Support extending specific fields.
      val fields = fieldName match {
        case "" => qf
        case _ => List(WeightedField(fieldName))
      }
      val baseQuery = query.elasticExtend(fields, pf, mm)
      plus match {
        case true => baseQuery
        case false => EQueryBuilders.boolQuery.mustNot(baseQuery)
      }
    }

    /** @inheritdoc */
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      // Support extending specific fields.
      val fields = fieldName match {
        case "" => qf
        case _ => List(WeightedField(fieldName))
      }
      val baseFilters = query.elasticFilter(fields)
      plus match {
        case true => baseFilters
        case false => EFilterBuilders.boolFilter.mustNot(baseFilters)
      }
    }
  }

  /**
   * Class representing a field that can have its score boosted.
   * @see ScoreBoost
   */
  case class Field(fieldName: String) extends ScoreBoost {
    def boost(): String = {
      fieldName
    }
    def elasticBoost(): Pair[List[String],String] = {
      Pair(Nil,"doc['" + fieldName + "'].value")
    }
  }

  /**
   * Class representing a boost (multiplier) for matches occuring in the
   * given field
   * @see ScoreBoost
   */
  case class WeightedField(fieldName: String, weight: Double = 1) extends ScoreBoost {
    def boost(): String = {
      weight match {
        case 1.0 => fieldName
        case x: Double => fieldName + "^" + x.toString
      }
    }
    def elasticBoost(): Pair[List[String],String] = {
      weight match {
        case 1.0 => Pair(Nil,"doc['" + fieldName + "'].value")
        case _ => Pair(Nil,"(doc['" + fieldName + "'].value *" + weight.toString + ")")
      }
    }
  }

  /**
   * A phrase weighted field. Results in a document scoring boost
   * @param pf Traditional phrase query
   * @param pf2 In edismax type queries two word shingle matches
   * @param pf3 In edismax type queries three word shingle matches
   */
  case class PhraseWeightedField(fieldName: String, weight: Double = 1,
                                 pf: Boolean, pf2: Boolean, pf3: Boolean) {
    def extend(): String = {
      weight match {
        case 1.0 => fieldName
        case x: Double => fieldName + "^" + x.toString
      }
    }
  }

  /**
   * Abstarct Query class that provides an API for common query operations.
   * @define extend
   * @define and
   * @define boost
   * @define elasticExtend
   * @define elasticFilter
   */
  abstract class Query[T]() {
    /**
     * @return String Query's Solr query format  string representation
     */
    def extend(): String
    /**
     * Combine two queries using an AND
     * @see And
     */
    def and(c: Query[T]): Query[T] = And(this, c)
    /**
     * Combine two queries using an Or
     * @see Or
     */
    def or(c: Query[T]): Query[T] = Or(this, c)
    /**
     * Boost a query by a weight
     * @see Boost
     */
    def boost(weight: Float): Query[T] = Boost(this, weight)
    /**
     * @return String Query's elastic query string representation
     */
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField], mm: Option[String]): ElasticQueryBuilder
    def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      EFilterBuilders.queryFilter(this.elasticExtend(qf, Nil, None))
    }
  }

  /**
   * A class that provides and API for boosting the score of a field.
   */
  abstract class ScoreBoost {
    /**
     * Solr field boost function
     */
    def boost(): String
    /**
     * Elastic Search field boost function
     * The first param is a list of of params and
     * the second is a string to which scala format could be applied
     */
    def elasticBoost(): Pair[List[String],String]
  }

  /**
   * Name doesn't have to be a field name for Solr
   * it could be "lat,lng". However for ES it must be
   * a field
   */
  case class GeoDist(name: String, lat: Double, lng: Double, distType: String = "") extends ScoreBoost {
    /** @inheritdoc */
    def boost(): String = {
      distType match {
        case "square" => "sqedist(%s,%s,%s)".format(lat,lng,name)
        case _ => "dist(2,%s,%s,%s".format(lat,lng,name)
      }
    }
    /** @inheritdoc */
    def elasticBoost(): Pair[List[String],String] = {
      val distanceInKm = Pair(List(lat,lng).map(_.toString),"doc['"+name+"'].distanceInKm(%s,%s)")
      distType match {
        case "square" => Pair(distanceInKm._1,"pow(%s,2.0)".format(distanceInKm._2))
        case _ => distanceInKm
      }
    }
  }

  case class Recip(query: ScoreBoost, x: Int, y: Int, z: Int) extends ScoreBoost {
    /** @inheritdoc */
    def boost: String = "recip(%s,%d,%d,%d)".format(query.boost, x, y, z)
    /** @inheritdoc */
    def elasticBoost(): Pair[List[String],String] = {
      val subquery= query.elasticBoost()
      Pair(subquery._1,
           "%d.0*pow(((%d.0*(%s))+%d.0),-1.0)".format(y, x, subquery._2, z))
    }
  }

  /**
   * An empty Query.
   */
  case class Empty[T]() extends Query[T] {
    /** @inheritdoc */
    def extend(): String = "\"\""
    /** @inheritdoc */
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField], mm: Option[String]): ElasticQueryBuilder = {
      //An empty query matches no documents, so it is the same as the negation of matchAll
      //Note: this is kind of ugly since this is may likely an OR clause or negated up above
      //so we should try and avoid generating this
      val q = EQueryBuilders.boolQuery.mustNot(EQueryBuilders.matchAllQuery())
      q
    }
  }

  /**
   * A phrase containing a query that is optionally escaped.
   *
   * Represents a contiguous series of words to be matched in that order.
   */
  case class Phrase[T](query: T, escapeQuery: Boolean = true) extends Query[T] {
    /** @inheritdoc */
    def extend(): String = {
      if (escapeQuery) {
        '"' + escape(query.toString) + '"'
      } else {
        '"' + query.toString + '"'
      }
    }
    /** @inheritdoc */
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField], mm: Option[String]): ElasticQueryBuilder = {
      val q = EQueryBuilders.queryString(this.extend())
      qf.map(f => q.field(f.fieldName, f.weight.toFloat))
      q
    }
  }

  /**
   * A Phrase Prefix.
   * @see Phrase
   */
  case class PhrasePrefix[T](query: T, escapeQuery: Boolean = true) extends Query[T] {
    /** @inheritdoc */
    def extend(): String = {
      if (escapeQuery) {
        '"' + escape(query.toString) + '*' + '"'
      } else {
        '"' + query.toString + '*' + '"'
      }
    }
    /** @inheritdoc */
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField], mm: Option[String]): ElasticQueryBuilder = {
      val q = EQueryBuilders.disMaxQuery()
      q.tieBreaker(1)

      qf.map(f => {
        val basePhrase = EQueryBuilders.textPhraseQuery(f.fieldName, this.extend())
        val phraseQuery = f.weight match {
          case 1.0 => basePhrase
          case _ => basePhrase.boost(f.weight.toFloat)
        }
        q.add(phraseQuery)
      })
      q
    }
  }


  case class Range[T](q1: Query[T],q2: Query[T]) extends Query[T] {
    /** @inheritdoc */
    def extend(): String = {'[' + q1.extend() + " TO " + q2.extend() +']'}
    /** @inheritdoc */
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField], mm: Option[String]): ElasticQueryBuilder = {
      val q = EQueryBuilders.rangeQuery(qf.head.fieldName)
      q.from(q1)
      q.to(q2)
    }
    //By default we can just use the QueryFilterBuilder and the query extender
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      val q = EFilterBuilders.rangeFilter(qf.head.fieldName)
      q.from(q1)
      q.to(q2)
    }
  }

  /**
   * A class representing a Bag of words style query
   */
  case class BagOfWords[T](query: T, escapeQuery: Boolean = true) extends Query[T] {
    /** @inheritdoc */
    def extend(): String = {
      if (escapeQuery) {
        escape(query.toString)
      } else {
        query.toString
      }
    }

    /**
     * @inheritdoc
     *
     * We take a look at the list of fields being queried
     * and the list of phrase boost fields and generate
     * corresponding phrase boost queries.
     */
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField], mm: Option[String]): ElasticQueryBuilder = {
      val normalq = EQueryBuilders.queryString(this.extend())
      // If we are matching 100% then set operation to "and"
      mm match {
        case Some("100%") => normalq.defaultOperator(QueryStringQueryBuilder.Operator.AND)
        case _ => {}
      }
      // Update the normal query with the query fields' names and their weights
      qf.map(f => normalq.field(f.fieldName, f.weight.toFloat))
      val qfnames: Set[String] = qf.map(_.fieldName).toSet
      val queriesToGen = pf.filter(pwf => {qfnames contains pwf.fieldName})
      queriesToGen match {
        case Nil => normalq
        case _ => {
          val q = EQueryBuilders.disMaxQuery
          q.tieBreaker(1)
          q.add(normalq)
          queriesToGen.map(pwf => {
            val basePhrase = EQueryBuilders.textPhraseQuery(pwf.fieldName, this.extend())
            val phraseQuery = pwf.weight match {
              case 1 => basePhrase
              case _ => basePhrase.boost(pwf.weight.toFloat)
            }
            q.add(phraseQuery)
          })
          q
        }
      }
    }
  }

  /**
   * Class representing a semantic grouping of Queries
   */
  case class Group[T](items: Query[T]) extends Query[T] {
    /** @inheritdoc */
    def extend(): String = {"(%s)".format(items.extend)}
    /** @inheritdoc */
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField], mm: Option[String]): ElasticQueryBuilder = {
      items.elasticExtend(qf, pf, mm)
    }
    //By default we can just use the QueryFilterBuilder and the query extender
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      items.elasticFilter(qf)
    }
  }

  /**
   * Class representing clauses ANDed together
   */
  case class And[T](queries: Query[T]*) extends Query[T] {
    /** @inheritdoc */
    def extend(): String = {
      "(" + queries.map(c => c.extend()).mkString(" AND ") + ")"
    }
    /** @inheritdoc */
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField], mm: Option[String]): ElasticQueryBuilder = {
      val eqb = EQueryBuilders.boolQuery
      // Extend the queries and add them to the ElasticQueryBuilder
      queries.map(q => q.elasticExtend(qf, pf, mm)).map(qb => eqb.must(qb))
      eqb
    }
    //By default we can just use the QueryFilterBuilder and the query extender
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      EFilterBuilders.andFilter(queries.map(_.elasticFilter(qf)):_*)
    }
  }
  /**
   * Case class representing a list of clauses ORed together
   */
  case class Or[T](queries: Query[T]*) extends Query[T] {
    /** @inheritdoc */
    def extend(): String = {
      queries.map(c => c.extend()).mkString(" OR ")
    }
    /** @inheritdoc */
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField], mm: Option[String]): ElasticQueryBuilder = {
      val query = EQueryBuilders.boolQuery
      queries.map(q => q.elasticExtend(qf, pf, mm)).map(q => query.should(q))
      query
    }
    //By default we can just use the QueryFilterBuilder and the query extender
    override def elasticFilter(qf: List[WeightedField]): ElasticFilterBuilder = {
      EFilterBuilders.orFilter(queries.map(_.elasticFilter(qf)):_*)
    }
  }

  case class Splat[T]() extends Query[T] {
    /** @inheritdoc */
    def extend(): String = "*"
    /** @inheritdoc */
    //Is there a better way to do this?
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField], mm: Option[String]): ElasticQueryBuilder = {
      println("extending splat query with "+qf)
      //So we have a special case, if we are searching against the
      //"_all" field we can just construct a ninja query
      if (qf.map(_.fieldName).contains("_all")) {
        println("pandas")
        EQueryBuilders.matchAllQuery()
      } else {
        val q = EQueryBuilders.queryString(this.extend())
        qf.map(f => q.field(f.fieldName, f.weight.toFloat))
        q
      }
    }
  }

  /**
   * Class representing a Query boosted by a weight.
   */
  case class Boost[T](q: Query[T], weight: Float) extends Query[T] {
    /** @inheritdoc */
    def extend(): String = q.extend() + "^" + weight.toString
    /** @inheritdoc */
    def elasticExtend(qf: List[WeightedField], pf: List[PhraseWeightedField], mm: Option[String]): ElasticQueryBuilder = {
      val boostedQuery = EQueryBuilders.boostingQuery
      if (weight > 0) {
        boostedQuery.positive(q.elasticExtend(qf, pf, mm))
      } else {
        boostedQuery.negative(q.elasticExtend(qf, pf, mm))
      }
      boostedQuery.boost(weight.abs)
      boostedQuery
    }
  }
}
