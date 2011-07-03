package com.foursquare.solr
import Ast._
import com.foursquare.lib.GeoS2
import net.liftweb.record.{Record}

// Phantom types
abstract sealed class Ordered
abstract sealed class Unordered
abstract sealed class Limited
abstract sealed class Unlimited

case class QueryBuilder[M <: Record[M], Ord, Lim](
 meta: M with SolrSchema[M],
 clauses: List[Clause[_]],  // Like AndCondition in MongoHelpers
 filters: List[Clause[_]],
 boostQueries: List[Clause[_]],
 queryFields: List[String],
 phraseBoostFields: List[String],
 start: Option[Long],
 limit: Option[Long],
 sort: Option[String],
 queryType: Option[String]) {

  val GeoS2FieldName = "geo_s2_cell_ids"
  val DefaultLimit = 10
  val DefaultStart = 0
  import Helpers._

  def and[F](c: M => Clause[F]): QueryBuilder[M, Ord, Lim] = {
    QueryBuilder(meta, List(c(meta)), filters=Nil, boostQueries=Nil, queryFields=Nil, phraseBoostFields=phraseBoostFields, start=None, limit=None, sort=None, queryType=None)
  }

  def filter[F](f: M => Clause[F]): QueryBuilder[M, Ord, Lim] = {
    QueryBuilder(meta, clauses, f(meta) :: filters, boostQueries, queryFields, phraseBoostFields, start, limit, sort, queryType)
  }

  def limit(l: Int)(implicit ev: Lim =:= Unlimited): QueryBuilder[M, Ord, Limited] = {
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields, phraseBoostFields, start, Some(l), sort=None, queryType)
  }

  def orderAsc[F](f: M => SolrField[F, M])(implicit ev: Ord =:= Unordered): QueryBuilder[M, Ordered, Lim] = {
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields, phraseBoostFields, start, limit, sort=Some(f(meta).name + " asc"), queryType)
  }

  def orderDesc[F](f: M => SolrField[F, M])(implicit ev: Ord =:= Unordered): QueryBuilder[M, Ordered, Lim] = {
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields, phraseBoostFields, start, limit, sort=Some(f(meta).name + "desc"), queryType)
  }

  def geoRadiusFilter(geoLat: Double, geoLong: Double, radiusInMeters: Int, maxCells: Int = GeoS2.DefaultMaxCells): QueryBuilder[M, Ord, Lim] = {
    val cellIds = GeoS2.cover(geoLat, geoLong, radiusInMeters, maxCells=maxCells).map({x: com.google.common.geometry.S2CellId => Phrase(x.toToken)})
    val geoFilter = Clause(GeoS2FieldName, groupWithOr(cellIds))
    QueryBuilder(meta, clauses, geoFilter :: filters, boostQueries, queryFields, phraseBoostFields, start, limit, sort, queryType)
  }

  def test(): Unit = {
    val q = clauses.map(_.extend).mkString
    println("clauses: " + clauses.map(_.extend).mkString)
    println("filters: " + filters.map(_.extend).mkString)
    println("start: " + start)
    println("limit: " + limit)
    println("sort: " + sort)
    println(queryParams)
    ()
  }

  def queryParams(): Seq[(String,String)] = {
    val p = List(("q" -> clauses.map(_.extend).mkString(" ")),
                 ("start" -> (start.getOrElse {DefaultStart}).toString),
                 ("rows" -> (limit.getOrElse {DefaultLimit}).toString)
                 )

    val s = sort match {
      case None => Nil
      case Some(sort) => List("sort" -> sort)
    }
    val qt = queryType match {
      case None => Nil
      case Some(method) => List("defType" -> method)
    }
    val f = filters.map({x => ("fq" -> x.extend)})

     p ++ s ++ f
  }

  def fetch(l: Int)(implicit ev: Lim =:= Unlimited): Iterable[_] = {
    this.limit(l).fetch
  }
  def fetch(): Iterable[_] = {
    // Gross
    meta.meta.query(queryParams)
  }
}
