package com.foursquare.slashem
import com.twitter.util.{Duration, Future}
import com.foursquare.slashem.Ast._
import net.liftweb.record.{Record}
import java.util.{ArrayList, HashMap}
import collection.JavaConversions._
import java.util.concurrent.TimeUnit

// Phantom types
/** Used for an Ordered query */
abstract sealed class Ordered
/** Used for an Unordered query */
abstract sealed class Unordered
/** Used for a query with a specified limit */
abstract sealed class Limited
/** Used for a query without a specified limit */
abstract sealed class Unlimited
trait MinimumMatchType
abstract sealed class defaultMM extends MinimumMatchType
abstract sealed class customMM extends MinimumMatchType
abstract sealed class NoSelect
trait Highlighting
abstract sealed class NoHighlighting extends Highlighting
abstract sealed class YesHighlighting extends Highlighting
trait QualityFilter
abstract sealed class NoQualityFilter extends QualityFilter
abstract sealed class StrictQualityFilter extends QualityFilter

case class QueryBuilder[M <: Record[M], Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter](
 meta: M with SolrSchema[M],
 clauses: AbstractClause,  // Like AndCondition in MongoHelpers
 filters: List[AbstractClause],
 boostQueries: List[AbstractClause],
 queryFields: List[WeightedField],
 phraseBoostFields: List[PhraseWeightedField],
 boostFields: List[String],
 start: Option[Long],
 limit: Option[Long],
 tieBreaker: Option[Double],
 sort: Option[String],
 minimumMatch: Option[String],
 queryType: Option[String],
 fieldsToFetch: List[String],
 hls: Option[String],
 creator: Option[(HashMap[String,Any],HashMap[String,HashMap[String,ArrayList[String]]]) => Y],
 fallOf: Option[Double],
 min: Option[Int]) {

  val DefaultLimit = 10
  val DefaultStart = 0
  import Helpers._

  def and[F](c: M => Clause[F]): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] = {
    this.copy(meta=meta,clauses=JoinClause(c(meta),clauses,"AND"))
  }

  def or[F](c: M => Clause[F]): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] = {
    this.copy(meta=meta,clauses=JoinClause(c(meta),clauses,"OR"))
  }


  /** Filter the result set. Filter queries can be run in parallel from the main query and
  *have a separate cache. Filter queries are great for queries that are repeated often which
  *you want to constrain your result set by.
  * @param f The query to filter on */
  def filter[F](f: M => Clause[F]): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] = {
    this.copy(filters=f(meta)::filters)
  }

  /** A boostQuery affects the scoring of the results.
  @param f The boost query
  */
  def boostQuery[F](f: M => Clause[F]): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] = {
    this.copy(boostQueries=f(meta) :: boostQueries)
  }

   /** Helper method for case class extraction */
   private def getForField[F1,M <: Record[M]](f: SolrField[F1,M], fName: String, doc: HashMap[String,Any]): Option[F1] = {
     if (doc.containsKey(fName)) f.valueBoxFromAny(doc.get(fName)).toOption else None
   }

   /** Select into a case class */
   def selectCase [F1, CC](f: M => SolrField[F1, M], create: Option[F1] => CC)(implicit ev: (Y,H) =:= (NoSelect,NoHighlighting)): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {
     val f1Name: String = f(meta).name
     val f1Field: SolrField[F1, M] = f(meta)
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
         val f1 = getForField(f1Field, f1Name, doc)
         create(f1)}))
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType, (f1Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, CC](f: M => SolrField[F1, M], create: (Option[F1], List[String]) => CC)(implicit ev: (Y,H) =:= (NoSelect,YesHighlighting)): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {
     val f1Name: String = f(meta).name
     val f1Field: SolrField[F1, M] = f(meta)
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
       val matchingHighlights: Option[HashMap[String,ArrayList[String]]] = if (doc.contains("id") && highlighting != null
                                    && highlighting.contains(doc.get("id").toString)) {
                                      Some(highlighting.get(doc.get("id").toString))
                                    } else {
                                      None
                                    }
       val f1 = getForField(f1Field, f1Name, doc)
       val f1HL = matchingHighlights match {
         case Some(mhl) if (mhl.contains(f1Name)) => {
           mhl.get(f1Name).toList
         }
         case _ => Nil
       }
       create(f1,f1HL)}))
     QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                  phraseBoostFields, boostFields, start, limit, tieBreaker,
                  sort, minimumMatch, queryType, (f1Name::fieldsToFetch).distinct,
                  hls, transformer, fallOf, min)
  }


  /** Where you want to start fetching results back from
  * @param s Where you want to start fetching results from.  */
  def start(s: Int): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] = {
    this.copy(start=Some(s))
  }

  /** Limit the query to only fetch back l results.
  * Can only be applied to a query without an existing limit
  * @param l The limit */
  def limit(l: Int)(implicit ev: Lim =:= Unlimited): QueryBuilder[M, Ord, Limited, MM, Y, H, Q] = {
    this.copy(limit=Some(l))
  }

   /** Turn on highlighting. Must be done prior to select case
    */
   def highlighting()(implicit ev: (Y,H) =:= (NoSelect,NoHighlighting)): QueryBuilder[M, Ord, Lim, MM, Y, YesHighlighting, Q] = {
     this.copy(hls=Some("on"))
   }

   /** Turn on quality filtering.
    */
   def qualityFilter(f: Double,m: Int)(implicit ev: Q =:= NoQualityFilter): QueryBuilder[M, Ord, Lim, MM, Y, H, StrictQualityFilter] = {
     this.copy(fallOf=Some(f),min=Some(m))
   }


  /** In edismax the score is max({scores})+tieBreak*\sum{scores}) */
  def tieBreaker(t: Double): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] = {
    this.copy(tieBreaker=Some(t))
  }

  // Right now we only support ordering by field
  // TODO: Support ordering by function query
  /** Order the results by a specific field in ascending order.
   * Can only be applied to an unordered query.
   * @param f Field to order by */
  def orderAsc[F](f: M => SolrField[F, M])(implicit ev: Ord =:= Unordered): QueryBuilder[M, Ordered, Lim, MM, Y, H, Q] = {
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields, phraseBoostFields,
                 boostFields, start, limit, tieBreaker,
                 sort=Some(f(meta).name + " asc"), minimumMatch, queryType, fieldsToFetch,
                 hls, creator, fallOf, min)
  }

  /** Order the results by a specific field in descending order.
   * Can only be applied to an unordered query.
   * @param f Field to order by */
  def orderDesc[F](f: M => SolrField[F, M])(implicit ev: Ord =:= Unordered): QueryBuilder[M, Ordered, Lim, MM, Y, H, Q] = {
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields, phraseBoostFields, boostFields,
                 start, limit, tieBreaker, sort=Some(f(meta).name + " desc"),
                 minimumMatch, queryType, fieldsToFetch, hls, creator, fallOf, min)
  }

  /** If you doing a phrase search this the percent of terms that must match,
   * rounded down. So if you have it set to 50 and then do a search with 3
   * terms at least one term must match. A search of 4 however would require 2
   * terms to match.
   * You can only use one of minimumMatchAbsolute or minimumMatchPercent.
   * @param percent The minimum percent of tokens to match */
  def minimumMatchPercent(percent: Int)(implicit ev: MM =:= defaultMM): QueryBuilder[M, Ord, Lim, customMM, Y, H, Q] = {
    this.copy(minimumMatch=Some(percent.toString+"%"))
  }

  /**  If you doing a phrase search this the absolute # of terms that must
   * match. You can only use one of minimumMatchAbsolute or minimumMatchPercent.
   * to match. Note: You must chose one or the other.
   * @param count The minimum number of tokens to match
   */
  def minimumMatchAbsolute(count: Int)(implicit ev: MM =:= defaultMM): QueryBuilder[M, Ord, Lim, customMM, Y, H, Q] = {
    this.copy(minimumMatch=Some(count.toString))
  }
  /** Set the query type. This corresponds to the "defType" field.
   * Some sample values include "edismax" , "dismax" or just empty to use
   * the default query type
   * @param qt The query type */
  def useQueryType(qt: String): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] ={
    this.copy(queryType=Some(qt))
  }

  /** Depending on the query type you set, you can specify different fields to
  * be queried. This allows you to set a field and a boost. Fair warning:
  * If you set this value, it may be ignored (it is by the default solr
  * query parser)
  * @param f The field to query
  * @param boost The (optional) amount to boost the query weight for the provided field */
  def queryField[F](f: M => SolrField[F,M], boost: Double = 1): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] ={
    this.copy(queryFields=WeightedField(f(meta).name,boost)::queryFields)
  }

  /** Same as queryField but takes a list of fields.
  * @param fs A list of fields to query
  * @param boost The (optional) amount to boost the query weight for the provided field */
  def queryFields(fs: List[M => SolrField[_,M]], boost: Double = 1): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] ={
    this.copy(queryFields=fs.map(f => WeightedField(f(meta).name,boost))++queryFields)
  }

  /** Certain query parsers allow you to set a phraseBoost field. Generally
   * these are only run on the returned documents. So if I want to return all
   * documents matching either coffee or shop but I want documents with
   * "coffee shop" to score higher I would set this. The params for pf,pf2,and
   * pf3 control what type of phrase boost query to generate. In edismax
   * pf2/pf3 results in a query which will match shingled phrase queries of
   * length 2 & 3 respectively. For example pf2=true in edismax and a query
   * of "delicious coffee shops" would boost documents containing
   * "delicious coffee" and "coffee shops".
   * @param f The field to boost phrase matches in
   * @param boost The (optional) boost value
   * @param pf Enable/disable full phrase boosting
   * @param pf2 Enable/disable 2-word shingle phrase boosting
   * @param pf3 Enable/disable 3-word shingle phrase boosting */
  def phraseBoost[F](f: M => SolrField[F,M], boost: Double = 1, pf: Boolean = true, pf2: Boolean = true, pf3: Boolean = true): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] ={
    this.copy(phraseBoostFields=PhraseWeightedField(f(meta).name,boost,pf,pf2,pf3)::phraseBoostFields)
  }

  /** Specify a field to be retrieved. If you want to get back all fields you
   * can use a field of name "*"
   * @param f Field to be retrieved */
  def fetchField[F](f: M => SolrField[F,M]): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] = {
    this.copy(fieldsToFetch=f(meta).name::fieldsToFetch)
  }

  /** Same as fetchField but takes multiple fields
  * @param fs List of fields to be retrieved */
  def fetchFields(fs: (M => SolrField[_,M])*): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] = {
    this.copy(fieldsToFetch=fs.map(f=> f(meta).name).toList++fieldsToFetch)
  }

  /** Boost a specific field/query. WARNING: NOT TYPE SAFE NO VALIDATION ETC. */
  def boostField(s: String): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] = {
    this.copy(boostFields=s::boostFields)
  }

  /** Boost a field (type safe version) */
  def boostField[F](f: M => SolrField[F,M], boost: Double = 1): QueryBuilder[M, Ord, Lim, MM, Y, H, Q] = {
    this.copy(boostFields=(f(meta).name+"^"+boost)::boostFields)
  }

  //Print out some debugging information.
  def test(): Unit = {
    println("clauses: " + clauses.extend)
    println("filters: " + filters.map(_.extend).mkString)
    println("start: " + start)
    println("limit: " + limit)
    println("sort: " + sort)
    println(queryParams)
    ()
  }

  def queryParams(): Seq[(String, String)] = queryParamsWithBounds(this.start, this.limit)

  def queryParamsWithBounds(qstart: Option[Long], qrows: Option[Long]): Seq[(String,String)] = {
    val bounds = List(("start" -> (qstart.getOrElse {DefaultStart}).toString),
                 ("rows" -> (qrows.getOrElse {DefaultLimit}).toString))
    bounds ++ queryParamsNoBounds()
  }

  //This is the part which generates most of the solr request
  def queryParamsNoBounds(): Seq[(String,String)] = {
    val p = List(("q" -> clauses.extend))

    val s = sort match {
      case None => Nil
      case Some(sort) => List("sort" -> sort)
    }
    val qt = queryType match {
      case None => Nil
      case Some(method) => List("defType" -> method)
    }
    val mm = minimumMatch match {
      case None => Nil
      case Some(mmParam) => List("mm" -> mmParam)
    }

    val bq = boostQueries.map({ x => ("bq" -> x.extend)})

    val qf = queryFields.filter({x => x.boost != 0}).map({x => ("qf" -> x.extend)})

    val pf = phraseBoostFields.filter(x => x.pf).map({x => ("pf" -> x.extend)})++phraseBoostFields.filter(x => x.pf2).map({x => ("pf2" -> x.extend)})++
             phraseBoostFields.filter(x => x.pf3).map({x => ("pf3" -> x.extend)})

    val fl = fieldsToFetch match {
      case Nil => Nil
      case x => List("fl" -> (x.mkString(",")))
    }

    val t = tieBreaker match {
      case None => Nil
      case Some(x) => List("tieBreaker" -> x.toString)
    }

    val hlp = hls match {
      case None => Nil
      case Some(a) => List("hl" -> a)
    }

    val bf = boostFields.map({x => ("bf" -> x)})

    val f = filters.map({x => ("fq" -> x.extend)})

     t ++ mm ++ qt ++ bq ++ qf ++ p ++ s ++ f ++ pf ++ fl ++ bf ++ hlp
  }

  /** Fetch the results with the limit of l. Can only be used on an unlimited
  * query */
  def fetch(l: Int)(implicit ev: Lim =:= Unlimited): SearchResults[M,Y] = {
    this.limit(l).fetch()
  }

  /** Fetch the results for a given query (blocking)*/
  def fetch():  SearchResults[M,Y] = {
    // Gross++
    fetch(Duration(1, TimeUnit.SECONDS))
  }
  /** Fetch the results for a given query (blocking) with a specified timeout*/
  def fetch(timeout: Duration):  SearchResults[M,Y] = {
    // Gross++
    (meta.queryFuture(creator, queryParams, fieldsToFetch, fallOf, min)(timeout))
  }
  /** Fetch the results for a given query (non-blocking)*/
  def fetchFuture(): Future[SearchResults[M,Y]] = {
    meta.queryFuture(creator, queryParams, fieldsToFetch, fallOf, min)
  }
  /** Call fetchBatch when you need a large number of results from SOLR.
   * Usage example: val res = (SVenue where (_.default eqs "coffee") start(10) limit(40) fetchBatch(10)) {_.response.oids }
   * @param batchSize The size of the batch to be retrieved
   * @param f A function to be applied over the result batches */
  def fetchBatch[T](batchSize: Int, timeout: Duration = Duration(1, TimeUnit.SECONDS))(f: SearchResults[M,Y] => List[T]): List[T] = {
    val startPos: Long = this.start.getOrElse(DefaultStart)
    val maxRowsToGet: Option[Long] = this.limit//If not specified try to get all rows
    //There is somewhat of a race condition here. If data is being inserted or deleted during the query
    //some results may not appear and some results may be duplicated.
    val firstQuery = meta.query(timeout,
                                creator,
                                queryParamsWithBounds(Option(startPos), Option(batchSize)),
                                fieldsToFetch,
                                fallOf,
                                min)
    val maxResults = firstQuery.response.numFound - firstQuery.response.start
    val rowsToGet: Long = maxRowsToGet.map(scala.math.min(_,maxResults)) getOrElse maxResults
    // Now make rowsToGet/batchSizes calls to meta.query
    //Note the 1 is not a typo since we have already fetched the first page.
    f(firstQuery)++(1 to scala.math.ceil(rowsToGet*1.0/batchSize).toInt).flatMap{i =>
      // cannot simply override this.start as it is a val, so removing/adding on queryParams
      val starti = startPos + (i*batchSize)
      f(meta.query(timeout,
                   creator,
                   queryParamsWithBounds(Option(starti), Option(batchSize)),
                   fieldsToFetch,
                   fallOf,
                   min))
    }.toList
  }
   //Auto generated code, is there a better way to do this?

   /** Select into a case class */
   def selectCase [F1, F2,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M], create: (Option[F1], List[String] ,Option[F2], List[String]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {

       val matchingHighlights: Option[HashMap[String,ArrayList[String]]] = if (doc.contains("id") && highlighting != null
                                    && highlighting.contains(doc.get("id").toString)) {
                                      Some(highlighting.get(doc.get("id").toString))
                                    } else {
                                      None
                                    }
     val f1 = getForField(f1Field, f1Name, doc)
     val f1HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f1Name)) => {
           mhl.get(f1Name).toList
         }
         case _ => Nil
       }
     val f2 = getForField(f2Field, f2Name, doc)
     val f2HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f2Name)) => {
           mhl.get(f2Name).toList
         }
         case _ => Nil
       }
     create(f1, f1HL , f2 , f2HL)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M], create: (Option[F1], Option[F2]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     create(f1 , f2)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M], create: (Option[F1], List[String] ,Option[F2], List[String] ,Option[F3], List[String]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {

       val matchingHighlights: Option[HashMap[String,ArrayList[String]]] = if (doc.contains("id") && highlighting != null
                                    && highlighting.contains(doc.get("id").toString)) {
                                      Some(highlighting.get(doc.get("id").toString))
                                    } else {
                                      None
                                    }
     val f1 = getForField(f1Field, f1Name, doc)
     val f1HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f1Name)) => {
           mhl.get(f1Name).toList
         }
         case _ => Nil
       }
     val f2 = getForField(f2Field, f2Name, doc)
     val f2HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f2Name)) => {
           mhl.get(f2Name).toList
         }
         case _ => Nil
       }
     val f3 = getForField(f3Field, f3Name, doc)
     val f3HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f3Name)) => {
           mhl.get(f3Name).toList
         }
         case _ => Nil
       }
     create(f1, f1HL , f2 , f2HL , f3 , f3HL)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M], create: (Option[F1], Option[F2], Option[F3]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     create(f1 , f2 , f3)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M], create: (Option[F1], List[String] ,Option[F2], List[String] ,Option[F3], List[String] ,Option[F4], List[String]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {

       val matchingHighlights: Option[HashMap[String,ArrayList[String]]] = if (doc.contains("id") && highlighting != null
                                    && highlighting.contains(doc.get("id").toString)) {
                                      Some(highlighting.get(doc.get("id").toString))
                                    } else {
                                      None
                                    }
     val f1 = getForField(f1Field, f1Name, doc)
     val f1HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f1Name)) => {
           mhl.get(f1Name).toList
         }
         case _ => Nil
       }
     val f2 = getForField(f2Field, f2Name, doc)
     val f2HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f2Name)) => {
           mhl.get(f2Name).toList
         }
         case _ => Nil
       }
     val f3 = getForField(f3Field, f3Name, doc)
     val f3HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f3Name)) => {
           mhl.get(f3Name).toList
         }
         case _ => Nil
       }
     val f4 = getForField(f4Field, f4Name, doc)
     val f4HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f4Name)) => {
           mhl.get(f4Name).toList
         }
         case _ => Nil
       }
     create(f1, f1HL , f2 , f2HL , f3 , f3HL , f4 , f4HL)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M], create: (Option[F1], Option[F2], Option[F3], Option[F4]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     create(f1 , f2 , f3 , f4)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M], create: (Option[F1], List[String] ,Option[F2], List[String] ,Option[F3], List[String] ,Option[F4], List[String] ,Option[F5], List[String]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {

       val matchingHighlights: Option[HashMap[String,ArrayList[String]]] = if (doc.contains("id") && highlighting != null
                                    && highlighting.contains(doc.get("id").toString)) {
                                      Some(highlighting.get(doc.get("id").toString))
                                    } else {
                                      None
                                    }
     val f1 = getForField(f1Field, f1Name, doc)
     val f1HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f1Name)) => {
           mhl.get(f1Name).toList
         }
         case _ => Nil
       }
     val f2 = getForField(f2Field, f2Name, doc)
     val f2HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f2Name)) => {
           mhl.get(f2Name).toList
         }
         case _ => Nil
       }
     val f3 = getForField(f3Field, f3Name, doc)
     val f3HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f3Name)) => {
           mhl.get(f3Name).toList
         }
         case _ => Nil
       }
     val f4 = getForField(f4Field, f4Name, doc)
     val f4HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f4Name)) => {
           mhl.get(f4Name).toList
         }
         case _ => Nil
       }
     val f5 = getForField(f5Field, f5Name, doc)
     val f5HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f5Name)) => {
           mhl.get(f5Name).toList
         }
         case _ => Nil
       }
     create(f1, f1HL , f2 , f2HL , f3 , f3HL , f4 , f4HL , f5 , f5HL)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     create(f1 , f2 , f3 , f4 , f5)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M], create: (Option[F1], List[String] ,Option[F2], List[String] ,Option[F3], List[String] ,Option[F4], List[String] ,Option[F5], List[String] ,Option[F6], List[String]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {

       val matchingHighlights: Option[HashMap[String,ArrayList[String]]] = if (doc.contains("id") && highlighting != null
                                    && highlighting.contains(doc.get("id").toString)) {
                                      Some(highlighting.get(doc.get("id").toString))
                                    } else {
                                      None
                                    }
     val f1 = getForField(f1Field, f1Name, doc)
     val f1HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f1Name)) => {
           mhl.get(f1Name).toList
         }
         case _ => Nil
       }
     val f2 = getForField(f2Field, f2Name, doc)
     val f2HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f2Name)) => {
           mhl.get(f2Name).toList
         }
         case _ => Nil
       }
     val f3 = getForField(f3Field, f3Name, doc)
     val f3HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f3Name)) => {
           mhl.get(f3Name).toList
         }
         case _ => Nil
       }
     val f4 = getForField(f4Field, f4Name, doc)
     val f4HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f4Name)) => {
           mhl.get(f4Name).toList
         }
         case _ => Nil
       }
     val f5 = getForField(f5Field, f5Name, doc)
     val f5HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f5Name)) => {
           mhl.get(f5Name).toList
         }
         case _ => Nil
       }
     val f6 = getForField(f6Field, f6Name, doc)
     val f6HL =  matchingHighlights match {
         case Some(mhl) if (mhl.contains(f6Name)) => {
           mhl.get(f6Name).toList
         }
         case _ => Nil
       }
     create(f1, f1HL , f2 , f2HL , f3 , f3HL , f4 , f4HL , f5 , f5HL , f6 , f6HL)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5], Option[F6]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     val f6 = getForField(f6Field, f6Name, doc)
     create(f1 , f2 , f3 , f4 , f5 , f6)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6, F7,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M],f7: M => SolrField[F7, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5], Option[F6], Option[F7]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name

     val f7Field: SolrField[F7, M] = f7(meta)
     val f7Name: String = f7Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     val f6 = getForField(f6Field, f6Name, doc)
     val f7 = getForField(f7Field, f7Name, doc)
     create(f1 , f2 , f3 , f4 , f5 , f6 , f7)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::f7Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6, F7, F8,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M],f7: M => SolrField[F7, M],f8: M => SolrField[F8, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5], Option[F6], Option[F7], Option[F8]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name

     val f7Field: SolrField[F7, M] = f7(meta)
     val f7Name: String = f7Field.name

     val f8Field: SolrField[F8, M] = f8(meta)
     val f8Name: String = f8Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     val f6 = getForField(f6Field, f6Name, doc)
     val f7 = getForField(f7Field, f7Name, doc)
     val f8 = getForField(f8Field, f8Name, doc)
     create(f1 , f2 , f3 , f4 , f5 , f6 , f7 , f8)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::f7Name::f8Name::
                 fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6, F7, F8, F9,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M],f7: M => SolrField[F7, M],f8: M => SolrField[F8, M],f9: M => SolrField[F9, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5], Option[F6], Option[F7], Option[F8], Option[F9]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name

     val f7Field: SolrField[F7, M] = f7(meta)
     val f7Name: String = f7Field.name

     val f8Field: SolrField[F8, M] = f8(meta)
     val f8Name: String = f8Field.name

     val f9Field: SolrField[F9, M] = f9(meta)
     val f9Name: String = f9Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     val f6 = getForField(f6Field, f6Name, doc)
     val f7 = getForField(f7Field, f7Name, doc)
     val f8 = getForField(f8Field, f8Name, doc)
     val f9 = getForField(f9Field, f9Name, doc)
     create(f1 , f2 , f3 , f4 , f5 , f6 , f7 , f8 , f9)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::f7Name::f8Name::
                 f9Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M],f7: M => SolrField[F7, M],f8: M => SolrField[F8, M],f9: M => SolrField[F9, M],f10: M => SolrField[F10, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5], Option[F6], Option[F7], Option[F8], Option[F9], Option[F10]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name

     val f7Field: SolrField[F7, M] = f7(meta)
     val f7Name: String = f7Field.name

     val f8Field: SolrField[F8, M] = f8(meta)
     val f8Name: String = f8Field.name

     val f9Field: SolrField[F9, M] = f9(meta)
     val f9Name: String = f9Field.name

     val f10Field: SolrField[F10, M] = f10(meta)
     val f10Name: String = f10Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     val f6 = getForField(f6Field, f6Name, doc)
     val f7 = getForField(f7Field, f7Name, doc)
     val f8 = getForField(f8Field, f8Name, doc)
     val f9 = getForField(f9Field, f9Name, doc)
     val f10 = getForField(f10Field, f10Name, doc)
     create(f1 , f2 , f3 , f4 , f5 , f6 , f7 , f8 , f9 , f10)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::f7Name::f8Name::
                 f9Name::f10Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M],f7: M => SolrField[F7, M],f8: M => SolrField[F8, M],f9: M => SolrField[F9, M],f10: M => SolrField[F10, M],f11: M => SolrField[F11, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5], Option[F6], Option[F7], Option[F8], Option[F9], Option[F10], Option[F11]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name

     val f7Field: SolrField[F7, M] = f7(meta)
     val f7Name: String = f7Field.name

     val f8Field: SolrField[F8, M] = f8(meta)
     val f8Name: String = f8Field.name

     val f9Field: SolrField[F9, M] = f9(meta)
     val f9Name: String = f9Field.name

     val f10Field: SolrField[F10, M] = f10(meta)
     val f10Name: String = f10Field.name

     val f11Field: SolrField[F11, M] = f11(meta)
     val f11Name: String = f11Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     val f6 = getForField(f6Field, f6Name, doc)
     val f7 = getForField(f7Field, f7Name, doc)
     val f8 = getForField(f8Field, f8Name, doc)
     val f9 = getForField(f9Field, f9Name, doc)
     val f10 = getForField(f10Field, f10Name, doc)
     val f11 = getForField(f11Field, f11Name, doc)
     create(f1 , f2 , f3 , f4 , f5 , f6 , f7 , f8 , f9 , f10 , f11)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::f7Name::f8Name::
                 f9Name::f10Name::f11Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M],f7: M => SolrField[F7, M],f8: M => SolrField[F8, M],f9: M => SolrField[F9, M],f10: M => SolrField[F10, M],f11: M => SolrField[F11, M],f12: M => SolrField[F12, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5], Option[F6], Option[F7], Option[F8], Option[F9], Option[F10], Option[F11], Option[F12]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name

     val f7Field: SolrField[F7, M] = f7(meta)
     val f7Name: String = f7Field.name

     val f8Field: SolrField[F8, M] = f8(meta)
     val f8Name: String = f8Field.name

     val f9Field: SolrField[F9, M] = f9(meta)
     val f9Name: String = f9Field.name

     val f10Field: SolrField[F10, M] = f10(meta)
     val f10Name: String = f10Field.name

     val f11Field: SolrField[F11, M] = f11(meta)
     val f11Name: String = f11Field.name

     val f12Field: SolrField[F12, M] = f12(meta)
     val f12Name: String = f12Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     val f6 = getForField(f6Field, f6Name, doc)
     val f7 = getForField(f7Field, f7Name, doc)
     val f8 = getForField(f8Field, f8Name, doc)
     val f9 = getForField(f9Field, f9Name, doc)
     val f10 = getForField(f10Field, f10Name, doc)
     val f11 = getForField(f11Field, f11Name, doc)
     val f12 = getForField(f12Field, f12Name, doc)
     create(f1 , f2 , f3 , f4 , f5 , f6 , f7 , f8 , f9 , f10 , f11 , f12)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::f7Name::f8Name::
                 f9Name::f10Name::f11Name::f12Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M],f7: M => SolrField[F7, M],f8: M => SolrField[F8, M],f9: M => SolrField[F9, M],f10: M => SolrField[F10, M],f11: M => SolrField[F11, M],f12: M => SolrField[F12, M],f13: M => SolrField[F13, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5], Option[F6], Option[F7], Option[F8], Option[F9], Option[F10], Option[F11], Option[F12], Option[F13]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name

     val f7Field: SolrField[F7, M] = f7(meta)
     val f7Name: String = f7Field.name

     val f8Field: SolrField[F8, M] = f8(meta)
     val f8Name: String = f8Field.name

     val f9Field: SolrField[F9, M] = f9(meta)
     val f9Name: String = f9Field.name

     val f10Field: SolrField[F10, M] = f10(meta)
     val f10Name: String = f10Field.name

     val f11Field: SolrField[F11, M] = f11(meta)
     val f11Name: String = f11Field.name

     val f12Field: SolrField[F12, M] = f12(meta)
     val f12Name: String = f12Field.name

     val f13Field: SolrField[F13, M] = f13(meta)
     val f13Name: String = f13Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     val f6 = getForField(f6Field, f6Name, doc)
     val f7 = getForField(f7Field, f7Name, doc)
     val f8 = getForField(f8Field, f8Name, doc)
     val f9 = getForField(f9Field, f9Name, doc)
     val f10 = getForField(f10Field, f10Name, doc)
     val f11 = getForField(f11Field, f11Name, doc)
     val f12 = getForField(f12Field, f12Name, doc)
     val f13 = getForField(f13Field, f13Name, doc)
     create(f1 , f2 , f3 , f4 , f5 , f6 , f7 , f8 , f9 , f10 , f11 , f12 , f13)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::f7Name::f8Name::
                 f9Name::f10Name::f11Name::f12Name::f13Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M],f7: M => SolrField[F7, M],f8: M => SolrField[F8, M],f9: M => SolrField[F9, M],f10: M => SolrField[F10, M],f11: M => SolrField[F11, M],f12: M => SolrField[F12, M],f13: M => SolrField[F13, M],f14: M => SolrField[F14, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5], Option[F6], Option[F7], Option[F8], Option[F9], Option[F10], Option[F11], Option[F12], Option[F13], Option[F14]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name

     val f7Field: SolrField[F7, M] = f7(meta)
     val f7Name: String = f7Field.name

     val f8Field: SolrField[F8, M] = f8(meta)
     val f8Name: String = f8Field.name

     val f9Field: SolrField[F9, M] = f9(meta)
     val f9Name: String = f9Field.name

     val f10Field: SolrField[F10, M] = f10(meta)
     val f10Name: String = f10Field.name

     val f11Field: SolrField[F11, M] = f11(meta)
     val f11Name: String = f11Field.name

     val f12Field: SolrField[F12, M] = f12(meta)
     val f12Name: String = f12Field.name

     val f13Field: SolrField[F13, M] = f13(meta)
     val f13Name: String = f13Field.name

     val f14Field: SolrField[F14, M] = f14(meta)
     val f14Name: String = f14Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     val f6 = getForField(f6Field, f6Name, doc)
     val f7 = getForField(f7Field, f7Name, doc)
     val f8 = getForField(f8Field, f8Name, doc)
     val f9 = getForField(f9Field, f9Name, doc)
     val f10 = getForField(f10Field, f10Name, doc)
     val f11 = getForField(f11Field, f11Name, doc)
     val f12 = getForField(f12Field, f12Name, doc)
     val f13 = getForField(f13Field, f13Name, doc)
     val f14 = getForField(f14Field, f14Name, doc)
     create(f1 , f2 , f3 , f4 , f5 , f6 , f7 , f8 , f9 , f10 , f11 , f12 , f13 , f14)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::f7Name::f8Name::
                 f9Name::f10Name::f11Name::f12Name::f13Name::f14Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M],f7: M => SolrField[F7, M],f8: M => SolrField[F8, M],f9: M => SolrField[F9, M],f10: M => SolrField[F10, M],f11: M => SolrField[F11, M],f12: M => SolrField[F12, M],f13: M => SolrField[F13, M],f14: M => SolrField[F14, M],f15: M => SolrField[F15, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5], Option[F6], Option[F7], Option[F8], Option[F9], Option[F10], Option[F11], Option[F12], Option[F13], Option[F14], Option[F15]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name

     val f7Field: SolrField[F7, M] = f7(meta)
     val f7Name: String = f7Field.name

     val f8Field: SolrField[F8, M] = f8(meta)
     val f8Name: String = f8Field.name

     val f9Field: SolrField[F9, M] = f9(meta)
     val f9Name: String = f9Field.name

     val f10Field: SolrField[F10, M] = f10(meta)
     val f10Name: String = f10Field.name

     val f11Field: SolrField[F11, M] = f11(meta)
     val f11Name: String = f11Field.name

     val f12Field: SolrField[F12, M] = f12(meta)
     val f12Name: String = f12Field.name

     val f13Field: SolrField[F13, M] = f13(meta)
     val f13Name: String = f13Field.name

     val f14Field: SolrField[F14, M] = f14(meta)
     val f14Name: String = f14Field.name

     val f15Field: SolrField[F15, M] = f15(meta)
     val f15Name: String = f15Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     val f6 = getForField(f6Field, f6Name, doc)
     val f7 = getForField(f7Field, f7Name, doc)
     val f8 = getForField(f8Field, f8Name, doc)
     val f9 = getForField(f9Field, f9Name, doc)
     val f10 = getForField(f10Field, f10Name, doc)
     val f11 = getForField(f11Field, f11Name, doc)
     val f12 = getForField(f12Field, f12Name, doc)
     val f13 = getForField(f13Field, f13Name, doc)
     val f14 = getForField(f14Field, f14Name, doc)
     val f15 = getForField(f15Field, f15Name, doc)
     create(f1 , f2 , f3 , f4 , f5 , f6 , f7 , f8 , f9 , f10 , f11 , f12 , f13 , f14 , f15)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::f7Name::f8Name::
                 f9Name::f10Name::f11Name::f12Name::f13Name::f14Name::f15Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M],f7: M => SolrField[F7, M],f8: M => SolrField[F8, M],f9: M => SolrField[F9, M],f10: M => SolrField[F10, M],f11: M => SolrField[F11, M],f12: M => SolrField[F12, M],f13: M => SolrField[F13, M],f14: M => SolrField[F14, M],f15: M => SolrField[F15, M],f16: M => SolrField[F16, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5], Option[F6], Option[F7], Option[F8], Option[F9], Option[F10], Option[F11], Option[F12], Option[F13], Option[F14], Option[F15], Option[F16]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name

     val f7Field: SolrField[F7, M] = f7(meta)
     val f7Name: String = f7Field.name

     val f8Field: SolrField[F8, M] = f8(meta)
     val f8Name: String = f8Field.name

     val f9Field: SolrField[F9, M] = f9(meta)
     val f9Name: String = f9Field.name

     val f10Field: SolrField[F10, M] = f10(meta)
     val f10Name: String = f10Field.name

     val f11Field: SolrField[F11, M] = f11(meta)
     val f11Name: String = f11Field.name

     val f12Field: SolrField[F12, M] = f12(meta)
     val f12Name: String = f12Field.name

     val f13Field: SolrField[F13, M] = f13(meta)
     val f13Name: String = f13Field.name

     val f14Field: SolrField[F14, M] = f14(meta)
     val f14Name: String = f14Field.name

     val f15Field: SolrField[F15, M] = f15(meta)
     val f15Name: String = f15Field.name

     val f16Field: SolrField[F16, M] = f16(meta)
     val f16Name: String = f16Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     val f6 = getForField(f6Field, f6Name, doc)
     val f7 = getForField(f7Field, f7Name, doc)
     val f8 = getForField(f8Field, f8Name, doc)
     val f9 = getForField(f9Field, f9Name, doc)
     val f10 = getForField(f10Field, f10Name, doc)
     val f11 = getForField(f11Field, f11Name, doc)
     val f12 = getForField(f12Field, f12Name, doc)
     val f13 = getForField(f13Field, f13Name, doc)
     val f14 = getForField(f14Field, f14Name, doc)
     val f15 = getForField(f15Field, f15Name, doc)
     val f16 = getForField(f16Field, f16Name, doc)
     create(f1 , f2 , f3 , f4 , f5 , f6 , f7 , f8 , f9 , f10 , f11 , f12 , f13 , f14 , f15 , f16)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::f7Name::f8Name::
                 f9Name::f10Name::f11Name::f12Name::f13Name::f14Name::f15Name::f16Name::
                 fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16, F17,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M],f7: M => SolrField[F7, M],f8: M => SolrField[F8, M],f9: M => SolrField[F9, M],f10: M => SolrField[F10, M],f11: M => SolrField[F11, M],f12: M => SolrField[F12, M],f13: M => SolrField[F13, M],f14: M => SolrField[F14, M],f15: M => SolrField[F15, M],f16: M => SolrField[F16, M],f17: M => SolrField[F17, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5], Option[F6], Option[F7], Option[F8], Option[F9], Option[F10], Option[F11], Option[F12], Option[F13], Option[F14], Option[F15], Option[F16], Option[F17]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name

     val f7Field: SolrField[F7, M] = f7(meta)
     val f7Name: String = f7Field.name

     val f8Field: SolrField[F8, M] = f8(meta)
     val f8Name: String = f8Field.name

     val f9Field: SolrField[F9, M] = f9(meta)
     val f9Name: String = f9Field.name

     val f10Field: SolrField[F10, M] = f10(meta)
     val f10Name: String = f10Field.name

     val f11Field: SolrField[F11, M] = f11(meta)
     val f11Name: String = f11Field.name

     val f12Field: SolrField[F12, M] = f12(meta)
     val f12Name: String = f12Field.name

     val f13Field: SolrField[F13, M] = f13(meta)
     val f13Name: String = f13Field.name

     val f14Field: SolrField[F14, M] = f14(meta)
     val f14Name: String = f14Field.name

     val f15Field: SolrField[F15, M] = f15(meta)
     val f15Name: String = f15Field.name

     val f16Field: SolrField[F16, M] = f16(meta)
     val f16Name: String = f16Field.name

     val f17Field: SolrField[F17, M] = f17(meta)
     val f17Name: String = f17Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     val f6 = getForField(f6Field, f6Name, doc)
     val f7 = getForField(f7Field, f7Name, doc)
     val f8 = getForField(f8Field, f8Name, doc)
     val f9 = getForField(f9Field, f9Name, doc)
     val f10 = getForField(f10Field, f10Name, doc)
     val f11 = getForField(f11Field, f11Name, doc)
     val f12 = getForField(f12Field, f12Name, doc)
     val f13 = getForField(f13Field, f13Name, doc)
     val f14 = getForField(f14Field, f14Name, doc)
     val f15 = getForField(f15Field, f15Name, doc)
     val f16 = getForField(f16Field, f16Name, doc)
     val f17 = getForField(f17Field, f17Name, doc)
     create(f1 , f2 , f3 , f4 , f5 , f6 , f7 , f8 , f9 , f10 , f11 , f12 , f13 , f14 , f15 , f16 , f17)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::f7Name::f8Name::
                 f9Name::f10Name::f11Name::f12Name::f13Name::f14Name::f15Name::f16Name::
                 f17Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }
   /** Select into a case class */
   def selectCase [F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, F13, F14, F15, F16, F17, F18,  CC](f1: M => SolrField[F1, M],f2: M => SolrField[F2, M],f3: M => SolrField[F3, M],f4: M => SolrField[F4, M],f5: M => SolrField[F5, M],f6: M => SolrField[F6, M],f7: M => SolrField[F7, M],f8: M => SolrField[F8, M],f9: M => SolrField[F9, M],f10: M => SolrField[F10, M],f11: M => SolrField[F11, M],f12: M => SolrField[F12, M],f13: M => SolrField[F13, M],f14: M => SolrField[F14, M],f15: M => SolrField[F15, M],f16: M => SolrField[F16, M],f17: M => SolrField[F17, M],f18: M => SolrField[F18, M], create: (Option[F1], Option[F2], Option[F3], Option[F4], Option[F5], Option[F6], Option[F7], Option[F8], Option[F9], Option[F10], Option[F11], Option[F12], Option[F13], Option[F14], Option[F15], Option[F16], Option[F17], Option[F18]) => CC)(implicit ev: Y =:= NoSelect): QueryBuilder[M, Ord, Lim, MM, CC, H, Q] = {

     val f1Field: SolrField[F1, M] = f1(meta)
     val f1Name: String = f1Field.name

     val f2Field: SolrField[F2, M] = f2(meta)
     val f2Name: String = f2Field.name

     val f3Field: SolrField[F3, M] = f3(meta)
     val f3Name: String = f3Field.name

     val f4Field: SolrField[F4, M] = f4(meta)
     val f4Name: String = f4Field.name

     val f5Field: SolrField[F5, M] = f5(meta)
     val f5Name: String = f5Field.name

     val f6Field: SolrField[F6, M] = f6(meta)
     val f6Name: String = f6Field.name

     val f7Field: SolrField[F7, M] = f7(meta)
     val f7Name: String = f7Field.name

     val f8Field: SolrField[F8, M] = f8(meta)
     val f8Name: String = f8Field.name

     val f9Field: SolrField[F9, M] = f9(meta)
     val f9Name: String = f9Field.name

     val f10Field: SolrField[F10, M] = f10(meta)
     val f10Name: String = f10Field.name

     val f11Field: SolrField[F11, M] = f11(meta)
     val f11Name: String = f11Field.name

     val f12Field: SolrField[F12, M] = f12(meta)
     val f12Name: String = f12Field.name

     val f13Field: SolrField[F13, M] = f13(meta)
     val f13Name: String = f13Field.name

     val f14Field: SolrField[F14, M] = f14(meta)
     val f14Name: String = f14Field.name

     val f15Field: SolrField[F15, M] = f15(meta)
     val f15Name: String = f15Field.name

     val f16Field: SolrField[F16, M] = f16(meta)
     val f16Name: String = f16Field.name

     val f17Field: SolrField[F17, M] = f17(meta)
     val f17Name: String = f17Field.name

     val f18Field: SolrField[F18, M] = f18(meta)
     val f18Name: String = f18Field.name
     val transformer = Some(((doc: HashMap[String,Any], highlighting: HashMap[String,HashMap[String,ArrayList[String]]]) => {
     val f1 = getForField(f1Field, f1Name, doc)
     val f2 = getForField(f2Field, f2Name, doc)
     val f3 = getForField(f3Field, f3Name, doc)
     val f4 = getForField(f4Field, f4Name, doc)
     val f5 = getForField(f5Field, f5Name, doc)
     val f6 = getForField(f6Field, f6Name, doc)
     val f7 = getForField(f7Field, f7Name, doc)
     val f8 = getForField(f8Field, f8Name, doc)
     val f9 = getForField(f9Field, f9Name, doc)
     val f10 = getForField(f10Field, f10Name, doc)
     val f11 = getForField(f11Field, f11Name, doc)
     val f12 = getForField(f12Field, f12Name, doc)
     val f13 = getForField(f13Field, f13Name, doc)
     val f14 = getForField(f14Field, f14Name, doc)
     val f15 = getForField(f15Field, f15Name, doc)
     val f16 = getForField(f16Field, f16Name, doc)
     val f17 = getForField(f17Field, f17Name, doc)
     val f18 = getForField(f18Field, f18Name, doc)
     create(f1 , f2 , f3 , f4 , f5 , f6 , f7 , f8 , f9 , f10 , f11 , f12 , f13 , f14 , f15 , f16 , f17 , f18)}));
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields,
                 phraseBoostFields, boostFields, start, limit, tieBreaker,
                 sort, minimumMatch, queryType,
                 (f1Name::f2Name::f3Name::f4Name::f5Name::f6Name::f7Name::f8Name::
                 f9Name::f10Name::f11Name::f12Name::f13Name::f14Name::f15Name::f16Name::
                 f17Name::f18Name::fieldsToFetch).distinct,
                 hls, transformer, fallOf, min)
  }

}
object Helpers {
  def groupWithOr[V](v: Iterable[Query[V]]): Query[V] = {
    if (v.isEmpty)
      Group(Empty[V])
    else
      Group(v.tail.foldLeft(v.head: Query[V])({(l, r) => Or(l, r)}))
  }

  def groupWithAnd[V](v: Iterable[Query[V]]): Query[V] = {
    if (v.isEmpty)
      Group(Empty[V])
    else
      Group(v.tail.foldLeft(v.head: Query[V])({(l, r) => And(l, r)}))
  }
}
