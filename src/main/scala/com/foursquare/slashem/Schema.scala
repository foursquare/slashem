// Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.slashem
import com.foursquare.slashem.Ast._
import net.liftweb.record.{Record, OwnedField, Field, MetaRecord}
import net.liftweb.record.field.{BooleanField, LongField, StringField, IntField, DoubleField}
import net.liftweb.common.{Box, Empty, Full}
import com.twitter.util.{Duration, Future, FutureTask}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import org.bson.types.ObjectId
import org.codehaus.jackson.annotate._
import org.codehaus.jackson.map.{DeserializationConfig, ObjectMapper}
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.client.action.search.SearchRequestBuilder
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.node.Node
import org.elasticsearch.index.query.{CustomScoreQueryBuilder,
                                      QueryBuilder => ElasticQueryBuilder,
                                      FilterBuilder => ElasticFilterBuilder,
                                      AndFilterBuilder};
import org.elasticsearch.action.support.nodes.NodesOperationRequest
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest
import org.elasticsearch.index.query.QueryBuilders.filteredQuery
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.handler.codec.http.{DefaultHttpRequest, HttpResponseStatus, HttpHeaders, HttpMethod,
                                           HttpVersion, QueryStringEncoder}

import scala.annotation.tailrec

import org.joda.time.DateTime
import java.util.{HashMap, ArrayList}
import java.lang.Integer
import java.net.InetSocketAddress
import collection.JavaConversions._

case class SolrResponseException(code: Int, reason: String, solrName: String, query: String) extends RuntimeException {
  override def getMessage(): String = {
     "Solr %s request resulted in HTTP %s: %s\n%s: %s".format(solrName, code, reason, solrName, query)
  }
}

/** The response header. There are normally more fields in the response header we could extract, but
 * we don't at present. */
case class ResponseHeader @JsonCreator()(@JsonProperty("status")status: Int, @JsonProperty("QTime")QTime: Int)

/** The response its self. The "docs" field is not type safe, you should use one of results or oids to access the results */
case class Response[T <: Record[T],Y] (schema: T, creator: Option[(Pair[Map[String,Any],Option[Map[String,ArrayList[String]]]]) => Y], numFound: Int, start: Int, docs: Array[Pair[Map[String,Any],Option[Map[String,ArrayList[String]]]]], fallOf: Option[Double], min: Option[Int]) {
  val filteredDocs = filterHighQuality(docs)
  def results[T <: Record[T]](B: Record[T]): List[T] = {
    filteredDocs.map({input =>
      val doc = input._1
      val hl = input._2
      val q = B.meta.createRecord
      val matchingHighlights = hl
      doc.foreach(a => {
                 val fname = a._1
                 val value = a._2
                 q.fieldByName(fname).map((field) =>{
                  matchingHighlights match {
                    case Some(mhl) if (mhl.contains(fname)) => {
                      field match {
                        case f: SlashemField[_,_] => f.setHighlighted(mhl.get(fname).get.toList)
                        case _ => None
                      }
                    }
                    case _ => None
                  }
                  field.setFromAny(value)
                }
                                       )}
               )
              q.asInstanceOf[T]
            }).toList
  }
  // Collect results which are of a high enough lucene score to be relevant.
  private def filterHighQuality(r: Array[Pair[Map[String,Any],Option[Map[String,ArrayList[String]]]]]):
  Array[Pair[Map[String,Any],Option[Map[String,ArrayList[String]]]]] = {
    (min,fallOf) match {
      case (Some(minR),Some(qualityFallOf)) => {
        val scores = (r.map(x => x._1.get("score").map(_.asInstanceOf[Double])).toList)
        val hqCount = highQuality(scores,
                                score=0, count=0,minR=minR,qualityFallOf=qualityFallOf)
        r.take(hqCount)
      }
      case _ => r
    }
  }
  @tailrec private def highQuality(l: List[Option[Double]], score: Double = 0, count: Int = 0, minR: Int = 1, qualityFallOf: Double = 0): Int = {
    val minScore = qualityFallOf * (score / count)
    l match {
      case Some(x) :: xs if (count < minR || x > minScore) => highQuality(xs,score=(score+x),count=(count+1),minR=minR,qualityFallOf=qualityFallOf)
      case None :: xs if (score == 0) => highQuality(xs, score, count + 1)
      case None :: xs => count
      case _ => count
    }
  }

  /** Return a list of the documents in a usable form */
  def results: List[T] = results(schema)
  /** Return a list of results handled by the creator
   * Most commonly used for case class based queries */
  def processedResults: List[Y] = {
    creator match {
      case Some(func) => { filteredDocs.map(func(_)).toList }
      case None => Nil
    }
  }
  /** Special for extracting just ObjectIds without the overhead of record. */
  def oids: List[ObjectId] = {
    filteredDocs.map({ doc => doc._1.find(x => x._1 == "id").map(x => new ObjectId(x._2.toString))}).toList.flatten
  }
  /** Another special case for extracting just ObjectId & score pairs.
   * Please think twice before using*/
  def oidScorePair: List[(ObjectId, Double)] = {
    val oids = filteredDocs.map({doc => doc._1.find(x => x._1 == "id").map(x => new ObjectId(x._2.toString))}).toList.flatten
    val scores = filteredDocs.map({doc => doc._1.find(x => x._1 == "score").map(x => x._2.asInstanceOf[Double])}).toList.flatten
    oids zip scores
  }

}

/** The search results class, you are probably most interested in the contents of response */
case class SearchResults[T <: Record[T],Y] (responseHeader: ResponseHeader,
                             response: Response[T,Y])


//This is the raw representation of the response from solr, you probably don't want to poke at it directly.
case class RawResponse @JsonCreator()(@JsonProperty("numFound")numFound: Int, @JsonProperty("start")start: Int,
                                      @JsonProperty("docs")docs: Array[HashMap[String,Any]])

//This is the raw representation of the response from solr, you probably don't want to poke at it directly.
case class RawSearchResults @JsonCreator()(@JsonProperty("responseHeader") responseHeader: ResponseHeader,
                                           @JsonProperty("response") response: RawResponse,
                                           @JsonProperty("highlighting") highlighting: HashMap[String,HashMap[String,ArrayList[String]]])

trait SlashemMeta[T <: Record[T]] extends MetaRecord[T] {
  self: MetaRecord[T] with T =>
  var logger: SolrQueryLogger = NoopQueryLogger
}
trait ElasticMeta[T <: Record[T]] extends SlashemMeta[T] {
  self: MetaRecord[T] with T =>

  val clusterName = "testcluster" //Override me knthx
  val indexName = "testindex"//Override me too
  val docType = "slashemdoc"
  val useTransport = true// Override if you want to use transport client
  def servers: List[String] = List() //Define if your going to use the transport client
  def serverInetSockets = servers.map(x => {val h = x.split(":")
                             val s = h.head
                             val p = h.last
                             new InetSocketTransportAddress(s, p.toInt)})

  var node: Node = null

  var myClient: Option[Client]

  def client: Client = {
    myClient match {
      case Some(cl) => cl
      case _ => { myClient = Some({
        if (useTransport) {
          val settings = ImmutableSettings.settingsBuilder().put("cluster.name",clusterName)
          val tc = new TransportClient(settings);
          serverInetSockets.map(tc.addTransportAddress(_))
          tc
        } else {
          node.client()
        }
      })
     myClient.get
     }
    }
  }
}
trait SolrMeta[T <: Record[T]] extends SlashemMeta[T] {
  self: MetaRecord[T] with T =>

  /** The servers is a list used in round-robin for running solr read queries against.
   * It can just be one element if you wish */
  def servers: List[String]

  //The name is used to determine which props to use as well as for logging
  def solrName: String

  //Params for the client
  def retries = 3
  def hostConnectionLimit = 1000
  def hostConnectionCoresize = 300


  def client = {
    ClientBuilder()
    .codec(Http())
    .hosts(servers.map(x => {val h = x.split(":")
                             val s = h.head
                             val p = h.last
                             new InetSocketAddress(s, p.toInt)}))
    .hostConnectionLimit(hostConnectionLimit)
    .hostConnectionCoresize(hostConnectionCoresize)
    .retries(retries)
    .build()}

  //This is used so the json extractor can do its job
  implicit val formats = net.liftweb.json.DefaultFormats
  val mapper = {
    val a = new ObjectMapper
    //We don't extract all of the fields so we ignore unknown properties.
    a.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    a
  }

  def extractFromResponse[Y](r: String, creator: Option[(Pair[Map[String,Any],
                                                              Option[Map[String,ArrayList[String]]]]) => Y],
                             fieldstofetch: List[String]=Nil, fallOf: Option[Double] = None,
                             min: Option[Int] = None, queryText: String): Future[SearchResults[T, Y]] = {
    //This intentional avoids lift extract as it is too slow for our use case.
    try {
      val rsr = mapper.readValue(r, classOf[RawSearchResults])
      //Take the raw search result and make the type templated search result.
      val rawDocs = rsr.response.docs
      val rawHls = rsr.highlighting
      val joinedDocs = rawDocs.map(doc => {
        val hl = if (doc.contains("id") && rsr.highlighting != null
                     && rsr.highlighting.contains(doc.get("id").toString)
                   ) {
          Some(rsr.highlighting.get(doc.get("id").toString).toMap)
        } else {
          None
        }
        Pair(doc.toMap,hl)})
      Future(SearchResults(rsr.responseHeader, Response(createRecord, creator, rsr.response.numFound, rsr.response.start,
                                                        joinedDocs, fallOf, min)))

    } catch {
      case e => Future.exception(new Exception("An error occured while parsing solr result \""+r+
                                               "\" from query ("+queryText+")",e))
    }
  }

  def queryString(params: Seq[(String, String)]): QueryStringEncoder = {
    val qse = new QueryStringEncoder("/solr/select/")
    qse.addParam("wt", "json")

    params.foreach( x => {
      qse.addParam(x._1, x._2)
    })

  qse
  }

  // This method performs the actually query / http request. It should probably
  // go in another file when it gets more sophisticated.
  def rawQueryFuture(params: Seq[(String, String)]): Future[String] = {
    //Ugly

    val qse = queryString(params ++
                      logger.queryIdToken.map("magicLoggingToken" -> _).toList)


    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, qse.toString)
    //Here be dragons! If you have multiple backends with shared IPs this could very well explode
    //but finagle doesn't seem to properly set the http host header for http/1.1
    request.addHeader(HttpHeaders.Names.HOST, servers.head);

    client(request).map(response => {
      response.getStatus match {
        case HttpResponseStatus.OK => response.getContent.toString(CharsetUtil.UTF_8)
        case status => throw SolrResponseException(status.getCode, status.getReasonPhrase, solrName, qse.toString)
      }
    }).ensure { client.release }
  }

}
//If you want to get some simple logging/timing implement this trait
trait SolrQueryLogger {
  def log(name: String, msg: String, time: Long)

  def debug(msg: String)

  // If this returns a string then it will be appended to the query
  // so you can use it to match your query logs with application
  // logs.
  def queryIdToken(): Option[String] = None

  //Log failure
  def failure(name: String, message: String, e: Throwable): Unit = {
  }
  //Log success
  def success(name: String): Unit = {
  }
}
/** The default logger, does nothing. */
object NoopQueryLogger extends SolrQueryLogger {
  override def log(name: String, msg: String, time: Long) = Unit
  override def debug(msg: String) = println(msg)
}

//If you want any of the geo queries you will have to implement this
trait SolrGeoHash {
  def coverString (geoLat: Double, geoLong: Double, radiusInMeters: Int, maxCells: Int ): Seq[String]
  def rectCoverString(topRight: (Double, Double), bottomLeft: (Double, Double), maxCells: Int = 0, minLevel: Int = 0, maxLevel: Int = 0): Seq[String]
  def maxCells: Int = 0
}
//Default geohash, does nothing.
object NoopSolrGeoHash extends SolrGeoHash {
  def coverString (geoLat: Double, geoLong: Double, radiusInMeters: Int, maxCells: Int ): Seq[String] = List("pleaseUseaRealGeoHash")
  def rectCoverString(topRight: (Double, Double), bottomLeft: (Double, Double), maxCells: Int = 0, minLevel: Int = 0, maxLevel: Int = 0): Seq[String] = List("pleaseUseaRealGeoHash")
}

trait SlashemSchema[M <: Record[M]] extends Record[M] {
  self: M with Record[M] =>

  def meta: SlashemMeta[M]

  //Set me to something which collects timing if you want (hint: you do)
  var geohash: SolrGeoHash = NoopSolrGeoHash

  // fixme(jonshea) this should go somewhere else
  def timeFuture[T](someFuture: Future[T]): Future[(Long, T)] = {
    val startTime = System.currentTimeMillis
    someFuture.map(x => {
      val endTime = System.currentTimeMillis
      (endTime-startTime,x)
    })
  }


  def where[F](c: M => Clause[F]): QueryBuilder[M, Unordered, Unlimited, defaultMM, NoSelect, NoHighlighting, NoQualityFilter] = {
    QueryBuilder(self, c(self), filters=Nil, boostQueries=Nil, queryFields=Nil,
                 phraseBoostFields=Nil, boostFields=Nil, start=None, limit=None,
                 tieBreaker=None, sort=None, minimumMatch=None ,queryType=None,
                 fieldsToFetch=Nil, hls=None, creator=None, comment=None,
                 fallOf=None, min=None)
  }
  def query[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter](timeout: Duration, qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q]): SearchResults[M, Y]
  def queryFuture[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter](qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q]): Future[SearchResults[M, Y]]
}
trait ElasticSchema[M <: Record[M]] extends SlashemSchema[M] {
  self: M with SlashemSchema[M] =>

  def meta: ElasticMeta[M]

  def query[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter](timeout: Duration, qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q]):
  SearchResults[M, Y] = {
    queryFuture(qb)(timeout)
  }

  def queryFuture[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter](qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q]):
  Future[SearchResults[M, Y]] = {
    elasticQueryFuture(qb, buildElasticQuery(qb))
  }
  def elasticQueryFuture[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter](qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q], query: ElasticQueryBuilder): Future[SearchResults[M, Y]] = {
    val future : FutureTask[SearchResults[M,Y]]= new FutureTask({
      val client = meta.client
      val from = qb.start.map(_.toInt).getOrElse(qb.DefaultStart)
      val limit =  qb.limit.map(_.toInt).getOrElse(qb.DefaultLimit)
      meta.logger.debug("Query details "+query.toString())
      val baseRequest: SearchRequestBuilder = client.prepareSearch(meta.indexName)
      .setQuery(query.buildAsBytes())
      .setFrom(from)
      .setSize(limit)
      .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
      val request = qb.sort match {
        case None => baseRequest
        case Some(Pair(sort,"asc")) => baseRequest.addSort(sort.elasticExtend(),SortOrder.ASC)
        case Some(Pair(sort,"desc")) => baseRequest.addSort(sort.elasticExtend(),SortOrder.DESC)
        case _ => baseRequest
      }
      val response: SearchResponse  = request
      .execute().actionGet()
      meta.logger.debug("Search response "+response.toString())
      constructSearchResults(qb.creator,
                             qb.start.map(_.toInt).getOrElse(qb.DefaultStart),
                             qb.fallOf,
                             qb.min,
                             response)
    }
    )
    future.run()
    timeFuture(future).map( {
      case (queryTime, result) => {
        meta.logger.log(meta.indexName+".query",query.toString(), queryTime)
        result
      }})
  }
  def constructSearchResults[Y](creator: Option[(Pair[Map[String,Any],
                                                      Option[Map[String,ArrayList[String]]]]) => Y],
                                start: Int,
                                fallOf: Option[Double],
                                min: Option[Int],
                                response: SearchResponse): SearchResults[M, Y] = {
    val time = response.tookInMillis()
    val hitCount = response.getHits().totalHits().toInt
    val docs: Array[(Map[String,Any], Option[Map[String,java.util.ArrayList[String]]])] = response.getHits().getHits().map(doc => {
      val m = doc.sourceAsMap()
      val annotedMap = m.toMap ++ List("score" -> doc.score())
      //If we don't get the score back
      //m.put("score",doc.score())
      val hlf = doc.getHighlightFields()
      if (hlf == null) {
        Pair(annotedMap,None)
      } else {
        Pair(annotedMap,
             Some(doc.getHighlightFields()
                  .mapValues(v => new ArrayList(v.getFragments().toList))
                  .toMap))
      }
    })

   SearchResults(ResponseHeader(200,time.toInt),
                 Response(this, creator, hitCount, start, docs,
                        fallOf=fallOf, min=min))
  }
  def buildElasticQuery[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter](qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q]): ElasticQueryBuilder = {
    val baseQuery: ElasticQueryBuilder= qb.clauses.elasticExtend(qb.queryFields,qb.phraseBoostFields)
    //Apply filters if necessary
    val fq = qb.filters match {
      case Nil => baseQuery
      case _ => filteredQuery(baseQuery,combineFilters(qb.filters.map(_.elasticFilter(qb.queryFields))))
    }
    //Apply any custom scoring rules (aka emulating Solr's bq/bf)
    qb.boostFields match {
      case Nil => fq
      case _ => boostFields(fq,qb.boostFields)
    }

  }
  def boostFields(query: ElasticQueryBuilder, boostFields: List[ScoreBoost]): ElasticQueryBuilder =  {
    val boostedQuery = new CustomScoreQueryBuilder(query)
    val scoreScript = "_score + "+(boostFields.map(_.elasticExtend).mkString(" + "))
    boostedQuery.script(scoreScript)
  }
  def combineFilters(filters: List[ElasticFilterBuilder]): ElasticFilterBuilder = {
    new AndFilterBuilder(filters:_*)
  }

}
trait SolrSchema[M <: Record[M]] extends SlashemSchema[M] {
  self: M with SlashemSchema[M] =>

  def meta: SolrMeta[M]
  // 'Where' is the entry method for a SolrRogue query.

  def queryParams[Ord, Lim, MM <: MinimumMatchType, Select, H <: Highlighting, Q <: QualityFilter](qb: QueryBuilder[M, Ord, Lim, MM, Select, H, Q]): Seq[(String, String)] = queryParamsWithBounds(qb,qb.start, qb.limit)

  def queryParamsWithBounds[Ord, Lim, MM <: MinimumMatchType, Select, H <: Highlighting, Q <: QualityFilter](qb: QueryBuilder[M, Ord, Lim, MM, Select, H, Q], qstart: Option[Long], qrows: Option[Long]): Seq[(String,String)] = {
    val bounds = List(("start" -> (qstart.getOrElse {qb.DefaultStart}).toString),
                 ("rows" -> (qrows.getOrElse {qb.DefaultLimit}).toString))
    bounds ++ queryParamsNoBounds(qb)
  }

  //This is the part which generates most of the solr request
  def queryParamsNoBounds[Ord, Lim, MM <: MinimumMatchType, Select, H <: Highlighting, Q <: QualityFilter](qb: QueryBuilder[M, Ord, Lim, MM, Select, H, Q]): Seq[(String,String)] = {
    val p = List(("q" -> qb.clauses.extend))

    val s = qb.sort match {
      case None => Nil
      case Some(sort) => List("sort" -> (sort._1.extend + " " + sort._2))
    }
    val qt = qb.queryType match {
      case None => Nil
      case Some(method) => List("defType" -> method)
    }
    val mm = qb.minimumMatch match {
      case None => Nil
      case Some(mmParam) => List("mm" -> mmParam)
    }

    val bq = qb.boostQueries.map({ x => ("bq" -> x.extend)})

    val qf = qb.queryFields.filter({x => x.boost != 0}).map({x => ("qf" -> x.extend)})

    val pf = qb.phraseBoostFields.filter(x => x.pf).map({x => ("pf" -> x.extend)})++
    qb.phraseBoostFields.filter(x => x.pf2).map({x => ("pf2" -> x.extend)})++
    qb.phraseBoostFields.filter(x => x.pf3).map({x => ("pf3" -> x.extend)})

    val fl = qb.fieldsToFetch match {
      case Nil => Nil
      case x => List("fl" -> (x.mkString(",")))
    }

    val t = qb.tieBreaker match {
      case None => Nil
      case Some(x) => List("tieBreaker" -> x.toString)
    }

    val hlp = qb.hls match {
      case None => Nil
      case Some(a) => List("hl" -> a)
    }

    val bf = qb.boostFields.map({x => ("bf" -> x.extend)})

    val f = qb.filters.map({x => ("fq" -> x.extend)})

    val ct = qb.comment match {
      case None => Nil
      case Some(a) => List("comment" -> a)
    }

     ct ++ t ++ mm ++ qt ++ bq ++ qf ++ p ++ s ++ f ++ pf ++ fl ++ bf ++ hlp
  }


  def query[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter](timeout: Duration, qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q]):
  SearchResults[M, Y] = {
    queryFuture(qb)(timeout)
  }

  def queryFuture[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter](qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q]):
  Future[SearchResults[M, Y]] = {
    solrQueryFuture(qb.creator, queryParams(qb), qb.fieldsToFetch, qb.fallOf, qb.min)
  }
  //The query builder calls into this to do actually execute the query.
  def solrQueryFuture[Y](creator: Option[(Pair[Map[String,Any],
                                               Option[Map[String,ArrayList[String]]]]) => Y],
                     params: Seq[(String, String)],
                     fieldstofetch: List[String],
                     fallOf: Option[Double],
                     min: Option[Int]): Future[SearchResults[M, Y]] = {
    val queryText = meta.queryString(params).toString

    timeFuture(meta.rawQueryFuture(params)).map({
      case (queryTime, jsonString) => {
        meta.logger.log(meta.solrName + ".query", queryText, queryTime)
        jsonString
      }}).flatMap(jsonString => {
      meta.extractFromResponse(jsonString, creator,
                               fieldstofetch,
                               fallOf,
                               min,
                               queryText)
                })
    .onSuccess(_ => meta.logger.success(meta.solrName))
    .onFailure(e => meta.logger.failure(meta.solrName, queryText, e))
  }

}


trait SlashemField[V, M <: Record[M]] extends OwnedField[M] {
  self: Field[V, M] =>
  import Helpers._

  //Not eqs and neqs results in phrase queries!
  def eqs(v: V) = Clause[V](self.name, Group(Phrase(v)))
  def neqs(v: V) = Clause[V](self.name, Phrase(v),false)

  //This allows for bag of words style matching.
  def contains(v: V) = Clause[V](self.name, Group(BagOfWords(v)))
  def contains(v: V, b: Float) = Clause[V](self.name, Boost(Group(BagOfWords(v)),b))

  def in(v: Iterable[V]) = Clause[V](self.name, groupWithOr(v.map({x: V => Phrase(x)})))
  def nin(v: Iterable[V]) = Clause[V](self.name, groupWithOr(v.map({x: V => Phrase(x)})),false)

  def in(v: Iterable[V], b: Float) = Clause[V](self.name, Boost(groupWithOr(v.map({x: V => Phrase(x)})),b))
  def nin(v: Iterable[V], b: Float) = Clause[V](self.name, Boost(groupWithOr(v.map({x: V => Phrase(x)})),b),false)


  def inRange(v1: V, v2: V) = Clause[V](self.name, Group(Range(BagOfWords(v1),BagOfWords(v2))))
  def ninRange(v1: V, v2: V) = Clause[V](self.name, Group(Range(BagOfWords(v1),BagOfWords(v2))),false)

  def lessThan(v: V) = Clause[V](self.name, Group(Range(Splat[V](),BagOfWords[V](v))))
  def greaterThan(v: V) = Clause[V](self.name, Group(Range(BagOfWords[V](v),Splat[V]())))


  def any = Clause[V](self.name,Splat[V]())

  def query(q: Query[V]) = Clause[V](self.name, q)

  def setFromAny(a: Any): Box[V]

  def valueBoxFromAny(a: Any): Box[V] = {
    try {
      Full(a.asInstanceOf[V])
    } catch {
      case _ => Empty
    }
  }
  //Support for highlighting matches
  var hl: List[String] = Nil
  def highlighted: List[String] = {
    hl
  }
  def setHighlighted(a: List[String]) = {
    hl = a
  }

}

//Slashem field types
class SlashemStringField[T <: Record[T]](owner: T) extends StringField[T](owner, 0) with SlashemField[String, T]
//Allows for querying against the default filed in solr. This field doesn't have a name
class SlashemDefaultStringField[T <: Record[T]](owner: T) extends StringField[T](owner, 0) with SlashemField[String, T] {
  override def name = ""
}
class SlashemIntField[T <: Record[T]](owner: T) extends IntField[T](owner) with SlashemField[Int, T]
class SlashemDoubleField[T <: Record[T]](owner: T) extends DoubleField[T](owner) with SlashemField[Double, T]
class SlashemLongField[T <: Record[T]](owner: T) extends LongField[T](owner) with SlashemField[Long, T]
class SlashemObjectIdField[T <: Record[T]](owner: T) extends ObjectIdField[T](owner) with SlashemField[ObjectId, T] {
  override def valueBoxFromAny(a: Any): Box[ObjectId] = objectIdBoxFromAny(a)
}
class SlashemIntListField[T <: Record[T]](owner: T) extends IntListField[T](owner) with SlashemField[List[Int], T] {
  override def valueBoxFromAny(a: Any) = {
  try {
    a match {
      case "" => Empty
      case ar: Array[Int] => Full(ar.toList)
      case ar: Array[Integer] => Full(ar.toList.map(x=>x.intValue))
      case s: String => Full(s.split(" ").map(x => x.toInt).toList)
      case _ => Empty
    }
    } catch {
      case _ => Empty
    }
  }
}
class SlashemPointField[T <: Record[T]](owner: T) extends PointField[T](owner) with SlashemField[Pair[Double,Double], T] {
  def geoDistance(geolat: Double, geolng: Double) = {
    GeoDist(this.name,geolat,geolng)
  }
  //Shortcut since we normally want the recip not the actual distance
  def recipGeoDistance(geolat: Double, geolng: Double,x : Int, y: Int, z: Int) = {
    Recip(GeoDist(this.name,geolat,geolng),x,y,z)
  }
  def sqeGeoDistance(geolat: Double, geolng: Double) = {
    GeoDist(this.name,geolat,geolng,"square")
  }
  //Shortcut since we normally want the recip not the actual distance
  def recipSqeGeoDistance(geolat: Double, geolng: Double,x : Int, y: Int, z: Int) = {
    Recip(GeoDist(this.name,geolat,geolng,"square"),x,y,z)
  }

}
class SlashemBooleanField[T <: Record[T]](owner: T) extends BooleanField[T](owner) with SlashemField[Boolean, T]
class SlashemDateTimeField[T <: Record[T]](owner: T) extends JodaDateTimeField[T](owner) with SlashemField[DateTime, T]
//More restrictive type so we can access the geohash
class SlashemGeoField[T <: SlashemSchema[T]](owner: T) extends StringField[T](owner,0) with SlashemField[String, T] {
  def inRadius(geoLat: Double, geoLong: Double, radiusInMeters: Int, maxCells: Int = owner.geohash.maxCells) = {
    val cellIds = owner.geohash.coverString(geoLat, geoLong, radiusInMeters, maxCells = maxCells)
    //If we have an empty cover we default to everything.
    cellIds match {
      case Nil => this.any
      case _ => this.in(cellIds)
    }
  }
  def inBox(topRight: (Double, Double), botLeft: (Double, Double), maxCells: Int = owner.geohash.maxCells) = {
    val cellIds = owner.geohash.rectCoverString(topRight,botLeft, maxCells = maxCells)
    //If we have an empty cover we default to everything.
    cellIds match {
      case Nil => this.any
      case _ => this.in(cellIds)
    }
  }
}
//Backwards compat
//Slashem field types
class SolrStringField[T <: Record[T]](owner: T) extends SlashemStringField[T](owner)
//Allows for querying against the default filed in solr. This field doesn't have a name
class SolrDefaultStringField[T <: Record[T]](owner: T) extends SlashemDefaultStringField[T](owner)
class SolrIntField[T <: Record[T]](owner: T) extends SlashemIntField[T](owner)
class SolrDoubleField[T <: Record[T]](owner: T) extends SlashemDoubleField[T](owner)
class SolrLongField[T <: Record[T]](owner: T) extends SlashemLongField[T](owner)
class SolrObjectIdField[T <: Record[T]](owner: T) extends SlashemObjectIdField[T](owner)
class SolrIntListField[T <: Record[T]](owner: T) extends SlashemIntListField[T](owner)
class SolrBooleanField[T <: Record[T]](owner: T) extends SlashemBooleanField[T](owner)
class SolrDateTimeField[T <: Record[T]](owner: T) extends SlashemDateTimeField[T](owner)
class SolrGeoField[T <: SlashemSchema[T]](owner: T) extends SlashemGeoField[T](owner)
// This insanity makes me want to 86 Record all together. DummyField allows us
// to easily define our own Field types. I use this for ObjectId so that I don't
// have to import all of MongoRecord. We could trivially reimplement the other
// Field types using it.
class ObjectIdField[T <: Record[T]](override val owner: T) extends Field[ObjectId, T] {

  type ValueType = ObjectId
  var e: Box[ValueType] = Empty

  def setFromString(s: String) = Full(set(new ObjectId(s)))

  // NOTE(benjy): We can't put this implementation directly in valueBoxFromAny, because SlashemObjectIdField
  // wouldn't be able to use this definition (it must redefine it so it can add the 'override' modifier, and it
  // can't call super.valueBoxFromAny because that would, according to the rules of linearization, invoke
  // SlashemField.valueBoxFromAny, which is not what we want).
  // TODO: This has bad code smell and indicates a brittle design.
  def objectIdBoxFromAny(a: Any): Box[ObjectId] = {
    try {
      a match {
        case "" => Empty
        case s: String => Full(new ObjectId(s))
        case i: ObjectId => Full(i)
        case _ => Empty
      }
    } catch {
      case _ => Empty
    }
  }

  def valueBoxFromAny(a: Any): Box[ObjectId] = objectIdBoxFromAny(a)

  override def setFromAny(a: Any) ={
    val vb = valueBoxFromAny(a)
    vb.map(set(_))
  }
  override def setFromJValue(jv: net.liftweb.json.JsonAST.JValue) = Empty
  override def liftSetFilterToBox(a: Box[ValueType]) = Empty
  override def toBoxMyType(a: ValueType) = Empty
  override def defaultValueBox = Empty
  override def toValueType(a: Box[MyType]) = null.asInstanceOf[ValueType]
  override def asJValue() = net.liftweb.json.JsonAST.JNothing
  override def asJs() = net.liftweb.http.js.JE.JsNull
  override def toForm = Empty
  override def set(a: ValueType) = {e = Full(a)
                                    a.asInstanceOf[ValueType]}
  override def get() = e.get
  override def is() = e.get
  override def valueBox() = e
}
class JodaDateTimeField[T <: Record[T]](override val owner: T) extends DummyField[DateTime, T](owner) {
  type ValueType = DateTime
  var e: Box[ValueType] = Empty

  override def setFromString(s: String): Box[ValueType] = {
    try {
      Full(set(new DateTime(s)))
    } catch {
      case _ => Empty
    }
  }
  override def setFromAny(a: Any): Box[ValueType] ={
    a match {
      case s: String => setFromString(s)
      case d: DateTime => Full(set(d))
      case _ => Empty
    }
  }
  override def set(a: ValueType) = {e = Full(a)
                                    a.asInstanceOf[ValueType]}
  override def get() = e.get
  override def is() = e.get
  override def valueBox() = e
}

//This allows support for a list of integers as a field value.
class IntListField[T <: Record[T]](override val owner: T) extends Field[List[Int], T] {
  type ValueType = List[Int]
  var e: Box[ValueType] = Empty

  def setFromString(s: String) = {
    Full(set(s.split(" ").map(x => x.toInt).toList))
  }
  override def setFromAny(a: Any) ={
  try {
    a match {
      case "" => Empty
      case ar: Array[Int] => Full(set(ar.toList))
      case ar: Array[Integer] => Full(set(ar.toList.map(x=>x.intValue)))
      case s: String => Full(set(s.split(" ").map(x => x.toInt).toList))
      case _ => Empty
    }
    } catch {
      case _ => Empty
    }
  }
  override def setFromJValue(jv: net.liftweb.json.JsonAST.JValue) = Empty
  override def liftSetFilterToBox(a: Box[ValueType]) = Empty
  override def toBoxMyType(a: ValueType) = Empty
  override def defaultValueBox = Empty
  override def toValueType(a: Box[MyType]) = null.asInstanceOf[ValueType]
  override def asJValue() = net.liftweb.json.JsonAST.JNothing
  override def asJs() = net.liftweb.http.js.JE.JsNull
  override def toForm = Empty
  override def set(a: ValueType) = {e = Full(a)
                                    a.asInstanceOf[ValueType]}
  override def get() = e.get
  override def is() = e.get
  def value() = e getOrElse Nil
  override def valueBox() = e
}
class PointField[T <: Record[T]](override val owner: T) extends Field[Pair[Double, Double], T] {
  type ValueType = Pair[Double, Double]
  var e: Box[ValueType] = Empty

  def setFromString(s: String) = {
    val doubles = s.split(",").map(x => x.toDouble).toList
    doubles.length match {
      case 2 => Full(set(Pair(doubles.apply(0),doubles.apply(1))))
      case _ => Empty
    }
  }
  override def setFromAny(a: Any) ={
  try {
    a match {
      case "" => Empty
      case ar: Array[Double] => Full(set(Pair(ar.apply(0),ar.apply(1))))
      case (lat : Double)::(lng: Double)::Nil => Full(set(Pair(lat,lng)))
      case arl: ArrayList[_] => Full(set(Pair(arl.get(0).asInstanceOf[Double],arl.get(1).asInstanceOf[Double])))
      case s: String => setFromString(s)
      case _ => Empty
    }
    } catch {
      case _ => Empty
    }
  }
  override def setFromJValue(jv: net.liftweb.json.JsonAST.JValue) = Empty
  override def liftSetFilterToBox(a: Box[ValueType]) = Empty
  override def toBoxMyType(a: ValueType) = Empty
  override def defaultValueBox = Empty
  override def toValueType(a: Box[MyType]) = null.asInstanceOf[ValueType]
  override def asJValue() = net.liftweb.json.JsonAST.JNothing
  override def asJs() = net.liftweb.http.js.JE.JsNull
  override def toForm = Empty
  override def set(a: ValueType) = {e = Full(a)
                                    a.asInstanceOf[ValueType]}
  override def get() = e.get
  override def is() = e.get
  def value() = e.get
  override def valueBox() = e
}
class DummyField[V, T <: Record[T]](override val owner: T) extends Field[V, T] {
  override def setFromString(s: String): Box[V] = Empty
  override def setFromAny(a: Any): Box[V] = Empty
  override def setFromJValue(jv: net.liftweb.json.JsonAST.JValue) = Empty
  override def liftSetFilterToBox(a: Box[V]) = Empty
  override def toBoxMyType(a: ValueType) = Empty
  override def defaultValueBox = Empty
  override def toValueType(a: Box[MyType]) = null.asInstanceOf[ValueType]
  override def asJValue() = net.liftweb.json.JsonAST.JNothing
  override def asJs() = net.liftweb.http.js.JE.JsNull
  override def toForm = Empty
  override def set(a: ValueType) = null.asInstanceOf[ValueType]
  override def get() = null.asInstanceOf[ValueType]
  override def is() = null.asInstanceOf[ValueType]
}
