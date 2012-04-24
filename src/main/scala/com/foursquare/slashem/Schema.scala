// Copyright 2011-2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.slashem


import com.foursquare.slashem.Ast._
import com.twitter.util.{Duration, ExecutorServiceFuturePool, Future, FuturePool, FutureTask, Promise}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.finagle.Service
import java.lang.Integer
import java.net.InetSocketAddress
import java.util.{ArrayList, HashMap}
import java.util.concurrent.{Executors, ExecutorService}
import net.liftweb.common.{Box, Empty, Full}
import net.liftweb.record.{Record, OwnedField, Field, MetaRecord}
import net.liftweb.record.field.{BooleanField, LongField, StringField, IntField, DoubleField}
import org.bson.types.ObjectId
import org.codehaus.jackson.annotate._
import org.codehaus.jackson.map.{DeserializationConfig, ObjectMapper}
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.action.{ActionListener, ListenableActionFuture}
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders.filteredQuery
import org.elasticsearch.index.query.{AndFilterBuilder, CustomScoreQueryBuilder,
                                      FilterBuilder => ElasticFilterBuilder,
                                      QueryBuilder => ElasticQueryBuilder}
import org.elasticsearch.node.Node
import org.elasticsearch.node.NodeBuilder._

import org.elasticsearch.search.facet.AbstractFacetBuilder
import org.elasticsearch.search.facet.terms.TermsFacetBuilder
import org.elasticsearch.search.facet.terms.strings.InternalStringTermsFacet
import org.elasticsearch.search.sort.{ScriptSortBuilder, SortOrder}
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.handler.codec.http.{DefaultHttpRequest, HttpResponseStatus, HttpHeaders, HttpMethod,
                                           HttpVersion, QueryStringEncoder, HttpRequest, HttpResponse}
import org.joda.time.DateTime
import scala.annotation.tailrec
import scalaj.collection.Imports._

/**
 * SolrResponseException class that extends RuntimeException
 */
case class SolrResponseException(code: Int, reason: String, solrName: String, query: String) extends RuntimeException {
  override def getMessage(): String = {
    "Solr %s request resulted in HTTP %s: %s\n%s: %s"
      .format(solrName, code, reason, solrName, query)
  }
}

/** The response header. There are normally more fields in the response header we could extract, but
 * we don't at present. */
case class ResponseHeader @JsonCreator()(@JsonProperty("status")status: Int, @JsonProperty("QTime")QTime: Int)

object Response {
  type RawDoc = (Pair[Map[String, Any], Option[Map[String, ArrayList[String]]]])
}

/**
 * The response itself.
 * The "docs" field is not type safe, you should use one of results or oids to access the results
 * Y is the type that we are extracting from the response (e.g. a case class)
 */
case class Response[T <: Record[T], Y](schema: T, creator: Option[Response.RawDoc => Y],
                                       numFound: Int, start: Int, docs: Array[Response.RawDoc],
                                       fallOff: Option[Double], min: Option[Int],
                                       fieldFacets: Map[String,Map[String,Int]]) {
  // Docs with high Lucene scores.
  val filteredDocs: Array[Response.RawDoc] = filterHighQuality(docs)

  /**
   * Gets a List[T] of docs returned from Lucene.
   */
  def results[T <: Record[T]](B: Record[T]): List[T] = {
    filteredDocs.map(fd => {
      val doc: Map[String, Any] = fd._1
      val matchingHighlights: Option[Map[String, ArrayList[String]]] = fd._2
      val q: T = B.meta.createRecord
      doc.foreach(a => {
        val fname = a._1
        val value = a._2
        q.fieldByName(fname).map(field => {
          matchingHighlights match {
            case Some(mhl) if (mhl.contains(fname)) => {
              field match {
                case f: SlashemField[_,_] => f.setHighlighted(mhl.get(fname).get.asScala.toList)
                case _ => None
              }
            }
            case _ => None
          }
          field.setFromAny(value)
        })
      })
      q.asInstanceOf[T]
    }).toList
  }

  /**
   * Collect results which are of a high enough lucene score to be relevant.
   * @param rawDocs List of Docs to be filtered
   * @see RawDoc
   */
  private def filterHighQuality(rawDocs: Array[Response.RawDoc]): Array[Response.RawDoc] = {
    (min, fallOff) match {
      case (Some(minR), Some(qualityFallOff)) => {
        val scores = {
          val scoreArr = rawDocs.map(rd => rd._1.get("score").map(_.asInstanceOf[Double]))
          scoreArr.toList.map(dub => dub match {
            case Some(dubdub) => dubdub
            case None => 0.0
          })
        }
        val hqCount = countHighQuality(scores, scoreAcc=0, lastScore=0, count=0,
                                       minR=minR, qualityFallOff=qualityFallOff,
                                       individualQualityFallOff=(qualityFallOff * 1.1))
        rawDocs.take(hqCount)
      }
      case _ => rawDocs
    }
  }

  /**
   * Counts the number of high quality results using scores
   * returned from lucene
   */
  @tailrec
  private def countHighQuality(scores: List[Double], scoreAcc: Double = 0,
                               lastScore: Double = 0, count: Int = 0,
                               minR: Int = 1, qualityFallOff: Double = 0,
                               individualQualityFallOff: Double = 0): Int = {
    val minScore = {
      val avgScore = scoreAcc / count
      val threshold1 = qualityFallOff * avgScore
      val threshold2 = individualQualityFallOff * lastScore
      scala.math.min(threshold1, threshold2)
    }
    scores match {
      case score :: rest if (count < minR || score > minScore) => {
        countHighQuality(rest, scoreAcc=(scoreAcc + score), lastScore=score,
                         count=(count + 1), minR=minR, qualityFallOff=qualityFallOff,
                         individualQualityFallOff=individualQualityFallOff)
      }
      case score :: rest if (scoreAcc == 0) => countHighQuality(rest, scoreAcc, lastScore, count + 1)
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


/** This is the raw representation of the response from solr, you probably don't want to poke at it directly. */
case class RawResponse @JsonCreator()(@JsonProperty("numFound")numFound: Int, @JsonProperty("start")start: Int,
                                      @JsonProperty("docs")docs: Array[HashMap[String,Any]])

/** This is the raw representation of the response from solr, you probably don't want to poke at it directly. */
case class RawSearchResults @JsonCreator()(@JsonProperty("responseHeader") responseHeader: ResponseHeader,
                                           @JsonProperty("response") response: RawResponse,
                                           @JsonProperty("highlighting") highlighting: HashMap[String,HashMap[String,ArrayList[String]]],
                                           @JsonProperty("facet_counts") facetCounts: RawFacetCounts)

/** This is the raw rep of the facet counts */
case class RawFacetCounts @JsonCreator()(@JsonProperty("facet_fields") facetFields: HashMap[String,ArrayList[Object]])

/** Slashem MetaRecord */
trait SlashemMeta[T <: Record[T]] extends MetaRecord[T] {
  self: MetaRecord[T] with T =>
  var logger: SolrQueryLogger = NoopQueryLogger
  //Default timeout
  val timeout = 2
}

/** Elastic Search MetaRecord */
trait ElasticMeta[T <: Record[T]] extends SlashemMeta[T] {
  self: MetaRecord[T] with T =>

  val clusterName = "testcluster" // Override me knthx
  val indexName = "testindex"// Override me too
  val docType = "slashemdoc"
  val useTransport = true// Override if you want to use transport client
  def servers: List[String] = List() // Define if your going to use the transport client
  def serverInetSockets = servers.map(x => {val h = x.split(":")
                             val s = h.head
                             val p = h.last
                             new InetSocketTransportAddress(s, p.toInt)})

  var node: Node = null
  var myClient: Option[Client]

  val executorService: ExecutorService = Executors.newCachedThreadPool()
  val executorServiceFuturePool: FuturePool = FuturePool(executorService)

  /** Create or get the MetaRecord's client */
  def client: Client = {
    myClient match {
      case Some(cl) => cl
      case _ => {
        myClient = Some({
          if (useTransport) {
            val settings = ImmutableSettings.settingsBuilder().put("cluster.name",clusterName).put("client.transport.sniff",true)
            val tc = new TransportClient(settings)
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

/** Solr MetaRecord */
trait SolrMeta[T <: Record[T]] extends SlashemMeta[T] {
  self: MetaRecord[T] with T =>

  /** The servers is a list used in round-robin for running solr read queries against.
   * It can just be one element if you wish */
  def servers: List[String]

  // The name is used to determine which props to use as well as for logging
  def solrName: String

  // Params for the client
  def retries = 3
  def hostConnectionLimit = 1000
  def hostConnectionCoresize = 300

  var myClient: Option[Service[HttpRequest,HttpResponse]] = None

  def client = {
    myClient match {
      case Some(cl) => cl
      case _ => {
        myClient = Some({
          ClientBuilder()
            .codec(Http())
            .hosts(servers.map(x => {
              val h = x.split(":")
              val s = h.head
              val p = h.last
              new InetSocketAddress(s, p.toInt)
            }))
            .hostConnectionLimit(hostConnectionLimit)
            .hostConnectionCoresize(hostConnectionCoresize)
            .retries(retries)
            .build()})
        myClient.get
      }
    }
  }

  // This is used so the json extractor can do its job
  implicit val formats = net.liftweb.json.DefaultFormats
  val mapper = {
    val a = new ObjectMapper
    // We don't extract all of the fields so we ignore unknown properties.
    a.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    a
  }

  def extractFromResponse[Y](r: String, creator: Option[Response.RawDoc => Y],
                             fieldstofetch: List[String]=Nil, fallOf: Option[Double] = None,
                             min: Option[Int] = None, queryText: String): Future[SearchResults[T, Y]] = {
    def parseFacetCounts(a: List[Object]): List[(String,Int)] = {
      a match {
        case Nil => Nil
        case (x: String)::(y: Integer)::z => (x,y.toInt)::parseFacetCounts(z)
        // Shouldn't happen, but fail silently for now
        case _ => Nil
      }
    }
    // This intentionally avoids lift extract as it is too slow for our use case.
    try {
      val rsr = mapper.readValue(r, classOf[RawSearchResults])
      // Take the raw search result and make the type templated search result.
      val rawDocs = rsr.response.docs
      val rawHls = rsr.highlighting
      val joinedDocs: Array[(Map[String,Any], Option[Map[String,ArrayList[String]]])] = rawDocs.map(jdoc => {
        val doc = jdoc.asScala
        val hl = if (doc.contains("id") && rsr.highlighting != null) {
          val scalaHl = rsr.highlighting.asScala
          val key = doc.get("id").get.toString
          scalaHl.get(key) match {
            case Some(v) => Some(v.asScala.toMap)
            case _ => None
          }
        } else {
          None
        }
        Pair(doc.toMap,hl)})
      val facetCounts = rsr.facetCounts

      val facets: Map[String,Map[String,Int]] = if (facetCounts != null) {
        facetCounts.facetFields.asScala.map(ffCountPair => {
          (ffCountPair._1, parseFacetCounts(ffCountPair._2.asScala.toList).toMap)
        }).toMap
      } else {
        Map.empty
      }
      Future(SearchResults(rsr.responseHeader,
                           Response(createRecord, creator, rsr.response.numFound,
                                    rsr.response.start, joinedDocs, fallOf, min, facets)))

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
    // Ugly
    val qse = queryString(params ++
                      logger.queryIdToken.map("magicLoggingToken" -> _).toList)

    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, qse.toString)
    // Here be dragons! If you have multiple backends with shared IPs this could very well explode
    // but finagle doesn't seem to properly set the http host header for http/1.1
    request.addHeader(HttpHeaders.Names.HOST, servers.head)

    client(request).map(response => {
      response.getStatus match {
        case HttpResponseStatus.OK => response.getContent.toString(CharsetUtil.UTF_8)
        case status => throw SolrResponseException(status.getCode, status.getReasonPhrase, solrName, qse.toString)
      }
    })
  }

}

/** Logging and Timing solr trait */
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
  //Log the number of results
  def resultCount(name: String, count: Int): Unit = {
  }
}

/** The default logger, does nothing. */
object NoopQueryLogger extends SolrQueryLogger {
  override def log(name: String, msg: String, time: Long) = Unit
  override def debug(msg: String) = println(msg)
  override def resultCount(name: String, count:Int) = println("Got back "+count+" results while querying "+name)
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


  def where[F](c: M => Clause[F]): QueryBuilder[M, Unordered, Unlimited, defaultMM, NoSelect, NoHighlighting, NoQualityFilter, NoMinimumFacetCount, Unlimited] = {
    QueryBuilder(self, c(self), filters=Nil, boostQueries=Nil, queryFields=Nil,
                 phraseBoostFields=Nil, boostFields=Nil, start=None, limit=None,
                 tieBreaker=None, sort=None, minimumMatch=None ,queryType=None,
                 fieldsToFetch=Nil, facetSettings=FacetSettings(facetFieldList=Nil,
                                                                facetMinCount=None,
                                                                facetLimit=None),
                 hls=None, hlFragSize=None, creator=None, comment=None, fallOf=None, min=None)
  }
  def query[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter, FC <: FacetCount, FLim](timeout: Duration, qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q, FC, FLim]): SearchResults[M, Y]
  def queryFuture[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter, FC <: FacetCount, FLim](qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q, FC, FLim]): Future[SearchResults[M, Y]]
}
trait ElasticSchema[M <: Record[M]] extends SlashemSchema[M] {
  self: M with SlashemSchema[M] =>

  def meta: ElasticMeta[M]

  def query[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter, FC <: FacetCount, FLim](timeout: Duration, qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q, FC, FLim]):
  SearchResults[M, Y] = {
    queryFuture(qb, Some(timeout))(timeout)
  }

  def queryFuture[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter, FC <: FacetCount, FLim](qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q, FC, FLim]):
  Future[SearchResults[M, Y]] = {
    elasticQueryFuture(qb, buildElasticQuery(qb), None)
  }
  /*
   * queryFuture constructs a future query
   * @qb: The query builder representing the query to be executed
   * @timeoutOpt: An option type that requests a server side timeout for the query
   */
  def queryFuture[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter, FC <: FacetCount, FLim](qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q, FC, FLim], timeoutOpt: Option[Duration]):
  Future[SearchResults[M, Y]] = {
    elasticQueryFuture(qb, buildElasticQuery(qb), timeoutOpt)
  }

  def elasticQueryFuture[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter, FC <: FacetCount, FLim](qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q, FC, FLim], query: ElasticQueryBuilder, timeoutOpt: Option[Duration]): Future[SearchResults[M, Y]] = {
    val esfp = meta.executorServiceFuturePool

    val searchResultsFuture = esfp {
      val client = meta.client
      val from = qb.start.map(_.toInt).getOrElse(qb.DefaultStart)
      val limit =  qb.limit.map(_.toInt).getOrElse(qb.DefaultLimit)
      meta.logger.debug("Query details "+query.toString())
      val baseRequest: SearchRequestBuilder = client.prepareSearch(meta.indexName)
      .setQuery(query)
      .setFrom(from)
      .setSize(limit)
      .setSearchType(SearchType.QUERY_THEN_FETCH)
      val request = qb.sort match {
        case None => baseRequest
        //Handle sorting by fields quickly
        case Some(Pair(Field(fieldName),"asc")) => baseRequest.addSort(fieldName,SortOrder.ASC)
        case Some(Pair(Field(fieldName),"desc")) => baseRequest.addSort(fieldName,SortOrder.DESC)
        //Handle sorting by scripts in general
        case Some(Pair(sort,dir)) => {
          val (params,scriptSrc) = sort.elasticBoost()
          val paramNames = (1 to params.length).map("p"+_)
          val script = scriptSrc.format(paramNames:_*)
          val keyedParams =  paramNames zip params
          val sortOrder =  dir match {
            case "asc" => SortOrder.ASC
            case "desc" => SortOrder.DESC
          }
          val sortBuilder = new ScriptSortBuilder(script,"number").order(sortOrder)
          keyedParams.foreach(p => {sortBuilder.param(p._1,p._2)})
          baseRequest.addSort(sortBuilder)
        }
        case _ => baseRequest
      }

      /* Set the server side timeout */
      val timeLimmitedRequest = timeoutOpt match {
        case Some(timeout) => request.setTimeout(TimeValue.timeValueMillis(timeout.inMillis))
        case _ => request
      }

      /* Add a facet to the request */
      val facetedRequest = qb.facetSettings.facetFieldList match {
        case Nil => timeLimmitedRequest
        case _ => {
          termFacetQuery(qb.facetSettings.facetFieldList, qb.facetSettings.facetLimit).foreach(timeLimmitedRequest.addFacet(_))
          timeLimmitedRequest
        }
      }
      val response : SearchResponse = facetedRequest.execute().get()
      response
    }

    val queryText = query.toString()

    timeFuture(searchResultsFuture).map( {
      case (queryTime, result) => {
        meta.logger.log("e" + meta.indexName + ".query",queryText, queryTime)
        result
      }}).map({
      response =>
      meta.logger.debug("Search response "+response.toString())
      val results = constructSearchResults(qb.creator,
                                           qb.start.map(_.toInt).getOrElse(qb.DefaultStart),
                                           qb.fallOf,
                                           qb.min,
                                           response)

      results
    })
    .onSuccess((v: SearchResults[M,Y]) => {
      meta.logger.success("e"+meta.indexName)
      meta.logger.resultCount("e"+meta.indexName,v.response.numFound)})
    .onFailure(e => meta.logger.failure("e"+meta.indexName, queryText, e))

  }
  def constructSearchResults[Y](creator: Option[Response.RawDoc => Y],
                                start: Int,
                                fallOff: Option[Double],
                                min: Option[Int],
                                response: SearchResponse): SearchResults[M, Y] = {
    val time = response.tookInMillis()
    val hitCount = response.getHits().totalHits().toInt
    val esHits = response.getHits().getHits()
    val docs: Array[(Map[String,Any], Option[Map[String,java.util.ArrayList[String]]])] = esHits.map(doc => {
      val m = doc.sourceAsMap()
      val annotedMap = (m.asScala ++ List("score" -> doc.score().toDouble)).toMap
      val hlf = doc.getHighlightFields()
      if (hlf == null) {
        Pair(annotedMap,None)
      } else {
        Pair(annotedMap,
             Some(doc.getHighlightFields().asScala
                  .mapValues(v => {
                    val fragments: Array[String] = v.getFragments()
                    val fragmentList: List[String] = Nil++fragments
                    new ArrayList(fragmentList.asJava) }
                           )
                  .toMap))
      }
    })

    val fieldFacet: Map[String,Map[String,Int]] = {
      val facets = response.facets()
      if (facets != null) {
        facets.facets().asScala.filter(_.getType() == "terms").
        map(f => f.asInstanceOf[InternalStringTermsFacet]).
        map(f => f.name() -> (f.getEntries().asScala.map(t => t.term() -> t.count())).toMap).toMap
      } else {
        Map.empty
      }
    }

   SearchResults(ResponseHeader(200,time.toInt),
                 Response(this, creator, hitCount, start, docs,
                        fallOff=fallOff, min=min, fieldFacet))
  }
  def buildElasticQuery[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter, FC <: FacetCount, FLim](qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q, FC, FLim]): ElasticQueryBuilder = {
    val baseQuery: ElasticQueryBuilder= qb.clauses.elasticExtend(qb.queryFields,
                                                                 qb.phraseBoostFields,
                                                                 qb.minimumMatch)
    //Apply filters if necessary
    val fq = qb.filters match {
      case Nil => baseQuery
      case _ => filteredQuery(baseQuery,combineFilters(qb.filters.map(_.elasticFilter(qb.queryFields))))
    }
    //Apply any custom scoring rules (aka emulating Solr's bq/bf)
    val boostedQuery = qb.boostFields match {
      case Nil => fq
      case _ => boostFields(fq, qb.boostFields)
    }
    boostedQuery
  }
  def termFacetQuery(facetFields: List[Ast.Field], facetLimit: Option[Int]): List[AbstractFacetBuilder] = {
    val fieldNames = facetFields.map(_.boost())
    val facetQueries = fieldNames.map(name => {
      val q = new TermsFacetBuilder(name).field(name)
      facetLimit match {
        case Some(c) => {
          q.size(c)
        }
        case _ => q
      }
    }
    )
    facetQueries
  }
  def boostFields(query: ElasticQueryBuilder, boostFields: List[ScoreBoost]): ElasticQueryBuilder =  {
    val boostedQuery = new CustomScoreQueryBuilder(query)
    val boostedQuerys = boostFields.map(_.elasticBoost)
    val params = boostedQuerys.flatMap(_._1)
    val scriptSrc = boostedQuerys.map(_._2).mkString(" + ")
    val paramNames = (1 to params.length).map("p"+_)
    println("using param names"+paramNames+" from params "+params+"on a query string of "+scriptSrc)
    val script = scriptSrc.format(paramNames:_*)
    val keyedParams =  paramNames zip params
    keyedParams.foreach(p => {boostedQuery.param(p._1,p._2)})
    val scoreScript = "_score * (1 +"+ script + " )"
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

  def queryParams[Ord, Lim, MM <: MinimumMatchType, Select, H <: Highlighting, Q <: QualityFilter, FC <: FacetCount, FLim](qb: QueryBuilder[M, Ord, Lim, MM, Select, H, Q, FC, FLim]): Seq[(String, String)] = queryParamsWithBounds(qb,qb.start, qb.limit)

  def queryParamsWithBounds[Ord, Lim, MM <: MinimumMatchType, Select, H <: Highlighting, Q <: QualityFilter, FC <: FacetCount, FLim](qb: QueryBuilder[M, Ord, Lim, MM, Select, H, Q, FC, FLim], qstart: Option[Long], qrows: Option[Long]): Seq[(String,String)] = {
    val bounds = List(("start" -> (qstart.getOrElse {qb.DefaultStart}).toString),
                 ("rows" -> (qrows.getOrElse {qb.DefaultLimit}).toString))
    bounds ++ queryParamsNoBounds(qb)
  }

  //This is the part which generates most of the solr request
  def queryParamsNoBounds[Ord, Lim, MM <: MinimumMatchType, Select, H <: Highlighting, Q <: QualityFilter, FC <: FacetCount, FLim](qb: QueryBuilder[M, Ord, Lim, MM, Select, H, Q, FC, FLim]): Seq[(String,String)] = {

    //The actual query
    val p = List(("q" -> qb.clauses.extend))

    val s = qb.sort match {
      case None => Nil
      case Some(sort) => List("sort" -> (sort._1.boost + " " + sort._2))
    }

    //The query type. Most likely edismax or dismax
    val qt = qb.queryType match {
      case None => Nil
      case Some(method) => List("defType" -> method)
    }

    // Minimum match. If this is set to 100% then it is the same as setting
    // the default operation as AND
    val mm = qb.minimumMatch match {
      case None => Nil
      case Some(mmParam) => List("mm" -> mmParam)
    }

    //Facet field
    val ff = qb.facetSettings.facetFieldList match {
      case Nil => Nil
      case _ => ("facet" -> "true")::(qb.facetSettings.facetFieldList.map(field => "facet.field" -> field.boost))
    }

    //Facet settings
    val fs = (qb.facetSettings.facetMinCount match {
      case None => Nil
      case Some(x) => List("facet.mincount" -> x.toString)
    }) ++ (qb.facetSettings.facetLimit match {
      case None => Nil
      case Some(x) => List("facet.limit" -> x.toString)
    })

    //Boost queries only impact scoring
    val bq = qb.boostQueries.map({ x => ("bq" -> x.extend)})

    val qf = qb.queryFields.filter({x => x.weight != 0}).map({x => ("qf" -> x.boost)})

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

    val hlp = (qb.hls,qb.hlFragSize) match {
      case (Some(a),Some(b)) => List("hl" -> a, "hl.fragsize" -> b.toString)
      case (Some(a),None) => List("hl" -> a)
      case (None,_) => Nil
    }

    val bf = qb.boostFields.map({x => ("bf" -> x.boost)})

    val f = qb.filters.map({x => ("fq" -> x.extend)})

    val ct = qb.comment match {
      case None => Nil
      case Some(a) => List("comment" -> a)
    }

     ct ++ t ++ mm ++ qt ++ bq ++ qf ++ p ++ s ++ f ++ pf ++ fl ++ bf ++ hlp ++ ff ++ fs
  }


  def query[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter, FC <: FacetCount, FLim](timeout: Duration, qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q, FC, FLim]):
  SearchResults[M, Y] = {
    queryFuture(qb)(timeout)
  }

  def queryFuture[Ord, Lim, MM <: MinimumMatchType, Y, H <: Highlighting, Q <: QualityFilter, FC <: FacetCount, FLim](qb: QueryBuilder[M, Ord, Lim, MM, Y, H, Q, FC, FLim]):
  Future[SearchResults[M, Y]] = {
    solrQueryFuture(qb.creator, queryParams(qb), qb.fieldsToFetch, qb.fallOf, qb.min)
  }
  //The query builder calls into this to do actually execute the query.
  def solrQueryFuture[Y](creator: Option[Response.RawDoc => Y],
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
    .onSuccess((v: SearchResults[M,Y]) => {
      meta.logger.success(meta.solrName)
      meta.logger.resultCount(meta.solrName,v.response.numFound)})
    .onFailure(e => meta.logger.failure(meta.solrName, queryText, e))
  }

}


trait SlashemField[V, M <: Record[M]] extends OwnedField[M] {
  self: Field[V, M] =>
  import Helpers._

  //Note eqs and neqs results in phrase queries!
  def eqs(v: V) = Clause[V](self.queryName, Group(Phrase(v)))
  def neqs(v: V) = Clause[V](self.queryName, Phrase(v),false)
  //With a boost
  def eqs(v: V, b: Float) = Clause[V](self.queryName, Boost(Group(Phrase(v)),b))
  def neqs(v: V, b:Float) = Clause[V](self.queryName, Boost(Phrase(v),b),false)


  //This allows for bag of words style matching.
  def contains(v: V) = Clause[V](self.queryName, Group(BagOfWords(v)))
  def contains(v: V, b: Float) = Clause[V](self.queryName, Boost(Group(BagOfWords(v)),b))

  def in(v: Iterable[V]) = Clause[V](self.queryName, groupWithOr(v.map({x: V => Phrase(x)})))
  def nin(v: Iterable[V]) = Clause[V](self.queryName, groupWithOr(v.map({x: V => Phrase(x)})),false)

  def in(v: Iterable[V], b: Float) = Clause[V](self.queryName, Boost(groupWithOr(v.map({x: V => Phrase(x)})),b))
  def nin(v: Iterable[V], b: Float) = Clause[V](self.queryName, Boost(groupWithOr(v.map({x: V => Phrase(x)})),b),false)

  def inRange(v1: V, v2: V) = Clause[V](self.queryName, Group(Range(BagOfWords(v1),BagOfWords(v2))))
  def ninRange(v1: V, v2: V) = Clause[V](self.queryName, Group(Range(BagOfWords(v1),BagOfWords(v2))),false)

  def lessThan(v: V) = Clause[V](self.queryName, Group(Range(Splat[V](),BagOfWords[V](v))))
  def greaterThan(v: V) = Clause[V](self.queryName, Group(Range(BagOfWords[V](v),Splat[V]())))


  def any = Clause[V](self.queryName,Splat[V]())

  def query(q: Query[V]) = Clause[V](self.queryName, q)

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

  // Allow for a seperate name to be used for queries
  // useful for ES where a name might be stored as "name"
  // and then indexed as "name.edgengram" etc.
  def queryName = name

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
  import Helpers._
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
  def contains(item: Int) = {
    Clause[Int](queryName, Phrase(item))
  }
  /**
   * See if this list has any elements in that list.
   * @param List[Int] the list to check for any intersections.
   */
  def in(lst: List[Int]) = Clause[Int](queryName, groupWithOr(lst.map({i: Int => Phrase(i)})))
  def nin(lst: List[Int]) = Clause[Int](queryName, groupWithOr(lst.map({i: Int => Phrase(i)})),false)
}

class SlashemStringListField[T <: Record[T]](owner: T) extends StringListField[T](owner) with SlashemField[List[String], T] {
  import Helpers._
  override def valueBoxFromAny(a: Any) = {
    try {
      a match {
        case "" => Full(List(""))
        case strArr:     Array[String]  => Full(strArr.toList)
        case intArr:     Array[Int]     => Full(intArr.toList.map(int => int.toString))
        case integerArr: Array[Integer] => Full(integerArr.toList.map(integer => integer.toString))
        case _ => Empty
      }
    } catch {
      case _ => Empty
    }
  }
  def contains(item: String) = {
    Clause[String](queryName, Phrase(item))
  }
  /**
   * See if this list has any elements in that list.
   * @param List[String] the list to check for any intersections.
   */
  def in(v: List[String]) = Clause[String](queryName, groupWithOr(v.map({s: String => Phrase(s)})))
  def nin(v: List[String]) = Clause[String](queryName, groupWithOr(v.map({s: String => Phrase(s)})),false)
}

class SlashemLongListField[T <: Record[T]](owner: T) extends LongListField[T](owner) with SlashemField[List[Long], T] {
  import Helpers._
  override def valueBoxFromAny(a: Any) = {
    try {
      a match {
        case long:   Long => Full(List(long))
        case strArr: Array[Long] => Full(strArr.toList)
        case intArr: Array[Int]  => Full(intArr.toList.map(int => int.toLong))
        case str:    String => Full(str.split(" ").map(s => s.toLong).toList)
        case _ => Empty
      }
    } catch {
      case _ => Empty
    }
  }
  def contains(item: Long) = {
    Clause[Long](queryName, Phrase(item))
  }
  /**
   * See if this list has any elements in that list.
   * @param List[Long] the list to check for any intersections.
   */
  def in(lst: List[Long]) = Clause[Long](queryName, groupWithOr(lst.map({l: Long => Phrase(l)})))
  def nin(lst: List[Long]) = Clause[Long](queryName, groupWithOr(lst.map({l: Long => Phrase(l)})),false)
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
  def inBounds(bounds: GeoCover, maxCells: Int = owner.geohash.maxCells) = {
    val cellIds = bounds.boundsCoverString(maxCells = maxCells)
    cellIds match {
      case Nil => this.any
      case _ => this.in(cellIds)
    }
  }
}
// Legacy field name, in the future simply use Slashem*FieldName*
//Slashem field types
class SolrStringField[T <: Record[T]](owner: T) extends SlashemStringField[T](owner)
//Allows for querying against the default filed in solr. This field doesn't have a name
class SolrDefaultStringField[T <: Record[T]](owner: T) extends SlashemDefaultStringField[T](owner)
class SolrIntField[T <: Record[T]](owner: T) extends SlashemIntField[T](owner)
class SolrDoubleField[T <: Record[T]](owner: T) extends SlashemDoubleField[T](owner)
class SolrLongField[T <: Record[T]](owner: T) extends SlashemLongField[T](owner)
class SolrObjectIdField[T <: Record[T]](owner: T) extends SlashemObjectIdField[T](owner)
class SolrIntListField[T <: Record[T]](owner: T) extends SlashemIntListField[T](owner)
class SolrLongListField[T <: Record[T]](owner: T) extends SlashemLongListField[T](owner)
class SolrStringListField[T <: Record[T]](owner: T) extends SlashemStringListField[T](owner)
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

class LongListField[T <: Record[T]](override val owner: T) extends Field[List[Long], T] {
  type ValueType = List[Long]
  var e: Box[ValueType] = Empty

  def setFromString(s: String) = {
    Full(set(s.split(" ").map(x => x.toLong).toList))
  }
  override def setFromAny(a: Any) ={
    try {
      a match {
        case "" => Empty
        case ar: Array[Long] => Full(set(ar.toList))
        case ar: Array[Integer] => Full(set(ar.toList.map(x => x.longValue)))
        case s: String => Full(set(s.split(" ").map(x => x.toLong).toList))
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

class StringListField[T <: Record[T]](override val owner: T) extends Field[List[String], T] {
  type ValueType = List[String]
  var e: Box[ValueType] = Empty
  def setFromString(s: String) = {
    Full(set(s.split(" ").toList))
  }
  override def setFromAny(a: Any) = {
    try {
      a match {
        case "" => Empty
        case arr: Array[String] => Full(arr.toList)
        case str: String        => setFromString(str)
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
