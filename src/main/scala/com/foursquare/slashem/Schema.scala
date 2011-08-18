package com.foursquare.slashem
import com.foursquare.slashem.Ast._
import net.liftweb.record.{Record, OwnedField, Field, MetaRecord}
import net.liftweb.record.field.{BooleanField, LongField, StringField, IntField, DoubleField}
import net.liftweb.common.{Box, Empty, Full}
import com.twitter.util.{Duration, Future}
import com.twitter.finagle._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.finagle.util._
import net.liftweb.json.JsonParser
import org.bson.types.ObjectId
import org.codehaus.jackson._
import org.codehaus.jackson.annotate._
import org.codehaus.jackson.map._
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._

import org.joda.time.DateTime
import java.util.HashMap
import java.util.concurrent.TimeUnit
import java.lang.Integer
import java.net.InetSocketAddress
import collection.JavaConversions._

//The response header. There are normally more fields in the response header we could extract, but
//we don't at present.
case class ResponseHeader @JsonCreator()(@JsonProperty("status")status: Int, @JsonProperty("QTime")QTime: Int)

//The response its self. The "docs" field is not type safe, you should use one of results or oids to access the results
case class Response[T <: Record[T]] (schema: T, numFound: Int, start: Int, docs: Array[HashMap[String,Any]]) {
  //Convert the ArrayList to the concrete type using magic
  def results[T <: Record[T]](B: Record[T]): List[T] = {
    docs.map({doc => val q = B.meta.createRecord
              doc.foreach({a =>
                val fname = a._1
                val value = a._2
                q.fieldByName(fname).map(_.setFromAny(value))})
              q.asInstanceOf[T]
            }).toList
  }
  def results: List[T] = results(schema)
  //Special for extracting just ObjectIds without the overhead of record.
  def oids: List[ObjectId] = {
    docs.map({doc => doc.find(x => x._1 == "id").map(x => new ObjectId(x._2.toString))}).toList.flatten
  }
}

//The search results class, you are probably most interested in the contents of response
case class SearchResults[T <: Record[T]] (responseHeader: ResponseHeader,
                             response: Response[T])


//This is the raw representation of the response from solr, you probably don't want to poke at it directly.
case class RawResponse @JsonCreator()(@JsonProperty("numFound")numFound: Int, @JsonProperty("start")start: Int, @JsonProperty("docs")docs: Array[HashMap[String,Any]])

//This is the raw representation of the response from solr, you probably don't want to poke at it directly.
case class RawSearchResults @JsonCreator()(@JsonProperty("responseHeader") responseHeader: ResponseHeader,
                                          @JsonProperty("response") response: RawResponse)


trait SolrMeta[T <: Record[T]] extends MetaRecord[T] {
  self: MetaRecord[T] with T =>

  //The servers is a list used in round-robin for running solr read queries against.
  //It can just be one element if you wish
  def servers: List[String]

  //The name is used to determine which props to use as well as for logging
  def solrName: String

  var logger: SolrQueryLogger = NoopQueryLogger

  //Params semi-randomly chosen
  def client = ClientBuilder()
    .codec(Http())
    .hosts(servers.map(x => {val h = x.split(":")
                             val s = h.head
                             val p = h.last
                             new InetSocketAddress(s, p.toInt)}))
    .hostConnectionLimit(1000)
    .hostConnectionCoresize(300)
    .retries(3)
    .build()

  //This is used so the json extractor can do its job
  implicit val formats = net.liftweb.json.DefaultFormats
  val mapper = {
    val a = new ObjectMapper
    //We don't extract all of the fields so we ignore unknown properties.
    a.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    a
  }

  def extractFromResponse(r: String, fieldstofetch: List[String]=Nil): SearchResults[T] = {
    //This intentional avoids lift extract as it is too slow for our use case.
    logger.log(solrName + ".jsonExtract", "extacting json") {
      val rsr = try {
        mapper.readValue(r, classOf[RawSearchResults])
      } catch {
        case e => throw new Exception("An error occured while parsing solr result \""+r+"\"",e)
      }
      //Take the raw search result and make the type templated search result.
      SearchResults(rsr.responseHeader, Response(createRecord, rsr.response.numFound, rsr.response.start, rsr.response.docs))
    }
  }

  // This method performs the actually query / http request. It should probably
  // go in another file when it gets more sophisticated.
  def rawQuery(params: Seq[(String, String)]): String = {
    val response = rawQueryFuture(params)(Duration(10, TimeUnit.SECONDS))
    response
  }
  def rawQueryFuture(params: Seq[(String, String)]): Future[String] = {
    //Ugly :(
    val qse = new QueryStringEncoder("/solr/select")

    (("wt" -> "json") :: params.toList).foreach { x =>
      qse.addParam(x._1, x._2)
    }

    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, qse.toString)
    //Here be dragons! If you have multiple backends with shared IPs this could very well explode
    //but finagle doesn't seem to properly set the http host header for http/1.1
    request.addHeader(HttpHeaders.Names.HOST, servers.head);

    logger.log(solrName+".rawQuery", request.getUri) {
      client(request).map(_.getContent.toString(CharsetUtil.UTF_8))
    }
  }

}
//If you want to get some simple logging/timing implement this trait
trait SolrQueryLogger {
  def log[T](name: String, msg :String)(f: => T): T
}
//The default logger, does nothing.
object NoopQueryLogger extends SolrQueryLogger {
  override def log[T](name: String, msg :String)(f: => T): T = {
    f
  }
}

//If you want any of the geo queries you will have to implement this
trait SolrGeoHash {
  def coverString (geoLat : Double, geoLong : Double, radiusInMeters : Int, maxCells: Int ): Seq[String]
  def rectCoverString(topRight: (Double, Double), bottomLeft: (Double, Double), maxCells: Int = 0, minLevel: Int = 0, maxLevel: Int = 0): Seq[String]
  def maxCells: Int = 0
}
//Default geohash, does nothing.
object NoopSolrGeoHash extends SolrGeoHash {
  def coverString (geoLat : Double, geoLong : Double, radiusInMeters : Int, maxCells: Int ) : Seq[String] = List("pleaseUseaRealGeoHash")
  def rectCoverString(topRight: (Double, Double), bottomLeft: (Double, Double), maxCells: Int = 0, minLevel: Int = 0, maxLevel: Int = 0): Seq[String] = List("pleaseUseaRealGeoHash")
}

trait SolrSchema[M <: Record[M]] extends Record[M] {
  self: M with Record[M] =>

  def meta: SolrMeta[M]

  //Set me to something which collects timing if you want (hint: you do)
  var geohash: SolrGeoHash = NoopSolrGeoHash


  // 'Where' is the entry method for a SolrRogue query.
  def where[F](c: M => Clause[F]): QueryBuilder[M, Unordered, Unlimited, defaultMM] = {
    QueryBuilder(self, c(self), filters=Nil, boostQueries=Nil, queryFields=Nil, phraseBoostFields=Nil, boostFields=Nil, start=None, limit=None, tieBreaker=None, sort=None, minimumMatch=None ,queryType=None, fieldsToFetch=Nil)
  }

  //The query builder calls into this to do actually execute the query.
  def query(params: Seq[(String, String)], fieldstofetch: List[String]): SearchResults[M] = {
    val jsonResponse = meta.rawQuery(params)
    meta.extractFromResponse(jsonResponse, fieldstofetch)
  }
  //The query builder calls into this to do actually execute the query.
  def queryFuture(params: Seq[(String, String)], fieldstofetch: List[String]): Future[SearchResults[M]] = {
    val jsonResponseFuture = meta.rawQueryFuture(params)
    jsonResponseFuture.map(meta.extractFromResponse(_, fieldstofetch))
  }

}

trait SolrField[V, M <: Record[M]] extends OwnedField[M] {
  self: Field[V, M] =>
  import Helpers._

  //Not eqs and neqs results in phrase queries!
  def eqs(v: V) = Clause[V](self.name, Group(Plus(Phrase(v))))
  def neqs(v: V) = Clause[V](self.name, Minus(Phrase(v)))

  //This allows for bag of words style matching.
  def contains(v: V) = Clause[V](self.name, Group(BagOfWords(v)))
  def contains(v: V, b: Float) = Clause[V](self.name, Boost(Group(BagOfWords(v)),b))

  def in(v: Iterable[V]) = Clause[V](self.name, Plus(groupWithOr(v.map({x: V => Phrase(x)}))))
  def nin(v: Iterable[V]) = Clause[V](self.name, Minus(groupWithAnd(v.map({x: V => Phrase(x)}))))

  def in(v: Iterable[V], b: Float) = Clause[V](self.name, Boost(Plus(groupWithOr(v.map({x: V => Phrase(x)}))),b))
  def nin(v: Iterable[V], b: Float) = Clause[V](self.name, Boost(Minus(groupWithAnd(v.map({x: V => Phrase(x)}))),b))


  def inRange(v1: V, v2: V) = Clause[V](self.name, Group(Plus(Range(v1,v2))))
  def ninRange(v1: V, v2: V) = Clause[V](self.name, Group(Minus(Range(v1,v2))))

  def any = Clause[V](self.name,Splat[V]())

  def query(q: Query[V]) = Clause[V](self.name, q)
}

//Solr field types
class SolrStringField[T <: Record[T]](owner: T) extends StringField[T](owner, 0) with SolrField[String, T]
//Allows for querying against the default filed in solr. This field doesn't have a name
class SolrDefaultStringField[T <: Record[T]](owner: T) extends StringField[T](owner, 0) with SolrField[String, T] {
  override def name = ""
}
class SolrIntField[T <: Record[T]](owner: T) extends IntField[T](owner) with SolrField[Int, T]
class SolrDoubleField[T <: Record[T]](owner: T) extends DoubleField[T](owner) with SolrField[Double, T]
class SolrLongField[T <: Record[T]](owner: T) extends LongField[T](owner) with SolrField[Long, T]
class SolrObjectIdField[T <: Record[T]](owner: T) extends ObjectIdField[T](owner) with SolrField[ObjectId, T]
class SolrIntListField[T <: Record[T]](owner: T) extends IntListField[T](owner) with SolrField[List[Int], T]
class SolrBooleanField[T <: Record[T]](owner: T) extends BooleanField[T](owner) with SolrField[Boolean, T]
class SolrDateTimeField[T <: Record[T]](owner: T) extends JodaDateTimeField[T](owner) with SolrField[DateTime, T]
//More restrictive type so we can access the geohash
class SolrGeoField[T <: SolrSchema[T]](owner: T) extends StringField[T](owner,0) with SolrField[String, T] {
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

// This insanity makes me want to 86 Record all together. DummyField allows us
// to easily define our own Field types. I use this for ObjectId so that I don't
// have to import all of MongoRecord. We could trivially reimplement the other
// Field types using it.
class ObjectIdField[T <: Record[T]](override val owner: T) extends Field[ObjectId, T] {

  type ValueType = ObjectId
  var e : Box[ValueType] = Empty

  def setFromString(s: String) = Full(set(new ObjectId(s)))
  override def setFromAny(a: Any) ={
    try {
    a match {
      case "" => Empty
      case s : String => Full(set(new ObjectId(s)))
      case i : ObjectId => Full(set(i))
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
  override def valueBox() = e
}
class JodaDateTimeField[T <: Record[T]](override val owner: T) extends DummyField[DateTime, T](owner) {
  type ValueType = DateTime
  var e : Box[ValueType] = Empty

  override def setFromString(s: String): Box[ValueType] = {
    try {
      Full(set(new DateTime(s)))
    } catch {
      case _ => Empty
    }
  }
  override def setFromAny(a: Any): Box[ValueType] ={
    a match {
      case s : String => setFromString(s)
      case d : DateTime => Full(set(d))
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
  var e : Box[ValueType] = Empty

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
