package com.foursquare.solr
import com.foursquare.solr.Ast._
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

import java.util.HashMap
import java.util.concurrent.TimeUnit
import java.net.InetSocketAddress
import collection.JavaConversions._



case class ResponseHeader @JsonCreator()(@JsonProperty("status")status: Int, @JsonProperty("QTime")QTime: Int)

case class Response @JsonCreator()(@JsonProperty("numFound")numFound: Int, @JsonProperty("start")start: Int, @JsonProperty("docs")docs: Array[HashMap[String,Any]]) {
  //Convert the ArrayList to the concrete type using magic
  def results[T <: Record[T]](B : Record[T]) : List[T] = {
    docs.map({doc => val q = B.meta.createRecord
              doc.foreach({a =>
                val fname = a._1
                val value = a._2
                q.fieldByName(fname).map(_.setFromAny(value))})
              q.asInstanceOf[T]
            }).toList
  }
}

case class SearchResults @JsonCreator()(@JsonProperty("responseHeader") responseHeader: ResponseHeader,
                                          @JsonProperty("response") response: Response)

trait SolrMeta[T <: Record[T]] extends MetaRecord[T] {
  self: MetaRecord[T] with T =>

  def servers: List[String]

  val queryUrl = "http://%s/solr/select".format(servers.head)

  //Params semi-randomly chosen
  def client = ClientBuilder()
  .codec(Http())
  .hosts(servers.map(x => {val h = x.split(":")
                         val s = h.head
                         val p = h.last
                         new InetSocketAddress(s,p.toInt)}))
  .hostConnectionLimit(10)
  .retries(3)
  .build()

  // This method performs the actually query / http request. It should probably
  // go in another file when it gets more sophisticated.
  def rawQuery(params: Seq[(String, String)]): String = {
    val response = rawQueryFuture(params)(Duration(10,TimeUnit.SECONDS))
    val str = response.getContent.toString(CharsetUtil.UTF_8)
    str
  }
  def rawQueryFuture(params: Seq[(String, String)]): Future[HttpResponse] = {
    //Ugly :(
    val qse = new QueryStringEncoder("/solr/select")
    (("wt" -> "json") :: params.toList).map(x => qse.addParam(x._1,x._2))
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, qse.toString)
    //Here be dragons! If you have multiple backends with shared IPs this could very well explode
    //but finagle doesn't seem to properly set the http host header for http/1.1
    request.addHeader(HttpHeaders.Names.HOST, servers.head);
    val resultFuture = client(request)
    resultFuture
  }

}

trait SolrSchema[M <: Record[M]] extends Record[M] {
  self: M with Record[M] =>

  def meta: SolrMeta[M]

  implicit val formats = net.liftweb.json.DefaultFormats
  val mapper = {
    val a = new ObjectMapper
    a.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    a
  }

  // 'Where' is the entry method for a SolrRogue query.
  def where[F](c: M => Clause[F]): QueryBuilder[M, Unordered, Unlimited, defaultMM] = {
    QueryBuilder(self, List(c(self)), filters=Nil, boostQueries=Nil, queryFields=Nil, phraseBoostFields=Nil, boostFields=Nil, start=None, limit=None, tieBreaker=None, sort=None, minimumMatch=None ,queryType=None, fieldsToFetch=Nil)
  }
  def query(params: Seq[(String, String)], fieldstofetch: List[String]) : SearchResults = {
    val r = meta.rawQuery(params)
    extractFromResponse(r,fieldstofetch)
  }
  def extractFromResponse(r : String, fieldstofetch: List[String]=Nil): SearchResults = {
    mapper.readValue(r,classOf[SearchResults])
  }

}

trait SolrField[V, M <: Record[M]] extends OwnedField[M] {
  self: Field[V, M] =>
  import Helpers._

  def eqs(v: V) = Clause[V](self.name, Group(Plus(Phrase(v))))
  def neqs(v: V) = Clause[V](self.name, Minus(Phrase(v)))

  def contains(v: V) = Clause[V](self.name, Group(BagOfWords(v)))
  def contains(v: V, b: Float) = Clause[V](self.name, Boost(Group(BagOfWords(v)),b))

   def in(v: Iterable[V]) = Clause[V](self.name, groupWithOr(v.map({x: V => Plus(Phrase(x))})))
  def nin(v: Iterable[V]) = Clause[V](self.name, groupWithAnd(v.map({x: V => Minus(Phrase(x))})))

  def inRange(v1: V, v2: V) = Clause[V](self.name, Group(Plus(Range(v1,v2))))
  def ninRange(v1: V, v2: V) = Clause[V](self.name, Group(Minus(Range(v1,v2))))

  def query(q: Query[V]) = Clause[V](self.name, q)
}

//
class SolrStringField[T <: Record[T]](owner: T) extends StringField[T](owner, 0) with SolrField[String, T]
class SolrDefaultStringField[T <: Record[T]](owner: T) extends StringField[T](owner, 0) with SolrField[String, T] {
  override def name = ""
}
class SolrIntField[T <: Record[T]](owner: T) extends IntField[T](owner) with SolrField[Int, T]
class SolrDoubleField[T <: Record[T]](owner: T) extends DoubleField[T](owner) with SolrField[Double, T]
class SolrLongField[T <: Record[T]](owner: T) extends LongField[T](owner) with SolrField[Long, T]
class SolrObjectIdField[T <: Record[T]](owner: T) extends ObjectIdField[T](owner) with SolrField[ObjectId, T]
class SolrIntListField[T <: Record[T]](owner: T) extends IntListField[T](owner) with SolrField[List[Int], T]
class SolrBooleanField[T <: Record[T]](owner: T) extends BooleanField[T](owner) with SolrField[Boolean, T]

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
  override def setFromString(s: String) = Empty
  override def setFromAny(a: Any) = Empty
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
