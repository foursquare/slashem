package com.foursquare.solr
import com.foursquare.solr.Ast._
import net.liftweb.record.{Record, OwnedField, Field, MetaRecord}
import net.liftweb.record.field.{StringField, IntField, DoubleField}
import net.liftweb.common.{Box, Empty, Full}
import scalaj.http._

import net.liftweb.json.JsonParser
import org.bson.types.ObjectId
import org.codehaus.jackson._
import org.codehaus.jackson.annotate._
import org.codehaus.jackson.map._

import java.util.HashMap
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

  def host: String
  def port: String

  val queryUrl = "http://%s:%s/solr/select".format(host, port)

  // This method performs the actually query / http request. It should probably
  // go in another file when it gets more sophisticated.
  def rawQuery(params: Seq[(String, String)]): String = {
    val request = Http(queryUrl).params(("wt" -> "json") :: params.toList).options(HttpOptions.connTimeout(10000), HttpOptions.readTimeout(50000))
    println(request.getUrl.toString)
    val result = request.asString
//    println(result)
    result
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
    QueryBuilder(self, List(c(self)), filters=Nil, boostQueries=Nil, queryFields=Nil, phraseBoostFields=Nil, start=None, limit=None, sort=None, minimumMatch=None ,queryType=None, fieldsToFetch=Nil)
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
class SolrObjectIdField[T <: Record[T]](owner: T) extends ObjectIdField[T](owner) with SolrField[ObjectId, T]

// This insanity makes me want to 86 Record all together. DummyField allows us
// to easily define our own Field types. I use this for ObjectId so that I don't
// have to import all of MongoRecord. We could trivially reimplement the other
// Field types using it.
class ObjectIdField[T <: Record[T]](override val owner: T) extends Field[ObjectId, T] {

  type ValueType = ObjectId
  var e : Box[ValueType] = Empty

  def setFromString(s: String) = Full(set(new ObjectId(s)))
  override def setFromAny(a: Any) = a match {
    case s : String => Full(set(new ObjectId(s)))
    case i : ObjectId => Full(set(i))
    case _ => Empty
  }
  override def setFromJValue(jv: net.liftweb.json.JsonAST.JValue) = Empty
  override def liftSetFilterToBox(a: Box[ObjectId]) = Empty
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
