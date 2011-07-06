package com.foursquare.solr
import Ast._
import net.liftweb.record.{Record, OwnedField, Field, MetaRecord}
import net.liftweb.record.field.{StringField, IntField, DoubleField}
import net.liftweb.common.{Box, Empty}
import scalaj.http._
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonParser
import org.bson.types.ObjectId

case class Doc(id: String, user_type: Option[String], name: Option[String],
                 lat: Option[Double], lng: Option[Double],
                 address_string: Option[String])

case class ResponseHeader(status: Int, QTime: Int)

case class Response(numFound: Int, start: Int, docs: List[Doc])

case class SearchResults(responseHeader: ResponseHeader, response: Response)

trait SolrMeta[T <: Record[T]] extends MetaRecord[T] {
  self: MetaRecord[T] with T =>

  implicit val formats = net.liftweb.json.DefaultFormats
  def host: String
  def port: String

  val queryUrl = "http://%s:%s/solr/select".format(host, port)

  // This method performs the actually query / http request. It should probably
  // go in another file when it gets more sophisticated.
  def query(params: Seq[(String, String)]): List[ObjectId] = {
    val request = Http(queryUrl).params(("wt" -> "json") :: params.toList).options(HttpOptions.connTimeout(10000), HttpOptions.readTimeout(50000))
    println(request.getUrl.toString)
    val result = request.asString
//    println(result)
    JsonParser.parse(result).extract[SearchResults].response.docs.map(d => new ObjectId(d.id))
  }
}

trait SolrSchema[M <: Record[M]] extends Record[M] {
  self: M with Record[M] =>

  def meta: SolrMeta[M]

  // 'Where' is the entry method for a SolrRogue query.
  def where[F](c: M => Clause[F]): QueryBuilder[M, Unordered, Unlimited, defaultMM] = {
    QueryBuilder(self, List(c(self)), filters=Nil, boostQueries=Nil, queryFields=Nil, phraseBoostFields=Nil, start=None, limit=None, sort=None, minimumMatch=None ,queryType=None)
  }
}

trait SolrField[V, M <: Record[M]] extends OwnedField[M] {
  self: Field[V, M] =>
  import Helpers._

  def eqs(v: V) = Clause[V](self.name, Group(Plus(Phrase(v))))
  def neqs(v: V) = Clause[V](self.name, Minus(Phrase(v)))

  def phrase(v: V) = Clause[V](self.name,Phrase(v))

  def in(v: Iterable[V]) = Clause[V](self.name, groupWithOr(v.map({x: V => Plus(Phrase(x))})))
  def nin(v: Iterable[V]) = Clause[V](self.name, groupWithAnd(v.map({x: V => Minus(Phrase(x))})))

  def query(q: Query[V]) = Clause[V](self.name, q)
}

//
class SolrStringField[T <: Record[T]](owner: T) extends StringField[T](owner, 0) with SolrField[String, T]
class SolrIntField[T <: Record[T]](owner: T) extends IntField[T](owner) with SolrField[Int, T]
class SolrDoubleField[T <: Record[T]](owner: T) extends DoubleField[T](owner) with SolrField[Double, T]
class SolrObjectIdField[T <: Record[T]](owner: T) extends DummyField[ObjectId, T](owner) with SolrField[ObjectId, T]

// This insanity makes me want to 86 Record all together. DummyField allows us
// to easily define our own Field types. I use this for ObjectId so that I don't
// have to import all of MongoRecord. We could trivially reimplement the other
// Field types using it.
class DummyField[V, T <: Record[T]](override val owner: T) extends Field[ObjectId, T] {
  override def setFromString(s: String) = Empty
  override def setFromAny(a: Any) = Empty
  override def setFromJValue(jv: net.liftweb.json.JsonAST.JValue) = Empty
  override def liftSetFilterToBox(a: Box[ObjectId]) = Empty
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
