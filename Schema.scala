package com.foursquare.solr
import com.foursquare.solr.Ast._
import net.liftweb.record.{Record, OwnedField, Field, MetaRecord}
import net.liftweb.record.field.{StringField, IntField, DoubleField}
import net.liftweb.common.{Box, Empty}
import scalaj.http._

import net.liftweb.json.JsonParser
import org.bson.types.ObjectId
import org.codehaus.jackson._
import org.codehaus.jackson.annotate._
import org.codehaus.jackson.map._


class Doc(val id: String, val user_type: Option[String]=None, val name: Option[String]=None,
               val lat: Option[Double]=None, val lng: Option[Double]=None,
               val address: Option[String]=None, val score: Option[Double]=None, val decayedPopularity1 : Option[Double]=None,
               val category_id_0: Option[String]=None, val mayorid : Option[Long]=None, val userid:Option[Long]=None,
               val popularity : Option[Int]=None, val dtzone: Option[String]=None, val partionedPopularity : Option[List[Int]]=None,
               val geomobile:Option[Boolean]=None, val checkin_info:Option[String]=None, val hasSpecial: Option[Boolean]=None,
               val crossstreet: Option[String]=None, val city: Option[String]=None, val state: Option[String]=None,
               val zip: Option[String]=None, val country: Option[String]=None, val checkinCount: Option[Int] = None) {
}

case class BasicDoc @JsonCreator()(@JsonProperty("id") id: String, @JsonProperty("score") score: Double)

case class ResponseHeader @JsonCreator()(@JsonProperty("status")status: Int, @JsonProperty("Qtime")QTime: Int)

case class Response(numFound: Int, start: Int, docs: List[Doc])

case class BasicResponse @JsonCreator()(@JsonProperty("numFound")numFound: Int, @JsonProperty("start")start: Int, @JsonProperty("docs")docs: List[BasicDoc])

case class SearchResults(responseHeader: ResponseHeader, response: Response)

case class BasicSearchResults @JsonCreator()(@JsonProperty("responseHeader")responseHeader: ResponseHeader, @JsonProperty("response")response: BasicResponse)

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
    if (fieldstofetch == Nil ||
        fieldstofetch == List("id") ||
        fieldstofetch == List("id","score")) {
      val results = mapper.readValue(r,classOf[BasicSearchResults])
      val responseHeader = ResponseHeader(results.responseHeader.status,results.responseHeader.QTime)
      val response = results.response
      val newResponse = Response(response.numFound,response.start,response.docs.toList.map({x: BasicDoc => new Doc(x.id,score = Some(x.score))}))
      SearchResults(responseHeader,newResponse)
    } else {
      JsonParser.parse(r).extract[SearchResults]
    }
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
