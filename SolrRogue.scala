package com.foursquare.solr
import Ast._

object SVenue extends SVenue with SolrMeta[SVenue] {
  def host = "repo.foursquare.com"
  def port = "8002"
}
class SVenue extends SolrSchema[SVenue] {
  def meta = SVenue

  object default extends SolrDefaultStringField(this)
  object id extends SolrObjectIdField(this)
  object text extends SolrStringField(this)
  object name extends SolrStringField(this)
  object keywords extends SolrStringField(this)
  object tags extends SolrStringField(this)
  object category_string extends SolrStringField(this)
  object address_string extends SolrStringField(this)
  object category_ids extends SolrStringField(this)
  object decayedPopularity1 extends SolrDoubleField(this)
}

object STip extends STip with SolrMeta[STip] {
  def host = "repo.foursquare.com"
  def port = "8000"
}
class STip extends SolrSchema[STip] {
  def meta = STip

  object id extends SolrObjectIdField(this)
  object text extends SolrStringField(this)
  object userid extends SolrStringField(this)
}

object SUser extends SUser with SolrMeta[SUser] {
  def host = "repo.foursquare.com"
  def port = "8001"
}
class SUser extends SolrSchema[SUser] {
  def meta = SUser

  object id extends SolrObjectIdField(this)
  object fullname extends SolrStringField(this)
  object firstname extends SolrStringField(this)
  object lastname extends SolrStringField(this)
  object email extends SolrStringField(this)
  object phone extends SolrStringField(this)
  object user_type extends SolrStringField(this)
}


object Helpers {
  def groupWithOr[V](v: Iterable[Query[V]]): Query[V] = {
    Group(v.tail.foldLeft(v.head: Query[V])({(l, r) => Or(l, r)}))
  }

  def groupWithAnd[V](v: Iterable[Query[V]]): Query[V] = {
    Group(v.tail.foldLeft(v.head: Query[V])({(l, r) => And(l, r)}))
  }
}
