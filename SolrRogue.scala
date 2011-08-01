package com.foursquare.solr
import Ast._
import net.liftweb.util.Props


object SVenue extends SVenue with SolrMeta[SVenue] {
  //The name is used to determine which props to use.
  def solrName = "venues"
  //The servers is a list used in round-robin for running solr read queries against.
  def servers = Props.get("sorl."+solrName+".servers").map(x => x.split(",").toList).openOr {
    //If the server list prop isn't present fall back to the old style of host/port
    val Host = Props.get("solr." + solrName + ".host").openOr(throw new RuntimeException("Props not found for %s Solr".format(solrName)))
    val Port = Props.getInt("solr." + solrName + ".port").openOr(throw new RuntimeException("Props not found for %s Solr".format(solrName)))
    List("%s:%d".format(Host, Port))
  }
}

class SVenue extends SolrSchema[SVenue] {
  def meta = SVenue

  //The default field will result in queries against the default field
  //or if a list of fields to query has been specified to an edismax query then
  //the query will be run against this.
  object default extends SolrDefaultStringField(this)
  //This is a special field to allow for querying of *:*
  object metall extends SolrStringField(this) {
    override def name="*"
  }
  object id extends SolrObjectIdField(this)
  object lat extends SolrDoubleField(this)
  object lng extends SolrDoubleField(this)
  object userid extends SolrLongField(this)
  object text extends SolrStringField(this)
  object name extends SolrStringField(this)
  object ngram_name extends SolrStringField(this)
  object keywords extends SolrStringField(this)
  object aliases extends SolrStringField(this)
  object tags extends SolrStringField(this)
  object category_string extends SolrStringField(this)
  object meta_categories extends SolrStringField(this)
  object address extends SolrStringField(this)
  object score extends SolrDoubleField(this)
  object mayorid extends SolrLongField(this)
  object category_id_0 extends SolrObjectIdField(this)
  object popularity extends SolrIntField(this)
  object dtzone extends SolrStringField(this)
  // Temporarily, added this field to test SolrDateTimeField. Not present in solr conf.
  object createdDate extends SolrDateTimeField(this)
  object partitionedPopularity extends SolrIntListField(this)
  object geomobile extends SolrBooleanField(this)
  object checkin_info extends SolrStringField(this)
  object hasSpecial extends SolrBooleanField(this)
  object crossstreet extends SolrStringField(this)
  object city extends SolrStringField(this)
  object state extends SolrStringField(this)
  object zip extends SolrStringField(this)
  object country extends SolrStringField(this)
  object checkinCount extends SolrIntField(this)
  object category_ids extends SolrStringField(this)
  object decayedPopularity1 extends SolrDoubleField(this)
}

object STip extends STip with SolrMeta[STip] {
  //The name is used to determine which props to use.
  def solrName = "tips"
  //The servers is a list used in round-robin for running solr read queries against.
  def servers = Props.get("sorl."+solrName+".servers").map(x => x.split(",").toList).openOr {
    //If the server list prop isn't present fall back to the old style of host/port
    val Host = Props.get("solr." + solrName + ".host").openOr(throw new RuntimeException("Props not found for %s Solr".format(solrName)))
    val Port = Props.getInt("solr." + solrName + ".port").openOr(throw new RuntimeException("Props not found for %s Solr".format(solrName)))
    List("%s:%d".format(Host, Port))
  }
}
class STip extends SolrSchema[STip] {
  def meta = STip

  object id extends SolrObjectIdField(this)
  object text extends SolrStringField(this)
  object userid extends SolrStringField(this)
}

object SUser extends SUser with SolrMeta[SUser] {
  def solrName = "user"
  def servers = Props.get("sorl."+solrName+".servers").map(x => x.split(",").toList).openOr {
    val Host = Props.get("solr." + solrName + ".host").openOr(throw new RuntimeException("Props not found for %s Solr".format(solrName)))
    val Port = Props.getInt("solr." + solrName + ".port").openOr(throw new RuntimeException("Props not found for %s Solr".format(solrName)))
    List("%s:%d".format(Host, Port))
  }
}
class SUser extends SolrSchema[SUser] {
  def meta = SUser

  //The default field will result in queries against the default field
  //or if a list of fields to query has been specified to an edismax query then
  //the query will be run against this.
  object default extends SolrDefaultStringField(this)
  object id extends SolrLongField(this)
  object fullname extends SolrStringField(this)
  object firstname extends SolrStringField(this)
  object lastname extends SolrStringField(this)
  object twitter_name extends SolrStringField(this)
  object email extends SolrStringField(this)
  object phone extends SolrStringField(this)
  object user_type extends SolrStringField(this)
  object brand_sidebar_content extends SolrStringField(this)
}


object Helpers {
  def groupWithOr[V](v: Iterable[Query[V]]): Query[V] = {
    Group(v.tail.foldLeft(v.head: Query[V])({(l, r) => Or(l, r)}))
  }

  def groupWithAnd[V](v: Iterable[Query[V]]): Query[V] = {
    Group(v.tail.foldLeft(v.head: Query[V])({(l, r) => And(l, r)}))
  }
}
