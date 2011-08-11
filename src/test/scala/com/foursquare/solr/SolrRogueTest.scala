package com.foursquare.solr
import Ast._
import net.liftweb.util.Props

object SVenueTest extends SVenueTest with SolrMeta[SVenueTest] {
  //The name is used to determine which props to use.
  def solrName = "venues"
  //The servers is a list used in round-robin for running solr read queries against.
  def servers = List("localhost:8002")
}

class SVenueTest extends SolrSchema[SVenueTest] {
  def meta = SVenueTest

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
  object geo_s2_cell_ids extends SolrGeoField(this)
}

object STipTest extends STipTest with SolrMeta[STipTest] {
  //The name is used to determine which props to use.
  def solrName = "tips"
  //The servers is a list used in round-robin for running solr read queries against.
  def servers = List("localhost:8001")
}
class STipTest extends SolrSchema[STipTest] {
  def meta = STipTest

  object id extends SolrObjectIdField(this)
  object text extends SolrStringField(this)
  object userid extends SolrStringField(this)
  object geo_s2_cell_ids extends SolrGeoField(this)

}

object SUserTest extends SUserTest with SolrMeta[SUserTest] {
  def solrName = "user"
  def servers = List("localhost:8003")
}
class SUserTest extends SolrSchema[SUserTest] {
  def meta = SUserTest

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
  object friend_ids extends SolrStringField(this)
  object geo_s2_cell_ids extends SolrGeoField(this)
  object follower_count extends SolrLongField(this)

}

object SEventTest extends SEventTest with SolrMeta[SEventTest] {
  //The name is used to determine which props to use.
  def solrName = "events"
  //The servers is a list used in round-robin for running solr read queries against.
  def servers = List("localhost:8004")
}

class SEventTest extends SolrSchema[SEventTest] {
  def meta = SEventTest

  //The default field will result in queries against the default field
  //or if a list of fields to query has been specified to an edismax query then
  //the query will be run against this.
  object default extends SolrDefaultStringField(this)
  //This is a special field to allow for querying of *:*
  object metall extends SolrStringField(this) {
    override def name="*"
  }
  object id extends SolrObjectIdField(this)
  object venueid extends SolrObjectIdField(this)
  object lat extends SolrDoubleField(this)
  object lng extends SolrDoubleField(this)
  object name extends SolrStringField(this)
  object tags extends SolrStringField(this)
  object start_time extends SolrDateTimeField(this)
  object expires_time extends SolrDateTimeField(this)
  object geo_s2_cell_ids extends SolrGeoField(this)

}

