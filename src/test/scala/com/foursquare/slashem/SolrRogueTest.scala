package com.foursquare.slashem
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
  object default extends SlashemDefaultStringField(this)
  //This is a special field to allow for querying of *:*
  object metall extends SlashemStringField(this) {
    override def name="*"
  }
  object id extends SlashemObjectIdField(this)
  object lat extends SlashemDoubleField(this)
  object lng extends SlashemDoubleField(this)
  object userid extends SlashemLongField(this)
  object text extends SlashemStringField(this)
  object name extends SlashemStringField(this)
  object ngram_name extends SlashemStringField(this)
  object keywords extends SlashemStringField(this)
  object aliases extends SlashemStringField(this)
  object tags extends SlashemStringField(this)
  object category_string extends SlashemStringField(this)
  object meta_categories extends SlashemStringField(this)
  object address extends SlashemStringField(this)
  object score extends SlashemDoubleField(this)
  object mayorid extends SlashemLongField(this)
  object category_id_0 extends SlashemObjectIdField(this)
  object popularity extends SlashemIntField(this)
  object dtzone extends SlashemStringField(this)
  object partitionedPopularity extends SlashemIntListField(this)
  object geomobile extends SlashemBooleanField(this)
  object checkin_info extends SlashemStringField(this)
  object hasSpecial extends SlashemBooleanField(this)
  object crossstreet extends SlashemStringField(this)
  object city extends SlashemStringField(this)
  object state extends SlashemStringField(this)
  object zip extends SlashemStringField(this)
  object country extends SlashemStringField(this)
  object checkinCount extends SlashemIntField(this)
  object category_ids extends SlashemStringField(this)
  object decayedPopularity1 extends SlashemDoubleField(this)
  object geo_s2_cell_ids extends SlashemGeoField(this)
}

object STipTest extends STipTest with SolrMeta[STipTest] {
  //The name is used to determine which props to use.
  def solrName = "tips"
  //The servers is a list used in round-robin for running solr read queries against.
  def servers = List("localhost:8001")
}
class STipTest extends SolrSchema[STipTest] {
  def meta = STipTest

  object id extends SlashemObjectIdField(this)
  object text extends SlashemStringField(this)
  object userid extends SlashemStringField(this)
  object geo_s2_cell_ids extends SlashemGeoField(this)

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
  object default extends SlashemDefaultStringField(this)
  object id extends SlashemLongField(this)
  object fullname extends SlashemStringField(this)
  object firstname extends SlashemStringField(this)
  object lastname extends SlashemStringField(this)
  object twitter_name extends SlashemStringField(this)
  object email extends SlashemStringField(this)
  object phone extends SlashemStringField(this)
  object user_type extends SlashemStringField(this)
  object brand_sidebar_content extends SlashemStringField(this)
  object friend_ids extends SlashemStringField(this)
  object geo_s2_cell_ids extends SlashemGeoField(this)
  object follower_count extends SlashemLongField(this)

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
  object default extends SlashemDefaultStringField(this)
  //This is a special field to allow for querying of *:*
  object metall extends SlashemStringField(this) {
    override def name="*"
  }
  object id extends SlashemObjectIdField(this)
  object venueid extends SlashemObjectIdField(this)
  object lat extends SlashemDoubleField(this)
  object lng extends SlashemDoubleField(this)
  object name extends SlashemStringField(this)
  object tags extends SlashemStringField(this)
  object start_time extends SlashemDateTimeField(this)
  object expires_time extends SlashemDateTimeField(this)
  object geo_s2_cell_ids extends SlashemGeoField(this)

}

