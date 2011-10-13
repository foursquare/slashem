package com.foursquare.slashem

object ESimplePanda extends ESimplePanda with ElasticMeta[ESimplePanda] {
  //Force local for testing
  override val local = true
  override val clientOnly = false
  override val clusterName = "simpletest" //Override me knthx

}
class ESimplePanda extends ElasticSchema[ESimplePanda] {
  def meta = ESimplePanda
  object default extends SlashemDefaultStringField(this)
  object id extends SlashemObjectIdField(this)
  object name extends SlashemStringField(this)
  object score extends SlashemDoubleField(this)
}

object ESimpleGeoPanda extends ESimpleGeoPanda with ElasticMeta[ESimpleGeoPanda] {
  //Force local for testing
  override val local = true
  override val clusterName = "simpletest" //Override me knthx

}
class ESimpleGeoPanda extends ElasticSchema[ESimpleGeoPanda] {
  def meta = ESimpleGeoPanda
  object default extends SlashemDefaultStringField(this)
  object id extends SlashemObjectIdField(this)
  object name extends SlashemStringField(this)
  object score extends SlashemDoubleField(this)
}


