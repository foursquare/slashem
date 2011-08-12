package com.foursquare.solr

import com.foursquare.solr._

import net.liftweb.common.Full

import org.bson.types.ObjectId
import org.junit.Test
import org.junit._

import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary.arbitrary

import org.specs.SpecsMatchers
import org.specs.matcher.ScalaCheckMatchers


class ParseTest extends SpecsMatchers with ScalaCheckMatchers {
  //This is the test for the extraction code.
  @Test
  def testParseVenueFields {
    //Here is some json returned from solr.
    val r = """{
  "responseHeader":{
    "status":0,
    "QTime":1,
    "params":{
      "indent":"on",
      "start":"0",
      "q":"test",
      "wt":"json",
      "version":"2.2",
      "rows":"1"}},
  "response":{"numFound":40831,"start":0,"docs":[
      {
        "id":"4c809f4251ada1cdc3790b10",
        "name":"test",
        "userid":2056317,
        "mayorid":0,
        "checkin_info":"{\"checkins\":[]}",
        "dtzone":"Asia/Kuala_Lumpur",
        "geomobile":false,
        "address":"",
        "crossstreet":"",
        "city":"Shah Alam",
        "state":"Selangor",
        "zip":"",
        "country":"",
        "category_id_0":"",
        "popularity":15,
        "decayedPopularity1":0.00306415687810945,
        "partitionedPopularity":"0 0 1 0 0 1 2 2 0 1 0 0 0 0 0 0 0 0 0 1 0 1 0 0 0 0 0 0 0 1 0 0 0 1 1 0 0 0 0 0 0 0 0 0 1 0 0 0",
        "lat":3.014217,
        "lng":101.495239,
        "hasSpecial":false},
    {
        "id":"4c809f4251ada1cdc3790b10",
        "name":"test",
        "userid":2056317,
        "mayorid":0,
        "checkin_info":"{\"checkins\":[]}",
        "dtzone":"Asia/Kuala_Lumpur",
        "geomobile":false,
        "address":"",
        "crossstreet":"",
        "city":"Shah Alam",
        "state":"Selangor",
        "zip":"",
        "country":"",
        "category_id_0":"",
        "popularity":15,
        "decayedPopularity1":0.00306415687810945,
        "partitionedPopularity":"0 0 1 0 0 1 2 2 0 1 0 0 0 0 0 0 0 0 0 1 0 1 0 0 0 0 0 0 0 1 0 0 0 1 1 0 0 0 0 0 0 0 0 0 1 0 0 0",
        "lat":3.014217,
        "lng":101.495239,
        "hasSpecial":false}]
  }}"""
    val parsed = SVenueTest.extractFromResponse(r, Nil)
    Assert.assertEquals(parsed.responseHeader, ResponseHeader(0, 1))
    Assert.assertEquals(parsed.response.results(SVenueTest).apply(0).name.value, "test")
    Assert.assertEquals(parsed.response.results(SVenueTest).apply(0).name.valueBox, Full("test"))
    Assert.assertEquals(parsed.response.results(SVenueTest).apply(0).decayedPopularity1.value, 0.00306415687810945, 0.000000001)
    Assert.assertEquals(parsed.response.results(SVenueTest).apply(0).id.is, new ObjectId("4c809f4251ada1cdc3790b10"))
    Assert.assertEquals(parsed.response.results(SVenueTest).apply(0).id.valueBox, Full(new ObjectId("4c809f4251ada1cdc3790b10")))
    Assert.assertEquals(parsed.response.results(SVenueTest).apply(0).partitionedPopularity.value,
                        List(0, 0, 1, 0, 0, 1, 2, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0))
    Assert.assertEquals(parsed.response.results(SVenueTest).apply(0).partitionedPopularity.valueBox,
                        Full(List(0, 0, 1, 0, 0, 1, 2, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0)))

    Assert.assertEquals(parsed.response.results.apply(0).name.value, "test")
    Assert.assertEquals(parsed.response.results.apply(0).name.valueBox, Full("test"))
    Assert.assertEquals(parsed.response.results.apply(0).decayedPopularity1.value, 0.00306415687810945, 0.000000001)
    Assert.assertEquals(parsed.response.results.apply(0).id.is, new ObjectId("4c809f4251ada1cdc3790b10"))
    Assert.assertEquals(parsed.response.results.apply(0).id.valueBox, Full(new ObjectId("4c809f4251ada1cdc3790b10")))
    Assert.assertEquals(parsed.response.results.apply(0).partitionedPopularity.value,
                        List(0, 0, 1, 0, 0, 1, 2, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0))
    Assert.assertEquals(parsed.response.results.apply(0).partitionedPopularity.valueBox,
                        Full(List(0, 0, 1, 0, 0, 1, 2, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0)))
  }
}
