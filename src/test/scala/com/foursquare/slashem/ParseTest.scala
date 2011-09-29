package com.foursquare.slashem

import com.foursquare.slashem._

import net.liftweb.common.Full

import org.bson.types.ObjectId
import org.junit.Test
import org.junit._

import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary.arbitrary

import org.specs.SpecsMatchers
import org.specs.matcher.ScalaCheckMatchers
import java.util.HashMap


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
    val parsed = SVenueTest.extractFromResponse(r, Some(testCreator _), Nil, queryText="foo").get()
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
  @Test
  def testOidandScoreExtract = {
    val r = """{
  "responseHeader":{
    "status":0,
    "QTime":1,
    "params":{
      "fl":"id,score",
      "indent":"on",
      "start":"0",
      "q":"foursquare hq",
      "wt":"json",
      "version":"2.2",
      "rows":"2"}},
  "response":{"numFound":45776,"start":0,"maxScore":9.185221,"docs":[
      {
        "id":"4c809f4251ada1cdc3790b10",
        "score":9.185221},
      {
        "id":"4d102d0d6331a093714e5594",
        "score":9.185220}]
  }}"""
    val parsed = SVenueTest.extractFromResponse(r, Some(testCreator _), Nil, queryText="foo2").get()
    val oids = parsed.response.oids
    val oidAndScores = parsed.response.oidScorePair
    Assert.assertEquals(oids.apply(0),new ObjectId("4c809f4251ada1cdc3790b10"))
    Assert.assertEquals(oids, List(new ObjectId("4c809f4251ada1cdc3790b10"),
                                   new ObjectId("4d102d0d6331a093714e5594")))
    Assert.assertEquals(oidAndScores,
                        List((new ObjectId("4c809f4251ada1cdc3790b10") -> 9.185221),
                           (new ObjectId("4d102d0d6331a093714e5594") -> 9.185220)))
  }
  @Test
  def testScoreFiltering = {
    val r = """{
  "responseHeader":{
    "status":0,
    "QTime":1,
    "params":{
      "fl":"id,score",
      "indent":"on",
      "start":"0",
      "q":"foursquare hq",
      "wt":"json",
      "version":"2.2",
      "rows":"3"}},
  "response":{"numFound":45776,"start":0,"maxScore":9.185221,"docs":[
      {
        "id":"4c809f4251ada1cdc3790b10",
        "score":9000.185221},
      {
        "id":"4d102d0d6331a093714e5594",
        "score":9.185220},
      {
        "id":"4d102d0d6331a093714e5595",
        "score":1.185220}]
  }}"""
    val parsed = SVenueTest.extractFromResponse(r, Some(testCreator _), Nil, fallOf = Some(0.5), min=Some(1),"foo3").get()
    val oids = parsed.response.oids
    val oidAndScores = parsed.response.oidScorePair
    Assert.assertEquals(oids.apply(0),new ObjectId("4c809f4251ada1cdc3790b10"))
    Assert.assertEquals(oids, List(new ObjectId("4c809f4251ada1cdc3790b10")))
    Assert.assertEquals(oidAndScores,
                        List((new ObjectId("4c809f4251ada1cdc3790b10") -> 9000.185221)))
  }

  @Test
  def testCaseClassExtraction = {
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
        "name":"test2",
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
    case class TestPirate(state: Option[String])
    val parsedQuery = (SVenueTest where (_.name eqs "test") selectCase(_.name,((x: Option[String]) => TestPirate(x))))
    val parsed = SVenueTest.extractFromResponse(r, parsedQuery.creator, Nil, queryText="rawrs").get()
    val extracted = parsed.response.processedResults
    Assert.assertEquals(extracted.length,2)
    Assert.assertEquals(extracted,List(TestPirate(Some("test")),TestPirate(Some("test2"))))
  }
  def testCreator(a: HashMap[String,Any], b: Any) = {
    "lols"
  }
  @Test
  def testHighlighting() = {
    val r= """{
  "responseHeader":{
    "status":0,
    "QTime":1,
    "params":{
      "indent":"on",
      "start":"0",
      "q":"pancakes sucka",
      "wt":"json",
      "hl":"true",
      "version":"2.2",
      "rows":"2"}},
  "response":{"numFound":14066,"start":0,"docs":[
      {
        "id":"4bbee3c170c603bba83a97b4",
        "text":"SUCKA FREE.",
        "lat":-33.892586,
        "lng":151.203223,
        "venueid":"4b594c4bf964a520578428e3"},
      {
        "id":"4bd3921670c603bb931a99b4",
        "text":"Sucka free!",
        "lat":39.76184,
        "lng":-105.011328,
        "venueid":"4bbfd22a4cdfc9b65c049221"}]
  },
  "highlighting":{
    "4bbee3c170c603bba83a97b4":{
      "text":["<em>SUCKA</em> FREE."]},
    "4bd3921670c603bb931a99b4":{
      "text":["<em>Sucka</em> free!"]}}}"""
    val parsed = STipTest.extractFromResponse(r, Some(testCreator _), Nil,queryText="cheetos").get()
    Assert.assertEquals(parsed.responseHeader, ResponseHeader(0, 1))
    Assert.assertEquals(parsed.response.results.apply(0).text.value, "SUCKA FREE.")
    Assert.assertEquals(parsed.response.results.apply(0).text.highlighted.length, 1)
    Assert.assertEquals(parsed.response.results.apply(0).text.highlighted.apply(0), "<em>SUCKA</em> FREE.")
  }
  @Test
  def testHighlightingCaseClass() = {
    val r= """{
  "responseHeader":{
    "status":0,
    "QTime":1,
    "params":{
      "indent":"on",
      "start":"0",
      "q":"pancakes sucka",
      "wt":"json",
      "hl":"true",
      "version":"2.2",
      "rows":"2"}},
  "response":{"numFound":14066,"start":0,"docs":[
      {
        "id":"4bbee3c170c603bba83a97b4",
        "text":"SUCKA FREE.",
        "lat":-33.892586,
        "lng":151.203223,
        "venueid":"4b594c4bf964a520578428e3"},
      {
        "id":"4bd3921670c603bb931a99b4",
        "text":"Sucka free!",
        "lat":39.76184,
        "lng":-105.011328,
        "venueid":"4bbfd22a4cdfc9b65c049221"}]
  },
  "highlighting":{
    "4bbee3c170c603bba83a97b4":{
      "text":["<em>SUCKA</em> FREE."]},
    "4bd3921670c603bb931a99b4":{
      "text":["<em>Sucka</em> free!"]}}}"""
    case class TestPirate(state: Option[String], lolerskates: List[String])
    val parsedQuery = (STipTest where (_.text eqs "test") highlighting() selectCase(_.text,((x: Option[String], y: List[String]) => TestPirate(x,y))))
    val parsed = STipTest.extractFromResponse(r, parsedQuery.creator, Nil,queryText="are delicious!").get()
    val extracted = parsed.response.processedResults
    Assert.assertEquals(parsed.responseHeader, ResponseHeader(0, 1))
    Assert.assertEquals(parsed.response.results.apply(0).text.value, "SUCKA FREE.")
    Assert.assertEquals(parsed.response.results.apply(0).text.highlighted.length, 1)
    Assert.assertEquals(parsed.response.results.apply(0).text.highlighted.apply(0), "<em>SUCKA</em> FREE.")
    Assert.assertEquals(extracted,List(TestPirate(Some("SUCKA FREE."),List("<em>SUCKA</em> FREE.")),TestPirate(Some("Sucka free!"),List("<em>Sucka</em> free!"))))
  }
  @Test
  def test504() = {
    val r= """<>504<>"""
    case class TestPirate(state: Option[String], lolerskates: List[String])
    val parsedQuery = (STipTest where (_.text eqs "test") highlighting() selectCase(_.text,((x: Option[String], y: List[String]) => TestPirate(x,y))))
    val parsedFuture = STipTest.extractFromResponse(r, parsedQuery.creator, Nil,queryText="are delicious!")
    val exception = try {
      parsedFuture.get()
      false
    } catch {
      case e => true
    }
    Assert.assertEquals(exception,true)
  }

}

