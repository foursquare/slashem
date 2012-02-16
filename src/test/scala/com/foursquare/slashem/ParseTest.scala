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
        "commentList":"hi there how are you",
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
        "commentList":["hi", "there", "how", "are", "you"],
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
    Assert.assertEquals(parsed.response.results(SVenueTest).apply(0).commentList.value,
                        List("hi", "there", "how", "are", "you"))
    Assert.assertEquals(parsed.response.results(SVenueTest).apply(0).commentList.valueBox,
                        Full(List("hi", "there", "how", "are", "you")))
    Assert.assertEquals(parsed.response.results.apply(0).name.value, "test")
    Assert.assertEquals(parsed.response.results.apply(0).name.valueBox, Full("test"))
    Assert.assertEquals(parsed.response.results.apply(0).decayedPopularity1.value, 0.00306415687810945, 0.000000001)
    Assert.assertEquals(parsed.response.results.apply(0).id.is, new ObjectId("4c809f4251ada1cdc3790b10"))
    Assert.assertEquals(parsed.response.results.apply(0).id.valueBox, Full(new ObjectId("4c809f4251ada1cdc3790b10")))
    Assert.assertEquals(parsed.response.results.apply(0).partitionedPopularity.value,
                        List(0, 0, 1, 0, 0, 1, 2, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0))
    Assert.assertEquals(parsed.response.results.apply(0).partitionedPopularity.valueBox,
                        Full(List(0, 0, 1, 0, 0, 1, 2, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0)))
    Assert.assertEquals(parsed.response.results.apply(0).commentList.value,
                        List("hi", "there", "how", "are", "you"))
    Assert.assertEquals(parsed.response.results.apply(0).commentList.valueBox,
                        Full(List("hi", "there", "how", "are", "you")))

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
  def testFaceting = {
    val r = """
{"responseHeader":{"status":0,"QTime":42,"params":{"mm":"100%","facet":"true","facet.mincount":"1","qf":"text","wt":"json","hl":"on","defType":"edismax","rows":"5","pf":"text^100000.0","fl":"id,score,text","start":"0","q":"(fried chicken)","facet.field":"venueid","fq":"geo_s2_cell_ids:(\"89C250AB00000000\" OR \"89C2575400000000\" OR \"89C2590700000000\" OR \"89C2590C00000000\" OR \"89C2591400000000\" OR \"89C2597000000000\" OR \"89C2599000000000\" OR \"89C259B000000000\" OR \"89C259C400000000\" OR \"89C259DD00000000\" OR \"89C259F000000000\" OR \"89C25A1000000000\" OR \"89C25A3000000000\" OR \"89C25A4100000000\" OR \"89C25A4740000000\" OR \"89C25A6AC0000000\" OR \"89C25BCC00000000\" OR \"89C25BD400000000\" OR \"89C25BD900000000\" OR \"89C25BDA40000000\")"}},"response":{"numFound":344,"start":0,"maxScore":7.544886,"docs":[{"id":"4af9fceb70c603bbaed88eb4","text":"fried chicken!!!","score":7.544886},{"id":"4f0a342ce4b09d21f86625d1","text":"Best fried chicken wings!","score":6.0359087},{"id":"4ee610974690f8894201a0e2","text":"The fried chicken is to die for.","score":6.0359087},{"id":"4dc4a875d164eb9c9fdbf07c","text":"Fried chicken is a must!","score":6.0359087},{"id":"4d7f90f46174a35d172d9d03","text":"Fried chicken in cafeteria = not bad.","score":6.0359087}]},"facet_counts":{"facet_queries":{},"facet_fields":{"venueid":["4c8ac04852a98cfa84992fe9",23,"49008118f964a5205a521fe3",21,"3fd66200f964a5205ae91ee3",16,"4d9db7087f9e4eb9436ea1fc",12,"3fd66200f964a52077e91ee3",11,"4aeb5fe4f964a52080c121e3",9,"4731be8af964a520244c1fe3",8,"4c78103edf08a1cd411fd65d",8,"43334580f964a52013281fe3",6,"49ea44caf964a52043661fe3",6,"4e32d529483bf02b1f8dc50f",6,"4b31574af964a520130525e3",5,"4b912a24f964a52089a733e3",5,"3fd66200f964a52064e61ee3",4,"3fd66200f964a52069e71ee3",4,"40a55d80f964a52020f31ee3",4,"411ff900f964a520240c1fe3",4,"425f0400f964a52011211fe3",4,"44522a64f964a520a8321fe3",4,"49ebc888f964a5202d671fe3",4,"4b1896caf964a52069d423e3",4,"4b59e21df964a520209e28e3",4,"4bb3ec359af3b7132dc48b28",4,"3fd66200f964a5201ae51ee3",3,"3fd66200f964a52037e31ee3",3,"3fd66200f964a5207feb1ee3",3,"4079dc00f964a52070f21ee3",3,"46e6d800f964a520c84a1fe3",3,"4a78c865f964a52068e61fe3",3,"4bf18a68324cc9b60f27cc92",3,"4f00dea9f9abd5b3917d422c",3,"3fd66200f964a5207eea1ee3",2,"3fd66200f964a520b1ea1ee3",2,"3fd66200f964a520e9e61ee3",2,"40be6a00f964a520c9001fe3",2,"40f1d480f964a5205b0a1fe3",2,"49baa2cef964a5208d531fe3",2,"4a149749f964a52055781fe3",2,"4a2ae34ff964a52065961fe3",2,"4a43af8ef964a520ada61fe3",2,"4a6a57bef964a52029cd1fe3",2,"4ad39a8df964a52022e520e3",2,"4ae91871f964a52059b421e3",2,"4b2bc45ff964a52087ba24e3",2,"4b43dd16f964a5207cec25e3",2,"4b58a360f964a520f76228e3",2,"4b6607d6f964a520820f2be3",2,"4b798b33f964a52050002fe3",2,"4c5214d89d642d7fdb649dde",2,"4c9a999d292a6dcb7871d076",2,"4c9b84ee78ffa0939fd47575",2,"4e1df590b61c7cb34d9735a8",2,"3fd66200f964a5203fe71ee3",1,"3fd66200f964a52044e61ee3",1,"3fd66200f964a52067e61ee3",1,"3fd66200f964a5206cea1ee3",1,"3fd66200f964a52077e51ee3",1,"3fd66200f964a5207ee51ee3",1,"3fd66200f964a52083e61ee3",1,"3fd66200f964a52084ea1ee3",1,"3fd66200f964a520a9f11ee3",1,"3fd66200f964a520c9e91ee3",1,"3fd66200f964a520dbe31ee3",1,"3fd66200f964a520eae81ee3",1,"3fd66200f964a520ede41ee3",1,"40cf8d80f964a52033011fe3",1,"42055e00f964a5206e1f1fe3",1,"43850280f964a520012b1fe3",1,"438b3682f964a520242b1fe3",1,"440e9d36f964a5209f301fe3",1,"461352e3f964a52038451fe3",1,"46cbd4edf964a520344a1fe3",1,"4701165bf964a520384b1fe3",1,"47111950f964a5209e4b1fe3",1,"477a3514f964a520214d1fe3",1,"47a1bddbf964a5207a4d1fe3",1,"4846c5acf964a5206f501fe3",1,"4858e6b4f964a520c0501fe3",1,"48636f02f964a520e3501fe3",1,"49c77fd0f964a520a9571fe3",1,"49cc2743f964a5204e591fe3",1,"49cd891cf964a520fc591fe3",1,"49d0cb5ff964a520315b1fe3",1,"49d2b2e3f964a520c95b1fe3",1,"49d96994f964a520345e1fe3",1,"49e0a7faf964a5205b611fe3",1,"49e15e76f964a520c1611fe3",1,"49f50c47f964a520896b1fe3",1,"49f60307f964a520ef6b1fe3",1,"49fcbb01f964a520d56e1fe3",1,"49fef55df964a520ba6f1fe3",1,"4a01e6e7f964a52009711fe3",1,"4a076650f964a52059731fe3",1,"4a08ef4ef964a52026741fe3",1,"4a0d7695f964a5207c751fe3",1,"4a0f01acf964a52011761fe3",1,"4a26a0c5f964a520b97e1fe3",1,"4a5d0bf5f964a52017bd1fe3",1,"4a6b9f7bf964a52069cf1fe3",1,"4a6f32aff964a52094d51fe3",1]},"facet_dates":{},"facet_ranges":{}},"highlighting":{"4af9fceb70c603bbaed88eb4":{"text":["<em>fried</em> <em>chicken</em>!!!"]},"4f0a342ce4b09d21f86625d1":{"text":["Best <em>fried</em> <em>chicken</em> wings!"]},"4ee610974690f8894201a0e2":{"text":["The <em>fried</em> <em>chicken</em> is to die for."]},"4dc4a875d164eb9c9fdbf07c":{"text":["<em>Fried</em> <em>chicken</em> is a must!"]},"4d7f90f46174a35d172d9d03":{"text":["<em>Fried</em> <em>chicken</em> in cafeteria = not bad."]}}}
    """
    val parsed = STipTest.extractFromResponse(r, Some(testCreator _), Nil, queryText="foo2").get()
    Assert.assertEquals(23,parsed.response.fieldFacets.get("venueid").get("4c8ac04852a98cfa84992fe9"))
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
        "commentList":"hi there how are you",
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
        "commentList":["hi", "there", "how", "are", "you"],
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
  def testCreator(a: Pair[Map[String,Any],Option[Map[String,Any]]]) = {
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

