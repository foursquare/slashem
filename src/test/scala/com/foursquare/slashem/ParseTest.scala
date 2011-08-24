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
  @Test
  def csvExtractTest1 = {
    val r = """zip,state,userid,name,id
"",Selangor,2056317,Foursquare HQ,4c809f4251ada1cdc3790b10
"",Selangor,3837864,Foursquare HQ,4d102d0d6331a093714e5594
"",,5431332,Foursquares HQ,4debfe4952b11677f06432e3
2763,NSW,1088978,Foursquare International HQ,4cd6a63ab6962c0f6f392f96
37076,Tennessee,1610362,Allen's Foursquare HQ,4c369e5618e72d7f843e15f5
"",MN,143800,foursquare HQ - MN,4d1a2fc1daf82c0f9193a452
"",The Netherlands,25383,Foursquare EMEA HQ,4c19e50e23a29c748f111953
94107,CA,1048882,foursquare SF,4de0117c45dd3eae8764d6ac
02142,MA,33833,Foursquare Cops - Boston HQ,4bc4e3c174a9a593c282d6f6
"",,1312654,foursquare HQ germany,4c61757e3986e21ed29b964f
    """
  }
  @Test
  def csvExtractTest2 = {
    val r = """zip,state,userid,name,id
94103,California,1048882,Holden's Hobo Hut,4dc5bc4845dd2645527930a9
94110,California,8380272,holdens hobo hut #2,4e4c1556922e02e292b748c3
"",,240274,Hobo,4b6db1b9f964a5202a872ce3
08088,New Jersey,801205,Hobo,4bba55bbb35776b0e0ccca01
"",,2881459,Hoboe,4c7842a0a8683704c0b70c4d
60181,I'll,2831874,HoBO,4cd1a9666449a0932168d1cf
97209,Oregon,2945250,Hobos,4cf2117e88de3704f40a772b
"",,3954869,HOBO,4d0368ff9d33a14345f5b378
"",,2958284,Hobo,4d72645c5838a09324620adb
"",Huila,9844807,Hobo,4ddea900227106
    """
  }
}
