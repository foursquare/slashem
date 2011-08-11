package com.foursquare.solr

import com.foursquare.solr._

import org.junit.Test
import org.junit._

import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary.arbitrary

import org.specs.SpecsMatchers
import org.specs.matcher.ScalaCheckMatchers


class QueryTest extends SpecsMatchers with ScalaCheckMatchers {
  @Test
  def testProduceCorrectSimpleQueryString {
    val q = SUserTest where (_.fullname eqs "jon")
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("q" -> "fullname:(+\"jon\")",
                                                      "start" -> "0",
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }
  @Test
  def testProduceCorrectEdisMaxQueryString {
    val q = SUserTest where (_.fullname eqs "holden") useQueryType("edismax")
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("defType" -> "edismax",
                                                      "q" -> "fullname:(+\"holden\")",
                                                      "start" -> "0",
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }
  @Test
  def testProduceCorrectEdisMaxQueryStringWithMinimumMatch {
    val q = SUserTest where (_.fullname eqs "jason") useQueryType("edismax") minimumMatchPercent(75)
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("defType" -> "edismax",
                                                      "mm" -> "75%",
                                                      "q" -> "fullname:(+\"jason\")",
                                                      "start" -> "0",
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }

  @Test
  def testProduceCorrectDefaultQuery {
    val q = SVenueTest where (_.default eqs "bedlam coffee") useQueryType("edismax") minimumMatchPercent(75)
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("defType" -> "edismax",
                                                      "mm" -> "75%",
                                                      "q" -> "(+\"bedlam coffee\")",
                                                      "start" -> "0",
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }
  @Test
  def testProduceCorrectFilterQuery {
    val q = SVenueTest where (_.default eqs "club") filter (_.tags neqs "douchebag") useQueryType("edismax") minimumMatchPercent(75)
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("defType" -> "edismax",
                                                      "mm" -> "75%",
                                                      "q" -> "(+\"club\")",
                                                      "fq" -> "tags:(-\"douchebag\")",
                                                      "start" -> "0",
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }
  @Test
  def testProduceCorrectSimpleLimit {
    val q = SUserTest where (_.fullname eqs "jon") limit 250
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("q" -> "fullname:(+\"jon\")",
                                                      "start" -> "0",
                                                      "rows" -> "250").sortWith(_._1 > _._1))
  }
  @Test
  def testProduceCorrectWithQueryField {
    val q = SVenueTest where (_.default eqs "club") useQueryType("edismax") queryField(_.name)
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("defType" -> "edismax",
                                                      "q" -> "(+\"club\")",
                                                      "qf" -> "name",
                                                      "start" -> "0",
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }
  @Test
  def testProduceCorrectWithQueryFieldAndZeroBoost {
    val q = SVenueTest where (_.default eqs "club") useQueryType("edismax") queryField(_.name,0)
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("defType" -> "edismax",
                                                      "q" -> "(+\"club\")",
                                                      "start" -> "0",
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }
  @Test
  def testProduceCorrectWithQueryFieldAndOneBoost {
    val q = SVenueTest where (_.default eqs "club") useQueryType("edismax") queryField(_.name,1)
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("defType" -> "edismax",
                                                      "q" -> "(+\"club\")",
                                                      "qf" -> "name",
                                                      "start" -> "0",
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }
  @Test
  def testProduceCorrectWithQueryFieldAndBoost {
    val q = SVenueTest where (_.default eqs "club") useQueryType("edismax") queryField(_.name,2)
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("defType" -> "edismax",
                                                      "q" -> "(+\"club\")",
                                                      "qf" -> "name^2.0",
                                                      "start" -> "0",
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }
  @Test
  def testProduceCorrectWithQueryFieldAndNonIntBoost {
    val q = SVenueTest where (_.default eqs "club") useQueryType("edismax") queryField(_.name,2.5)
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("defType" -> "edismax",
                                                      "q" -> "(+\"club\")",
                                                      "qf" -> "name^2.5",
                                                      "start" -> "0",
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }
  @Test
  def testProduceCorrectWithPhraseBoost {
    val q = SVenueTest where (_.default eqs "club") useQueryType("edismax") phraseBoost(_.name,2.5)
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("defType" -> "edismax",
                                                      "q" -> "(+\"club\")",
                                                      "pf" -> "name^2.5",
                                                      "pf2" -> "name^2.5",
                                                      "pf3" -> "name^2.5",
                                                      "start" -> "0",
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }
  @Test
  def testProduceCorrectWithRange {
    val q = SVenueTest where (_.default inRange("a","z")) useQueryType("edismax")
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("defType" -> "edismax",
                                                      "q" -> "(+[a to z])",
                                                      "start" -> "0",
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }
  @Test
  def testProduceCorrectWithDateRange {
    import org.joda.time.{DateTime, DateTimeZone}
    val d1 = new DateTime(2011, 5, 1, 0, 0, 0, 0, DateTimeZone.UTC)
    val d2 = new DateTime(2011, 5, 2, 0, 0, 0, 0, DateTimeZone.UTC)
    val q = SEventTest where (_.start_time inRange(d1, d2))
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("q" -> "start_time:(+[2011\\-05\\-01T00\\:00\\:00.000Z to 2011\\-05\\-02T00\\:00\\:00.000Z])",
                                                      "start" -> "0",
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }
  @Test
  def testFieldQuery {
    val q = SVenueTest where (_.default eqs "bedlam coffee") useQueryType("edismax") fetchField (_.name) fetchField(_.address)
    val qp = q.queryParams().toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),List("defType" -> "edismax",
                                                      "q" -> "(+\"bedlam coffee\")",
                                                      "start" -> "0",
                                                      "fl" -> "address,name",//Britle
                                                      "rows" -> "10").sortWith(_._1 > _._1))
  }
  @Test
  def testAutoComplexQuery1 {
    val lols="holden's hobohut"
    val q = SVenueTest where (_.default contains lols) useQueryType("edismax") phraseBoost(_.text,1) phraseBoost(_.name,200) phraseBoost(_.aliases,25) queryField(_.text) queryField(_.ngram_name,0.2)
    val qp = q.queryParams().toList
    val expected = List("defType" -> "edismax",
                        "q" -> "(holden's hobohut)",
                        "start" -> "0",
                        "pf" -> "text",
                        "pf2" -> "text",
                        "pf3" -> "text",
                        "pf" -> "name^200.0",
                        "pf2" -> "name^200.0",
                        "pf3" -> "name^200.0",
                        "pf" -> "aliases^25.0",
                        "pf2" -> "aliases^25.0",
                        "pf3" -> "aliases^25.0",
                        "qf" -> "text",
                        "qf" -> "ngram_name^0.2",
                        "rows" -> "10")
    Assert.assertEquals(qp.sortWith(((x,y) => (x._1+x._2) > (y._1+y._2))),expected.sortWith(((x,y) => (x._1+x._2) > (y._1+y._2))))
  }
  @Test
  def testAutoComplexQuery2 {
    val lols="holden's hobohut"
    val q = SVenueTest where (_.default contains lols) useQueryType("edismax") phraseBoost(_.text,1) phraseBoost(_.name,200) phraseBoost(_.aliases,25) queryField(_.text) queryField(_.ngram_name,0.2) queryField(_.tags,0.01)
    val qp = q.queryParams().toList
    val expected = List("defType" -> "edismax",
                        "q" -> "(holden's hobohut)",
                        "start" -> "0",
                        "pf" -> "text",
                        "pf2" -> "text",
                        "pf3" -> "text",
                        "pf" -> "name^200.0",
                        "pf2" -> "name^200.0",
                        "pf3" -> "name^200.0",
                        "pf" -> "aliases^25.0",
                        "pf2" -> "aliases^25.0",
                        "pf3" -> "aliases^25.0",
                        "qf" -> "text",
                        "qf" -> "ngram_name^0.2",
                        "qf" -> "tags^0.01",
                        "rows" -> "10")
    Assert.assertEquals(qp.sortWith(((x,y) => (x._1+x._2) > (y._1+y._2))),
                        expected.sortWith(((x,y) => (x._1+x._2) > (y._1+y._2))))
  }
  @Test
  def testAutoComplexQuery3 {
    val lols="holden's hobohut"
    val geoLat = 37.7519528215759
    val geoLong = -122.42086887359619
    val q = SVenueTest where (_.default contains lols) useQueryType("edismax") phraseBoost(_.text,1) phraseBoost(_.name,200) phraseBoost(_.aliases,25) queryField(_.text) queryField(_.ngram_name,0.2) queryField(_.tags,0.01) tieBreaker(0.2)
    val qp = q.queryParams().toList
    val expected = List("defType" -> "edismax",
                        "q" -> "(holden's hobohut)",
                        "start" -> "0",
                        "pf" -> "text",
                        "pf2" -> "text",
                        "pf3" -> "text",
                        "pf" -> "name^200.0",
                        "pf2" -> "name^200.0",
                        "pf3" -> "name^200.0",
                        "pf" -> "aliases^25.0",
                        "pf2" -> "aliases^25.0",
                        "pf3" -> "aliases^25.0",
                        "qf" -> "text",
                        "qf" -> "ngram_name^0.2",
                        "qf" -> "tags^0.01",
                        "tieBreaker" -> "0.2",
                        "rows" -> "10")
    Assert.assertEquals(Nil, ((qp.toSet &~ expected.toSet)).toList)
    Assert.assertEquals(Nil, (expected.toSet &~ qp.toSet).toList)
  }
  @Test
  def testAutoComplexQuery4 {
    val lols="holden's hobohut"
    val geoLat = 37.7519528215759
    val geoLong = -122.42086887359619
    val q = SVenueTest where (_.default contains lols) useQueryType("edismax") phraseBoost(_.text,1) phraseBoost(_.name,200) phraseBoost(_.aliases,25) queryField(_.text) queryField(_.ngram_name,0.2) queryField(_.tags,0.01) tieBreaker(0.2) boostField(_.decayedPopularity1) boostField("recip(sqedist(%s,%s,lat,lng),1,5000,1)".format(geoLat,geoLong))
    val qp = q.queryParams().toList
    val expected = List("defType" -> "edismax",
                        "q" -> "(holden's hobohut)",
                        "start" -> "0",
                        "pf" -> "text",
                        "pf2" -> "text",
                        "pf3" -> "text",
                        "pf" -> "name^200.0",
                        "pf2" -> "name^200.0",
                        "pf3" -> "name^200.0",
                        "pf" -> "aliases^25.0",
                        "pf2" -> "aliases^25.0",
                        "pf3" -> "aliases^25.0",
                        "qf" -> "text",
                        "qf" -> "ngram_name^0.2",
                        "qf" -> "tags^0.01",
                        "tieBreaker" -> "0.2",
                        "bf" -> "recip(sqedist(%s,%s,lat,lng),1,5000,1)".format(geoLat,geoLong),
                        "bf" -> "decayedPopularity1^1.0",
                        "rows" -> "10")
    Assert.assertEquals(Nil, ((qp.toSet &~ expected.toSet)).toList)
    Assert.assertEquals(Nil, (expected.toSet &~ qp.toSet).toList)
//    Assert.assertEquals(qp.sortWith(((x,y) => (x._1+x._2) > (y._1+y._2))),
//                        expected.sortWith(((x,y) => (x._1+x._2) > (y._1+y._2))))
  }

  @Test
  def testAutoComplexQuery5 {
    val lols="holden's hobohut"
    val geoLat = 37.7519528215759
    val geoLong = -122.42086887359619
    val q = SVenueTest where (_.default contains lols) useQueryType("edismax") phraseBoost(_.text,1) phraseBoost(_.name,200) phraseBoost(_.aliases,25) queryField(_.text) queryField(_.ngram_name,0.2) queryField(_.tags,0.01) tieBreaker(0.2) boostField(_.decayedPopularity1) boostField("recip(sqedist(%s,%s,lat,lng),1,5000,1)".format(geoLat,geoLong)) boostQuery((_.name contains lols))
    val qp = q.queryParams().toList
    val expected = List("defType" -> "edismax",
                        "q" -> "(holden's hobohut)",
                        "start" -> "0",
                        "pf" -> "text",
                        "pf2" -> "text",
                        "pf3" -> "text",
                        "pf" -> "name^200.0",
                        "pf2" -> "name^200.0",
                        "pf3" -> "name^200.0",
                        "pf" -> "aliases^25.0",
                        "pf2" -> "aliases^25.0",
                        "pf3" -> "aliases^25.0",
                        "qf" -> "text",
                        "qf" -> "ngram_name^0.2",
                        "qf" -> "tags^0.01",
                        "tieBreaker" -> "0.2",
                        "bq" -> "name:(holden's hobohut)",
                        "bf" -> "recip(sqedist(%s,%s,lat,lng),1,5000,1)".format(geoLat,geoLong),
                        "bf" -> "decayedPopularity1^1.0",
                        "rows" -> "10")
    Assert.assertEquals(Nil, ((qp.toSet &~ expected.toSet)).toList)
    Assert.assertEquals(Nil, (expected.toSet &~ qp.toSet).toList)
  }

  @Test
  def testAutoComplexQuery6 {
    val lols="holden's hobohut"
    val geoLat = 37.7519528215759
    val geoLong = -122.42086887359619
    val q = SVenueTest where (_.default contains lols) useQueryType("edismax") phraseBoost(_.text,1) phraseBoost(_.name,200) phraseBoost(_.aliases,25) queryField(_.text) queryField(_.ngram_name,0.2) queryField(_.tags,0.01) tieBreaker(0.2) boostField(_.decayedPopularity1) boostField("recip(sqedist(%s,%s,lat,lng),1,5000,1)".format(geoLat,geoLong)) boostQuery(_.name contains(lols,10))
    val qp = q.queryParams().toList
    val expected = List("defType" -> "edismax",
                        "q" -> "(holden's hobohut)",
                        "start" -> "0",
                        "pf" -> "text",
                        "pf2" -> "text",
                        "pf3" -> "text",
                        "pf" -> "name^200.0",
                        "pf2" -> "name^200.0",
                        "pf3" -> "name^200.0",
                        "pf" -> "aliases^25.0",
                        "pf2" -> "aliases^25.0",
                        "pf3" -> "aliases^25.0",
                        "qf" -> "text",
                        "qf" -> "ngram_name^0.2",
                        "qf" -> "tags^0.01",
                        "tieBreaker" -> "0.2",
                        "bq" -> "name:((holden's hobohut)^10.0)",
                        "bf" -> "recip(sqedist(%s,%s,lat,lng),1,5000,1)".format(geoLat,geoLong),
                        "bf" -> "decayedPopularity1^1.0",
                        "rows" -> "10")
    Assert.assertEquals(Nil, ((qp.toSet &~ expected.toSet)).toList)
    Assert.assertEquals(Nil, (expected.toSet &~ qp.toSet).toList)
  }

  @Test
  def testAutoComplexQuery7 {
    val lols="holden's hobohut"
    val geoLat = 37.7519528215759
    val geoLong = -122.42086887359619
    val q = SVenueTest where (_.default contains lols) useQueryType("edismax") phraseBoost(_.text,1) phraseBoost(_.name,200) phraseBoost(_.aliases,25) queryField(_.text) queryField(_.ngram_name,0.2) queryField(_.tags,0.01) tieBreaker(0.2) boostField(_.decayedPopularity1) boostField("recip(sqedist(%s,%s,lat,lng),1,5000,1)".format(geoLat,geoLong)) boostQuery(_.name contains(lols,10)) fetchFields(_.id,_.name,_.userid,_.mayorid,_.category_id_0,_.popularity,_.decayedPopularity1,_.lat,_.lng,_.checkin_info,_.score,_.hasSpecial,_.address,_.crossstreet,_.city,_.state,_.zip,_.country,_.checkinCount)
    val qp = q.queryParams().toList
    val expected = List("defType" -> "edismax",
                        "q" -> "(holden's hobohut)",
                        "start" -> "0",
                        "pf" -> "text",
                        "pf2" -> "text",
                        "pf3" -> "text",
                        "pf" -> "name^200.0",
                        "pf2" -> "name^200.0",
                        "pf3" -> "name^200.0",
                        "pf" -> "aliases^25.0",
                        "pf2" -> "aliases^25.0",
                        "pf3" -> "aliases^25.0",
                        "qf" -> "text",
                        "qf" -> "ngram_name^0.2",
                        "qf" -> "tags^0.01",
                        "tieBreaker" -> "0.2",
                        "fl" -> "id,name,userid,mayorid,category_id_0,popularity,decayedPopularity1,lat,lng,checkin_info,score,hasSpecial,address,crossstreet,city,state,zip,country,checkinCount",
                        "bq" -> "name:((holden's hobohut)^10.0)",
                        "bf" -> "recip(sqedist(%s,%s,lat,lng),1,5000,1)".format(geoLat,geoLong),
                        "bf" -> "decayedPopularity1^1.0",
                        "rows" -> "10")
    Assert.assertEquals(Nil, ((qp.toSet &~ expected.toSet)).toList)
    Assert.assertEquals(Nil, (expected.toSet &~ qp.toSet).toList)
  }
  @Test
  def testAutoComplexQuery8 {
    val lols="holden's hobohut"
    val geoLat = 37.7519528215759
    val geoLong = -122.42086887359619
    val q = SVenueTest where (_.default contains lols) useQueryType("edismax") phraseBoost(_.text,1) phraseBoost(_.name,200) phraseBoost(_.aliases,25) queryField(_.text) queryField(_.ngram_name,0.2) queryField(_.tags,0.01) tieBreaker(0.2) boostField(_.decayedPopularity1) boostField("recip(sqedist(%s,%s,lat,lng),1,5000,1)".format(geoLat,geoLong)) boostQuery(_.name contains(lols,10)) fetchFields(_.id,_.name,_.userid,_.mayorid,_.category_id_0,_.popularity,_.decayedPopularity1,_.lat,_.lng,_.checkin_info,_.score,_.hasSpecial,_.address,_.crossstreet,_.city,_.state,_.zip,_.country,_.checkinCount,_.partitionedPopularity)
    val qp = q.queryParams().toList
    val expected = List("defType" -> "edismax",
                        "q" -> "(holden's hobohut)",
                        "start" -> "0",
                        "pf" -> "text",
                        "pf2" -> "text",
                        "pf3" -> "text",
                        "pf" -> "name^200.0",
                        "pf2" -> "name^200.0",
                        "pf3" -> "name^200.0",
                        "pf" -> "aliases^25.0",
                        "pf2" -> "aliases^25.0",
                        "pf3" -> "aliases^25.0",
                        "qf" -> "text",
                        "qf" -> "ngram_name^0.2",
                        "qf" -> "tags^0.01",
                        "tieBreaker" -> "0.2",
                        "fl" -> "id,name,userid,mayorid,category_id_0,popularity,decayedPopularity1,lat,lng,checkin_info,score,hasSpecial,address,crossstreet,city,state,zip,country,checkinCount,partitionedPopularity",
                        "bq" -> "name:((holden's hobohut)^10.0)",
                        "bf" -> "recip(sqedist(%s,%s,lat,lng),1,5000,1)".format(geoLat,geoLong),
                        "bf" -> "decayedPopularity1^1.0",
                        "rows" -> "10")
    Assert.assertEquals(Nil, ((qp.toSet &~ expected.toSet)).toList)
    Assert.assertEquals(Nil, (expected.toSet &~ qp.toSet).toList)
  }
  @Test
  def testAutoComplexQuery9 {
    val lols="holden's hobohut"
    val geoLat = 37.7519528215759
    val geoLong = -122.42086887359619
    val q = SVenueTest where (_.default contains lols) useQueryType("edismax") phraseBoost(_.text,1) phraseBoost(_.name,200) phraseBoost(_.aliases,25) queryField(_.text) queryField(_.ngram_name,0.2) queryField(_.tags,0.01) tieBreaker(0.2) boostField(_.decayedPopularity1) boostField("recip(sqedist(%s,%s,lat,lng),1,5000,1)".format(geoLat,geoLong)) boostQuery(_.name contains(lols,10)) fetchFields(_.id,_.name,_.userid,_.mayorid,_.category_id_0,_.popularity,_.decayedPopularity1,_.lat,_.lng,_.checkin_info,_.score,_.hasSpecial,_.address,_.crossstreet,_.city,_.state,_.zip,_.country,_.checkinCount,_.partitionedPopularity) filter(_.geo_s2_cell_ids inRadius(geoLat, geoLong, 1))
    val qp = q.queryParams().toList
    val expected = List("defType" -> "edismax",
                        "q" -> "(holden's hobohut)",
                        "start" -> "0",
                        "pf" -> "text",
                        "pf2" -> "text",
                        "pf3" -> "text",
                        "pf" -> "name^200.0",
                        "pf2" -> "name^200.0",
                        "pf3" -> "name^200.0",
                        "pf" -> "aliases^25.0",
                        "pf2" -> "aliases^25.0",
                        "pf3" -> "aliases^25.0",
                        "qf" -> "text",
                        "qf" -> "ngram_name^0.2",
                        "qf" -> "tags^0.01",
                        "fq" -> "geo_s2_cell_ids:(+(\"pleaseUseaRealGeoHash\"))",
                        "tieBreaker" -> "0.2",
                        "fl" -> "id,name,userid,mayorid,category_id_0,popularity,decayedPopularity1,lat,lng,checkin_info,score,hasSpecial,address,crossstreet,city,state,zip,country,checkinCount,partitionedPopularity",
                        "bq" -> "name:((holden's hobohut)^10.0)",
                        "bf" -> "recip(sqedist(%s,%s,lat,lng),1,5000,1)".format(geoLat,geoLong),
                        "bf" -> "decayedPopularity1^1.0",
                        "rows" -> "10")
    Assert.assertEquals(Nil, ((qp.toSet &~ expected.toSet)).toList)
    Assert.assertEquals(Nil, (expected.toSet &~ qp.toSet).toList)
  }
  @Test
  def sortwithPopular {
    val lols="holden's hobohut"
    val geoLat = 37.7519528215759
    val geoLong = -122.42086887359619
    val mcat = List("Coffee", "Delicious", "pirates")
    val q = SVenueTest where (_.meta_categories in mcat) useQueryType("edismax") orderDesc(_.decayedPopularity1)
    val qp = q.queryParams().toList
    val cat_str = mcat.map((x=>"\""+x+"\"")).mkString(" OR ")
    val expected = List("defType" -> "edismax",
                        "sort" -> "decayedPopularity1 desc",
                        "q" -> "meta_categories:(+(%s))".format(cat_str),
                        "start" -> "0",
                        "rows" -> "10"
                      )
    Assert.assertEquals(Nil, ((qp.toSet &~ expected.toSet)).toList)
    Assert.assertEquals(Nil, (expected.toSet &~ qp.toSet).toList)
  }
  @Test
  def sortwithPopularandLimit {
    val lols="holden's hobohut"
    val geoLat = 37.7519528215759
    val geoLong = -122.42086887359619
    val mcat = List("Coffee", "Delicious", "pirates")
    val q = SVenueTest where (_.meta_categories in mcat) useQueryType("edismax") orderDesc(_.decayedPopularity1) limit(200)
    val qp = q.queryParams().toList
    val cat_str = mcat.map((x=>"\""+x+"\"")).mkString(" OR ")
    val expected = List("defType" -> "edismax",
                        "sort" -> "decayedPopularity1 desc",
                        "q" -> "meta_categories:(+(%s))".format(cat_str),
                        "start" -> "0",
                        "rows" -> "200"
                      )
    Assert.assertEquals(Nil, ((qp.toSet &~ expected.toSet)).toList)
    Assert.assertEquals(Nil, (expected.toSet &~ qp.toSet).toList)
  }
  @Test
  def sortwithPopularandLimitandCat {
    val lols="holden's hobohut"
    val geoLat = 37.7519528215759
    val geoLong = -122.42086887359619
    val cat = List("test")
    val q = SVenueTest where (_.category_ids in cat) useQueryType("edismax") orderDesc(_.decayedPopularity1) limit(200)
    val qp = q.queryParams().toList
    val cat_str = cat.map((x=>"\""+x+"\"")).mkString(" OR ")
    val expected = List("defType" -> "edismax",
                        "sort" -> "decayedPopularity1 desc",
                        "q" -> "category_ids:(+(%s))".format(cat_str),
                        "start" -> "0",
                        "rows" -> "200"
                      )
    Assert.assertEquals(Nil, ((qp.toSet &~ expected.toSet)).toList)
    Assert.assertEquals(Nil, (expected.toSet &~ qp.toSet).toList)
  }

  @Test
  def sortwithPopularandLimitandOr {
    val lols="holden's hobohut"
    val geoLat = 37.7519528215759
    val geoLong = -122.42086887359619
    val mcat = List("Coffee", "Delicious", "pirates")
    val cat = List("test")
    val q = SVenueTest where (_.category_ids in cat ) or (_.meta_categories in mcat)  useQueryType("edismax") orderDesc(_.decayedPopularity1) limit(200)
    val qp = q.queryParams().toList
    val mcat_str = mcat.map((x=>"\""+x+"\"")).mkString(" OR ")
    val cat_str = cat.map((x=>"\""+x+"\"")).mkString(" OR ")
    val expected = List("defType" -> "edismax",
                        "sort" -> "decayedPopularity1 desc",
                        "q" -> "(meta_categories:(+(%s))) OR (category_ids:(+(%s)))".format(mcat_str,cat_str),
                        "start" -> "0",
                        "rows" -> "200"
                      )
    Assert.assertEquals(Nil, ((qp.toSet &~ expected.toSet)).toList)
    Assert.assertEquals(Nil, (expected.toSet &~ qp.toSet).toList)
  }

  @Test
  def sortwithPopularandAll {
    val lols="holden's hobohut"
    val geoLat = 37.7519528215759
    val geoLong = -122.42086887359619
    val q = SVenueTest where (_.metall any) useQueryType("edismax") orderDesc(_.decayedPopularity1)
    val qp = q.queryParams().toList
    val expected = List("defType" -> "edismax",
                        "sort" -> "decayedPopularity1 desc",
                        "q" -> "*:*",
                        "start" -> "0",
                        "rows" -> "10"
                      )
    Assert.assertEquals(Nil, ((qp.toSet &~ expected.toSet)).toList)
    Assert.assertEquals(Nil, (expected.toSet &~ qp.toSet).toList)
  }



}

