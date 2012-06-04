package com.foursquare.slashem
import com.foursquare.slashem._

import com.twitter.util.Duration

import org.bson.types.ObjectId
import org.junit.Test
import org.junit._

import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary.arbitrary

import org.specs.SpecsMatchers
import org.specs.matcher.ScalaCheckMatchers

import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.node.Node
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory._;

import java.util.concurrent.TimeUnit
import java.util.UUID

import scalaj.collection.Imports._

import com.twitter.util.{Duration, ExecutorServiceFuturePool, Future, FuturePool, FutureTask, Throw, TimeoutException}
import java.util.concurrent.{Executors, ExecutorService}

class OptimizeTest extends SpecsMatchers with ScalaCheckMatchers {

  //We test this because the optimizer screwed this up once
  @Test
  def testTermFiltersOptimize {
    val q = ESimplePanda where (_.hugenums contains 1L) filter(_.termsfield in List("termhit", "randomterm"))
    val res1 =  q fetch()
    val res2 = q optimize() fetch()
    Assert.assertEquals(res1.response.results.length, res2.response.results.length)
  }

  @Test
  def testTermFiltersMetallFilter {
    val q = ESimpleGeoPanda where (_.name contains "ordertest") filter(_.metall any)
    val q2 = ESimpleGeoPanda where  (_.name contains "ordertest")
    val res1 =  q fetch()
    val res2 = q optimize() fetch()
    val res3 = q2  fetch()
    Assert.assertEquals(res1.response.results.length, res2.response.results.length)
    Assert.assertEquals(res1.response.results.length, res3.response.results.length)
    Assert.assertEquals(q.optimize(),q2)
  }

  @Test
  def testProduceCorrectListfieldFilterAny {
    val q = SVenueTest where (_.metall any) filter (_.metall any)
    val optimizedQ = q.optimize()
    val qp = q.meta.queryParams(optimizedQ).toList
    Assert.assertEquals(qp.sortWith(_._1 > _._1),
                        List("q" -> "*:*",
                             "start" -> "0",
                             "rows" -> "10").sortWith(_._1 > _._1))
  }

  @Before
  def esSetup {
    ElasticQueryTest.hoboPrepIndex()
  }

  @After
  def esDone {
    ElasticQueryTest.hoboDone()
  }

}
