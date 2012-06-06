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


}
