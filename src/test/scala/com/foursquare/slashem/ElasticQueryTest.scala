package com.foursquare.slashem
import com.foursquare.slashem._

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
import org.elasticsearch.common.xcontent.XContentFactory._;
import java.util.UUID;

object ElasticNode {
  val myUUID = UUID.randomUUID();
  val clusterName = "testcluster"+myUUID.toString()
  val node: Node = {
    nodeBuilder().local(true).client(false).clusterName(clusterName).node()
  }
}


class ElasticQueryTest extends SpecsMatchers with ScalaCheckMatchers {
  @Test
  def testEmptySearch {
    val r = ESimplePanda where (_.name eqs "lolsdonotinsertsomethingwiththisinit") fetch()
    Assert.assertEquals(0,r.response.results.length)
  }
  @Test
  def testNonEmptySearch {
    val r = ESimplePanda where (_.hobos contains "hobos") fetch()
    Assert.assertEquals(1,r.response.results.length)
    //Lets look at the document and make sure its what we expected
    val doc = r.response.results.apply(0)
    Assert.assertEquals("loler skates",doc.name.value)
    Assert.assertEquals("hobos",doc.hobos.value)
    Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b11"),doc.id.is)
  }
  @Test
  def testManyResultsSearch {
    val r = ESimplePanda where (_.name contains "loler") fetch()
    Assert.assertEquals(r.response.results.length,3)
  }
  @Test
  def testPhraseBoostOrdering {
    val rWithLowPhraseBoost = ESimplePanda where (_.name contains "loler skates") phraseBoost(_.name,10) fetch()
    val rWithHighPhraseBoost = ESimplePanda where (_.name contains "loler skates") phraseBoost(_.name,10000) fetch()
    val rNoPhraseBoost = ESimplePanda where (_.name contains "loler skates") fetch()
    Assert.assertEquals(rWithLowPhraseBoost.response.results.length,3)
    Assert.assertEquals(rWithHighPhraseBoost.response.results.length,3)
    Assert.assertEquals(rNoPhraseBoost.response.results.length,3)
    val doc1b = rWithLowPhraseBoost.response.results.apply(2)
    val doc2b = rWithHighPhraseBoost.response.results.apply(2)
    val doc3b = rNoPhraseBoost.response.results.apply(2)
    val lastResult = List(doc1b,doc2b,doc3b)
    lastResult.map(doc => Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b13"), doc.id.is))
    //Make sure the scores are actually impacted by the phraseBoost
    Assert.assertTrue(doc1b.score.value > doc2b.score.value)
    Assert.assertTrue(doc3b.score.value > doc1b.score.value)
  }
  @Test
  def testFieldBoost {
    val r1 = ESimplePanda where (_.magic contains "yes") fetch()
    val r2 = ESimplePanda where (_.magic contains "yes") boostField(_.followers,10) fetch()
    Assert.assertEquals(r1.response.results.length,2)
    Assert.assertEquals(r2.response.results.length,2)
    Assert.assertTrue(r2.response.results.apply(0).score.value > r1.response.results.apply(0).score.value)
  }


  @Before
  def hoboPrepIndex() {
    ESimplePanda.meta.node = ElasticNode.node
    val client = ESimplePanda.meta.client
    val r = client.prepareIndex(ESimplePanda.meta.indexName,ESimplePanda.meta.docType,"4c809f4251ada1cdc3790b10").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","lolerskates")
                                                                          .endObject()
      ).execute()
    .actionGet();
    val r2 = client.prepareIndex(ESimplePanda.meta.indexName,ESimplePanda.meta.docType,"4c809f4251ada1cdc3790b11").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","loler skates")
                                                                          .field("hobos","hobos")
                                                                          .endObject()
      ).execute()
    .actionGet();
    val r3 = client.prepareIndex(ESimplePanda.meta.indexName,ESimplePanda.meta.docType,"4c809f4251ada1cdc3790b12").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","loler loler loler skates")
                                                                          .field("hobos","nyet")
                                                                          .field("followers",0)
                                                                          .field("magic","yes yes")
                                                                          .endObject()
      ).execute()
    .actionGet();
    val r4 = client.prepareIndex(ESimplePanda.meta.indexName,ESimplePanda.meta.docType,"4c809f4251ada1cdc3790b13").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","loler loler loler loler loler loler loler loler chetos are delicious skates skates")
                                                                          .field("hobos","sounds like a robot is eating a")
                                                                          .field("followers",10)
                                                                          .field("magic","yes")
                                                                          .endObject()
      ).execute()
    .actionGet();

    client.admin().indices().prepareRefresh().execute().actionGet()
  }
  @After
  def hoboDone() {
    //TODO: Delete everything from the index
  }

}
