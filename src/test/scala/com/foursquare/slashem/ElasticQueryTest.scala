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
    val rWithPhraseBoost = ESimplePanda where (_.name contains "loler skates") phraseBoost(_.name,1000) fetch()
    val rNoPhraseBoost = ESimplePanda where (_.name contains "loler skates") fetch()
    Assert.assertEquals(rWithPhraseBoost.response.results.length,3)
    Assert.assertEquals(rNoPhraseBoost.response.results.length,3)
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
                                                                          .endObject()
      ).execute()
    .actionGet();
    val r4 = client.prepareIndex(ESimplePanda.meta.indexName,ESimplePanda.meta.docType,"4c809f4251ada1cdc3790b13").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","loler loler loler loler loler loler loler loler")
                                                                          .field("hobos","sounds like a robot is eating a")
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
