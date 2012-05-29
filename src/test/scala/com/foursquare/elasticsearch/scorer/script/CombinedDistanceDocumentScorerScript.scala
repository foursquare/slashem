package com.foursquare.elasticsearch.scorer;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.field.data.NumericDocFieldData;
import org.elasticsearch.index.mapper.geo.GeoPointDocFieldData;
import org.elasticsearch.script.AbstractFloatSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.search.lookup.DocLookup;

import java.util.Map;

/**
 * Note: assumes that the point field is point
 */
case class CombinedDistanceDocumentScorerSearchScript(val lat: Double,
                                                      val lon: Double,
                                                      val weight1: Float,
                                                      val weight2: Float) extends AbstractFloatSearchScript {

  override def runAsFloat(): Float = {
    val myDoc: DocLookup = doc();
    val point: GeoPointDocFieldData = myDoc.get("point").asInstanceOf[GeoPointDocFieldData];
    val popularity: Double = myDoc.numeric("decayedPopularity1").asInstanceOf[NumericDocFieldData[_]].getDoubleValue()
    // up to you to remove score from here or not..., also, possibly, add more weights options
    val myScore: Float = (score() *
                          (1 + weight1 * math.pow(((1.0 * (math.pow(point.distanceInKm(lat, lon), 2.0))) + 1.0), -1.0)
                           + popularity * weight2)).toFloat;
    myScore
  }
}

class ScoreFactory extends NativeScriptFactory {
  def newScript(@Nullable params:  Map[String, Object]): ExecutableScript =  {
    val lat: Double = if (params == null)  1 else XContentMapValues.nodeDoubleValue(params.get("lat"), 0);
    val lon: Double = if (params == null) 1 else XContentMapValues.nodeDoubleValue(params.get("lon"), 0);
    val weight1: Float = if(params == null)  1 else XContentMapValues.nodeFloatValue(params.get("weight1"), 5000.0f);
    val weight2: Float = if(params == null)  1 else XContentMapValues.nodeFloatValue(params.get("weight2"), 0.05f);
    return new CombinedDistanceDocumentScorerSearchScript(lat, lon, weight1, weight2);
  }
}
