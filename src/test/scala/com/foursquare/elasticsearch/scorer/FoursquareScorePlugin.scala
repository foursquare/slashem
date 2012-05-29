package com.foursquare.elasticsearch.scorer;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.script.ScriptModule;

/**
 * Provides a fast* score script for our primary use case
 */
class FourSquareScorePlugin extends AbstractPlugin {
  override def name(): String = "foursquare";

  override def description(): String = "foursquare plugin";

  def onModule(module: ScriptModule): Unit = {
    module.registerScript("distance_score_magic", classOf[ScoreFactory]);
  }
}
