// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.slashem

trait GeoCover {
  def boundsCoverString(maxCells: Int = 0, minLevel: Int = 0, maxLevel: Int = 0): Seq[String]
}
