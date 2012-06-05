/*
 * Applies some simple optimizations
 */
package com.foursquare.slashem

import com.foursquare.slashem.Ast._

object Optimizer {

  def optimizeFilters(filters: List[AbstractClause]): List[AbstractClause] = {
    filters.filter(f => {
      f match {
        //Remove all empty search clauses
        case Clause("*",Splat(),true) => false
        case Clause("_all",Splat(),true) => false
        case _ => true
      }
    })
  }
  def optimizeQuery(clause: AbstractClause): AbstractClause = {
    clause
  }
  def optimizeBoosts(boostQueries: List[AbstractClause]): List[AbstractClause] = {
    boostQueries
  }

}
