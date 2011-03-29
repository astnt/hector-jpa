/**
 * 
 */
package com.datastax.hectorjpa.query;

import java.util.List;

import com.datastax.hectorjpa.query.ast.EqualityExpression;

/**
 * Interface to provider operations for expression that can be compressed.  Any children
 * of && expressions that do not have an || are compressible by default.
 * 
 * @author Todd Nine
 *
 */
public interface IndexExpression {

  /**
   * Return all fields that can be compressed into a single expression
   * @return
   */
  public List<EqualityExpression> getEqualityOps();
}
