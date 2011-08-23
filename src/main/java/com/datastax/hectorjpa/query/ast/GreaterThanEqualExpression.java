/**
 * 
 */
package com.datastax.hectorjpa.query.ast;

import org.apache.openjpa.kernel.exps.Value;


/**
 * @author Todd Nine
 *
 */
public class GreaterThanEqualExpression extends EqualityExpression {

  
  /**
   * 
   */
  private static final long serialVersionUID = 5684006969977737840L;

  public GreaterThanEqualExpression(Value parameter, Value value) {
    super(parameter, value);
  }
  


}
