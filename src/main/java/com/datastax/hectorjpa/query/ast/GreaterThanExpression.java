/**
 * 
 */
package com.datastax.hectorjpa.query.ast;

import org.apache.openjpa.kernel.exps.Value;


/**
 * @author Todd Nine
 *
 */
public class GreaterThanExpression extends EqualityExpression {

  
  
  /**
   * 
   */
  private static final long serialVersionUID = -4394613263051650604L;

  public GreaterThanExpression(Value parameter, Value value) {
    super(parameter, value);
  }
  


}
