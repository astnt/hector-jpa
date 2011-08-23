/**
 * 
 */
package com.datastax.hectorjpa.query.ast;

import org.apache.openjpa.kernel.exps.Value;


/**
 * @author Todd Nine
 *
 */
public class LessThanExpression extends EqualityExpression {

  
  
  /**
   * 
   */
  private static final long serialVersionUID = 3343130689932621832L;

  public LessThanExpression(Value parameter, Value value) {
    super(parameter, value);
  }

  
}
