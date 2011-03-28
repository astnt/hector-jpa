/**
 * 
 */
package com.datastax.hectorjpa.query;

import org.apache.openjpa.kernel.exps.ExpressionVisitor;
import org.apache.openjpa.kernel.exps.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Todd Nine
 *
 */
public class LessThanExpression extends EqualityExpression {

 
  private static final Logger log = LoggerFactory.getLogger(LessThanExpression.class);
  
  
  /**
   * 
   */
  private static final long serialVersionUID = 3343130689932621832L;

  public LessThanExpression(Value parameter, Value value) {
    super(parameter, value);
  }

  
}
