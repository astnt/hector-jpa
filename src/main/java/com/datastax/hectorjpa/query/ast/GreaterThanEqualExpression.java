/**
 * 
 */
package com.datastax.hectorjpa.query.ast;

import org.apache.openjpa.kernel.exps.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Todd Nine
 *
 */
public class GreaterThanEqualExpression extends EqualityExpression {

 

  private static final Logger log = LoggerFactory.getLogger(GreaterThanEqualExpression.class);
  
  
  /**
   * 
   */
  private static final long serialVersionUID = 5684006969977737840L;

  public GreaterThanEqualExpression(Value parameter, Value value) {
    super(parameter, value);
  }
  


}
