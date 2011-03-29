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
public class LessThanEqualExpression extends EqualityExpression {

 

  private static final Logger log = LoggerFactory.getLogger(LessThanEqualExpression.class);
  
  
  /**
   * 
   */
  private static final long serialVersionUID = -8763196039567306596L;

  public LessThanEqualExpression(Value parameter, Value value) {
    super(parameter, value);
  }
  


}
