/**
 * 
 */
package com.datastax.hectorjpa.query;

import org.apache.openjpa.kernel.exps.Expression;
import org.apache.openjpa.kernel.exps.ExpressionVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Todd Nine
 *
 */
public class OrExpression extends BooleanExpression {





  private static final Logger log = LoggerFactory.getLogger(OrExpression.class);
  
  /**
   * 
   */
  private static final long serialVersionUID = -2895508846280036545L;

 

  
  public OrExpression(Expression left, Expression right) {
    super(left, right);
  }

  
  @Override
  public void acceptVisit(ExpressionVisitor visitor) {
    // TODO Auto-generated method stub
    
  }

}
