/**
 * 
 */
package com.datastax.hectorjpa.query.ast;

import org.apache.openjpa.kernel.exps.Expression;


/**
 * @author Todd Nine
 *
 */
public class OrExpression extends BooleanExpression {

  
  /**
   * 
   */
  private static final long serialVersionUID = -2895508846280036545L;

 

  
  public OrExpression(Expression left, Expression right) {
    super(left, right);
  }



}
