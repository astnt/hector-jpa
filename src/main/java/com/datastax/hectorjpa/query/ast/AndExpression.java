/**
 * 
 */
package com.datastax.hectorjpa.query.ast;

import org.apache.openjpa.kernel.exps.Expression;

/**
 * @author Todd Nine
 * 
 */
public class AndExpression extends BooleanExpression {


  /**
   * 
   */
  private static final long serialVersionUID = 8052178304294886758L;

  public AndExpression(Expression left, Expression right) {
    super(left, right);
  }
  
  
 
}
