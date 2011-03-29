/**
 * 
 */
package com.datastax.hectorjpa.query.ast;

import org.apache.openjpa.kernel.exps.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Todd Nine
 * 
 */
public class AndExpression extends BooleanExpression {

  private static final Logger log = LoggerFactory
      .getLogger(AndExpression.class);

  /**
   * 
   */
  private static final long serialVersionUID = 8052178304294886758L;

  public AndExpression(Expression left, Expression right) {
    super(left, right);
  }
  
  
 
}
