/**
 * 
 */
package com.datastax.hectorjpa.query;

import org.apache.openjpa.kernel.exps.Expression;
import org.apache.openjpa.kernel.exps.ExpressionVisitor;

/**
 * @author Todd Nine
 * 
 */
public abstract class BooleanExpression implements Expression {

  /**
   * 
   */
  private static final long serialVersionUID = -1987557421288975311L;

  protected Expression left;
  protected Expression right;

  /**
   * Default constructor for equality operands
   * 
   * @param parameter
   * @param value
   */
  public BooleanExpression(Expression left, Expression right) {
    this.left = left;
    this.right = right;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.openjpa.kernel.exps.Expression#acceptVisit(org.apache.openjpa
   * .kernel.exps.ExpressionVisitor)
   */
  @Override
  public void acceptVisit(ExpressionVisitor visitor) {
    visitor.enter(this);
    left.acceptVisit(visitor);
    right.acceptVisit(visitor);
    visitor.exit(this);
  }


}
