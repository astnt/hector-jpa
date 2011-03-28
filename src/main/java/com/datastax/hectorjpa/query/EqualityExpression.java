/**
 * 
 */
package com.datastax.hectorjpa.query;

import org.apache.openjpa.kernel.exps.Expression;
import org.apache.openjpa.kernel.exps.ExpressionVisitor;
import org.apache.openjpa.kernel.exps.Value;

/**
 * @author Todd Nine
 * 
 */
public abstract class EqualityExpression implements Expression {

  /**
   * 
   */
  private static final long serialVersionUID = -1987557421288975311L;

  protected Value parameter;
  protected Value value;

  /**
   * Default constructor for equality operands
   * 
   * @param parameter
   * @param value
   */
  public EqualityExpression(Value parameter, Value value) {
    this.parameter = parameter;
    this.value = value;
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
    parameter.acceptVisit(visitor);
    value.acceptVisit(visitor);
    visitor.exit(this);

  }

}
