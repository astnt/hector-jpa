/**
 * 
 */
package com.datastax.hectorjpa.query.ast;

import java.util.ArrayList;
import java.util.List;

import org.apache.openjpa.kernel.exps.Expression;
import org.apache.openjpa.kernel.exps.ExpressionVisitor;
import org.apache.openjpa.kernel.exps.Value;

import com.datastax.hectorjpa.query.IndexExpression;

/**
 * @author Todd Nine
 * 
 */
public abstract class EqualityExpression implements Expression, IndexExpression {

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

  /* (non-Javadoc)
   * @see com.datastax.hectorjpa.query.CompressionCapable#getEqualityOps()
   */
  @Override
  public List<EqualityExpression> getEqualityOps() {
    List<EqualityExpression> exp = new ArrayList<EqualityExpression>();
    
    exp.add(this);
    
    return exp;
  }

  
  
}
