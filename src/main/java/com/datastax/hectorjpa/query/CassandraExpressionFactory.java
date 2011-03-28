/**
 * 
 */
package com.datastax.hectorjpa.query;

import java.util.List;

import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.kernel.exps.Expression;
import org.apache.openjpa.kernel.exps.InMemoryExpressionFactory;
import org.apache.openjpa.kernel.exps.QueryExpressions;
import org.apache.openjpa.kernel.exps.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Will perform operations directly against our Cassandra index when possible.
 * Otherwise in memory operations may be performed.
 * 
 * @author Todd Nine
 * 
 */
public class CassandraExpressionFactory extends InMemoryExpressionFactory {

  private static final Logger log = LoggerFactory
      .getLogger(CassandraExpressionFactory.class);

  @Override
  public Expression equal(Value v1, Value v2) {
    return new EqualExpression(v1, v2);
  }

  @Override
  public Expression lessThan(Value v1, Value v2) {
    return new LessThanExpression(v1, v2);
  }

  @Override
  public Expression greaterThan(Value v1, Value v2) {
    return new GreaterThanExpression(v1, v2);
  }

  @Override
  public Expression lessThanEqual(Value v1, Value v2) {
    return new LessThanEqualExpression(v1, v2);
  }

  @Override
  public Expression greaterThanEqual(Value v1, Value v2) {
    return new GreaterThanEqualExpression(v1, v2);
  }

  @Override
  public Expression and(Expression exp1, Expression exp2) {
    return new AndExpression(exp1, exp2);
  }

  @Override
  public Expression or(Expression exp1, Expression exp2) {
    return new OrExpression(exp1, exp2);
  }

  /* (non-Javadoc)
   * @see org.apache.openjpa.kernel.exps.InMemoryExpressionFactory#order(org.apache.openjpa.kernel.exps.QueryExpressions, java.util.List, org.apache.openjpa.kernel.StoreContext, java.lang.Object[])
   */
  @Override
  public List order(QueryExpressions exps, List matches, StoreContext ctx,
      Object[] params) {
    // TODO Auto-generated method stub
    return super.order(exps, matches, ctx, params);
  }

}
