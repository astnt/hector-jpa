/**
 * 
 */
package com.datastax.hectorjpa.query;

import java.util.Set;
import java.util.TreeSet;

import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.kernel.exps.Expression;
import org.apache.openjpa.kernel.exps.ExpressionVisitor;
import org.apache.openjpa.kernel.exps.Value;

import com.datastax.hectorjpa.meta.MetaCache;
import com.datastax.hectorjpa.query.ast.EqualityExpression;
import com.datastax.hectorjpa.store.CassandraClassMetaData;

/**
 * A visitor that collects all the fields in our expression tree to be used to
 * determine which index to utilize
 * 
 * @author Todd Nine
 * 
 */
public class QueryExecutionVisitor implements ExpressionVisitor {

  // TODO, need a comparator that will compare only by order fields

  private Set<DynamicComposite> candidates = new TreeSet<DynamicComposite>();

  private Value[] orderFields;
  private MetaCache metaCache;
  private CassandraClassMetaData metaData;

  public QueryExecutionVisitor(Value[] orderFields, MetaCache metaCache,
      CassandraClassMetaData metaData) {
    this.orderFields = orderFields;
    this.metaCache = metaCache;
    this.metaData = metaData;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.openjpa.kernel.exps.ExpressionVisitor#enter(org.apache.openjpa
   * .kernel.exps.Expression)
   */
  @Override
  public void enter(Expression exp) {
    if (exp instanceof IndexExpression) {
      executeQuery((IndexExpression) exp);
      return;
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.openjpa.kernel.exps.ExpressionVisitor#exit(org.apache.openjpa
   * .kernel.exps.Expression)
   */
  @Override
  public void exit(Expression exp) {
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.openjpa.kernel.exps.ExpressionVisitor#enter(org.apache.openjpa
   * .kernel.exps.Value)
   */
  @Override
  public void enter(Value val) {

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.openjpa.kernel.exps.ExpressionVisitor#exit(org.apache.openjpa
   * .kernel.exps.Value)
   */
  @Override
  public void exit(Value val) {
  }

  /**
   * Find our index from our meta and query it
   * 
   * @param exp
   */
  private void executeQuery(IndexExpression exp) {
    
    CassandraClassMetaData meta = null;
    
    for(EqualityExpression ex: exp.getEqualityOps()){
//      ex.
    }
    
   }

  /**
   * Get all possible candidates and their dynamic columns
   * 
   * @return
   */
  public Set<DynamicComposite> getCandidates() {
    return candidates;
  }
}
