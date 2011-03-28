/**
 * 
 */
package com.datastax.hectorjpa.query;

import org.apache.openjpa.kernel.ExpressionStoreQuery;
import org.apache.openjpa.kernel.exps.ExpressionFactory;
import org.apache.openjpa.kernel.exps.ExpressionParser;
import org.apache.openjpa.kernel.exps.QueryExpressions;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra specific query implementation
 * 
 * @author Todd Nine
 * 
 */
public class CassandraStoreQuery extends ExpressionStoreQuery {

  
  private static final Logger log = LoggerFactory.getLogger(CassandraStoreQuery.class);
  
  
  /**
   * 
   */
  private static final long serialVersionUID = -756133912086570146L;
 
  public CassandraStoreQuery(ExpressionParser parser) {
    super(parser);
  }

  @Override
  public boolean supportsDataStoreExecution() {
    return true;
  }

  @Override
  protected ResultObjectProvider executeQuery(Executor ex, ClassMetaData base,
      ClassMetaData[] types, boolean subclasses, ExpressionFactory[] facts,
      QueryExpressions[] parsed, Object[] params, Range range) {
    
//    parse[0]
    
    // TODO Auto-generated method stub
    return super.executeQuery(ex, base, types, subclasses, facts, parsed,
        params, range);
  }



  @Override
  protected ExpressionFactory getExpressionFactory(ClassMetaData type) {
    return new CassandraExpressionFactory();
  }

}
