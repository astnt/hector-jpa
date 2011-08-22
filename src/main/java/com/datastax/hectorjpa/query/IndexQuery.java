package com.datastax.hectorjpa.query;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.openjpa.meta.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.query.field.FieldExpression;
import com.datastax.hectorjpa.store.CassandraClassMetaData;

/**
 * Wrapper for data required to perform an index query
 * 
 * @author Todd Nine
 *
 */
public class IndexQuery {
  private static Logger log = LoggerFactory.getLogger(IndexQuery.class);
  private Map<FieldMetaData, FieldExpression> expressions = new HashMap<FieldMetaData, FieldExpression>();
  private CassandraClassMetaData metaData;
  
  public IndexQuery(CassandraClassMetaData metaData){
    this.metaData = metaData;
  }
  
  
  
  public void addExpression(FieldExpression expression){
    log.debug("adding fieldExpression: {}", expression);
    expressions.put(expression.getField(), expression);
  }
  
  /**
   * Lookup the fieldExpression by field name if it exists
   * @param fieldName
   * @return
   */
  public FieldExpression getExpression(FieldMetaData fieldName){
    log.debug("FieldExpression for {} is {}", fieldName, expressions.get(fieldName));
    return expressions.get(fieldName);
  }



  /**
   * @return the FieldExpressions to invoke on this index
   */
  public Collection<FieldExpression> getExpressions() {
    return expressions.values();
  }



  /**
   * @return the metaData
   */
  public CassandraClassMetaData getMetaData() {
    return metaData;
  }
  
  
}
