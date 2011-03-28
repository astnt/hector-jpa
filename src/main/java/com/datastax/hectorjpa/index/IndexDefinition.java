package com.datastax.hectorjpa.index;

import com.datastax.hectorjpa.store.CassandraClassMetaData;

/**
 * A meta holder for index definitions
 * 
 * @author Todd Nine
 *
 */
public class IndexDefinition {

  private String[] indexedFields;
  
  private IndexOrder[] orderFields;
  
  private CassandraClassMetaData metaData;

   
  
  public IndexDefinition(CassandraClassMetaData metaData, String[] indexedFields, IndexOrder[] orderFields) {
    this.metaData = metaData;
    this.indexedFields = indexedFields;
    this.orderFields = orderFields;
  }

  /**
   * @return the indexedFields
   */
  public String[] getIndexedFields() {
    return indexedFields;
  }



  /**
   * @return the orderFields
   */
  public IndexOrder[] getOrderFields() {
    return orderFields;
  }



  /**
   * @return the metaData
   */
  public CassandraClassMetaData getMetaData() {
    return metaData;
  }
  
  
}
