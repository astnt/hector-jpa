package com.datastax.hectorjpa.index;

import com.datastax.hectorjpa.store.CassandraClassMetaData;

/**
 * A meta holder for index definitions
 * 
 * @author Todd Nine
 *
 */
public class IndexDefinition {

  private FieldOrder[] indexedFields;
  
  private IndexOrder[] orderFields;
  
  private CassandraClassMetaData metaData;

   
  
  public IndexDefinition(CassandraClassMetaData metaData, FieldOrder[] indexedFields, IndexOrder[] orderFields) {
    this.metaData = metaData;
    this.indexedFields = indexedFields;
    this.orderFields = orderFields;
  }

  /**
   * @return the indexedFields
   */
  public FieldOrder[] getIndexedFields() {
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
  
  /**
   * Return the index of this field name in our field list.
   * 
   * Returns -1 if it does not exist
   * 
   * @param fieldName
   * @return
   */
  public int getIndex(String fieldName){
    for(int i = 0; i < indexedFields.length; i ++){
      if(indexedFields[i].getName().equals(fieldName)){
        return i;
      }
    }
    
    return -1;
  }
  
  
}
