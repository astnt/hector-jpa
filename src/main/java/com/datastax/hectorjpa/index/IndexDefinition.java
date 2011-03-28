package com.datastax.hectorjpa.index;

/**
 * A meta holder for index definitions
 * 
 * @author Todd Nine
 *
 */
public class IndexDefinition {

  private String[] indexedFields;
  
  private IndexOrder[] orderFields;

  
  
  public IndexDefinition(String[] indexedFields, IndexOrder[] orderFields) {
    super();
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
   * @param indexedFields the indexedFields to set
   */
  public void setIndexedFields(String[] indexedFields) {
    this.indexedFields = indexedFields;
  }

  /**
   * @return the orderFields
   */
  public IndexOrder[] getOrderFields() {
    return orderFields;
  }

  /**
   * @param orderFields the orderFields to set
   */
  public void setOrderFields(IndexOrder[] orderFields) {
    this.orderFields = orderFields;
  }
  
  
}
