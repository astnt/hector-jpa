/**
 * 
 */
package com.datastax.hectorjpa.index;


/**
 * @author Todd Nine
 * 
 */
public abstract class AbstractEntityIndex {

//  protected static final CollectionCFSet DEFAULT_CF_SET = new CollectionCFSet();
//  
//  /**
//   * the name of the relationship (usually property name)
//   */
//  protected int fieldIndex;
//  protected List<Index> indexes;
//  protected String name;
//
//  protected AbstractEntityIndex(FieldMetaData fmd) {
//    this.fieldIndex = fmd.getIndex();
//    this.name = fmd.getName();
//    indexes = new ArrayList<Index>();
//  }
//
//  /**
//   * @return the fieldIndex
//   */
//  public int getFieldIndex() {
//    return fieldIndex;
//  }
//
//  /**
//   * @param fieldIndex
//   *          the fieldIndex to set
//   */
//  public void setFieldIndex(int fieldIndex) {
//    this.fieldIndex = fieldIndex;
//  }
//
//  protected void addIndex(Index idx) {
//    this.indexes.add(idx);
//  }
//
//  /**
//   * Load the given index and return the object.  Either a proxy or a collection proxy
//   * @param stateManager
//   * @param the keyspace
//   * @return
//   */
//  public abstract void loadIndex(OpenJPAStateManager stateManager, Keyspace keyspace);
//  
//  /**
//   * Construct all indexing operations from the given object
//   * 
//   * @param stateManager
//   */
//  public abstract void writeIndex(OpenJPAStateManager stateManager, Keyspace keyspace);
//
//  /**
//   * Delete this index
//   * 
//   * @param stateManager
//   */
//  public abstract void deleteIndex(OpenJPAStateManager stateManager, Keyspace keyspace);
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see java.lang.Object#hashCode()
//   */
//  @Override
//  public int hashCode() {
//    final int prime = 31;
//    int result = 1;
//    result = prime * result + fieldIndex;
//    return result;
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see java.lang.Object#equals(java.lang.Object)
//   */
//  @Override
//  public boolean equals(Object obj) {
//    if (this == obj)
//      return true;
//    if (obj == null)
//      return false;
//    if (!(obj instanceof AbstractEntityIndex))
//      return false;
//    AbstractEntityIndex other = (AbstractEntityIndex) obj;
//    if (fieldIndex != other.fieldIndex)
//      return false;
//    return true;
//  }
//
//  /**
//   * Inner class to represent in index for a given entity. If I given property
//   * is indexed by 2 indexes, such as email+firstname+lastname and
//   * lastLongDate+firstname+lastname there will be 2 Index instances with 1
//   * indexField and 2 order fields
//   * 
//   * @author Todd Nine
//   * 
//   */
//  protected class Index {
//    private List<IndexField<?>> indexedFields = new ArrayList<IndexField<?>>();
//
//    private List<OrderField> orderFields = new ArrayList<OrderField>();
//
//    public void addIndexField(IndexField<?> field) {
//      indexedFields.add(field);
//    }
//
//    public void addOrderField(OrderField order) {
//      this.orderFields.add(order);
//    }
//
//    public List<IndexField<?>> getIndexedFields() {
//      return indexedFields;
//    }
//
//    public List<OrderField> getOrderFields() {
//      return orderFields;
//    }
//    
//    
//
//  }


}
