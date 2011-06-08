package com.datastax.hectorjpa.index;

import java.util.Arrays;

/**
 * A meta holder for index definitions
 * 
 * @author Todd Nine
 * 
 */
public class IndexDefinition implements Comparable<IndexDefinition> {

  private FieldOrder[] indexedFields;

  private IndexOrder[] orderFields;

  public IndexDefinition(FieldOrder[] indexedFields, IndexOrder[] orderFields) {
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
   * Return the index of this field name in our field list.
   * 
   * Returns -1 if it does not exist
   * 
   * @param fieldName
   * @return
   */
  public int getIndex(String fieldName) {
    for (int i = 0; i < indexedFields.length; i++) {
      if (indexedFields[i].getName().equals(fieldName)) {
        return i;
      }
    }

    return -1;
  }

  /**
   * Compare 2 index definitions. Index definitions are compared in the
   * following way
   * 
   * If an order is defined, then all order fields must be preset and in the
   * same order for order comparison to == 0 If these orders are not the same,
   * the shortest number of order operands is returned as less.
   * 
   * If the order operands are the same, the fields are compared. The fields
   * follow the same logic of operands Indexes with less fields are returned
   * with < 0 to encourage the use of shorter rows for faster querying
   * 
   * Not a null safe comparator
   * 
   * @author Todd Nine
   * 
   */
  @Override
  public int compareTo(IndexDefinition def2) {

    int compare = 0;
    
    
    IndexOrder[] def2Order = def2.getOrderFields();

    if(def2Order.length > 0)
    if (orderFields.length > def2Order.length) {
      return 1;
    } else if (orderFields.length < def2Order.length) {
      return -1;
    }

    // fields are same length, compare them
    for (int i = 0; i < orderFields.length; i++) {
      compare = orderFields[i].getName().compareTo(def2Order[i].getName());

      if (compare != 0) {
        return compare;
      }

    }

    // our orders matched, now compare fields
    FieldOrder[] def2Field = def2.getIndexedFields();

    if (indexedFields.length > def2Field.length) {
      return 1;
    } else if (indexedFields.length < def2Field.length) {
      return -1;
    }

    // lengths are the same compare the fields
    int matchCount = 0;

    // same length, compare all fields
    for (int i = 0; i < indexedFields.length; i++) {
      for (int j = 0; j < def2Field.length; j++) {

        if (indexedFields[i].getName().equals(def2Field[j].getName())) {
          matchCount++;
          break;
        }

      }
    }

    return matchCount - indexedFields.length;

  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(indexedFields);
    result = prime * result + Arrays.hashCode(orderFields);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof IndexDefinition))
      return false;
    IndexDefinition other = (IndexDefinition) obj;
    if (!Arrays.equals(orderFields, other.orderFields))
      return false;

    if (indexedFields.length != other.indexedFields.length) {
      return false;
    }

    int matchCount = 0;

    // same length, compare all fields
    for (int i = 0; i < indexedFields.length; i++) {
      for (int j = 0; j < other.indexedFields.length; j++) {

        if (indexedFields[i].equals(other.indexedFields[i])) {
          matchCount++;
          continue;
        }

      }
    }

    return matchCount == indexedFields.length;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("IndexDefinition[")
    .append("indexedFields=")
    .append(Arrays.asList(indexedFields))
    .append(",orderedFields=")
    .append(Arrays.asList(orderFields))
    .append("]");
    return sb.toString();
  }

  
}
