/**
 * 
 */
package com.datastax.hectorjpa.index;

import java.util.Comparator;

import org.apache.openjpa.meta.Order;

/**
 * Order element or cassandra ordering
 * 
 * @author Todd Nine
 * 
 */
public class IndexOrder implements Order {


  /**
   * 
   */
  private static final long serialVersionUID = -2234285177676920926L;
  
  private String fieldName;
  private boolean ascending;

  public IndexOrder(String fieldName, boolean ascending) {
    this.fieldName = fieldName;
    this.ascending = ascending;
  }

  

  @Override
  public String getName() {
    return fieldName;
  }

  @Override
  public boolean isAscending() {
    return ascending;
  }

  @Override
  public Comparator<?> getComparator() {
    return null;
  }
  
 

}
