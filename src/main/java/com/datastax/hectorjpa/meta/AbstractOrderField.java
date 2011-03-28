package com.datastax.hectorjpa.meta;

import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.Order;

/**
 * Inner class to encapsulate order field logic and meta data
 * 
 * @author Todd Nine
 * 
 */
public abstract class AbstractOrderField extends AbstractIndexField {

  private Order order;

  public AbstractOrderField(Order order, FieldMetaData fmd) {
    super(fmd, order.getName());
    this.order = order;
    
  }
 
}