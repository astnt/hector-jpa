package com.datastax.hectorjpa.meta;

import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.Order;

/**
 * Inner class to encapsulate order field logic and meta data
 * 
 * @author Todd Nine
 * 
 */
public class CollectionOrderField extends AbstractOrderField{


  public CollectionOrderField(Order order, FieldMetaData fmd) {
    super(order, fmd);
  }

  @Override
  protected ClassMetaData getContainerClassMetaData(FieldMetaData fmd) {
    return fmd.getElement().getTypeMetaData();
  }

 
 
}