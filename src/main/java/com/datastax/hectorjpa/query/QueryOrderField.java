package com.datastax.hectorjpa.query;

import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.Order;

import com.datastax.hectorjpa.index.AbstractOrderField;


/**
 * Inner class to encapsulate order field logic and meta data
 * 
 * @author Todd Nine
 * 
 */
public class QueryOrderField  extends AbstractOrderField {

  public QueryOrderField(Order order, FieldMetaData fmd) {
    super(order, fmd);
  }

  @Override
  protected ClassMetaData getContainerClassMetaData(FieldMetaData fmd) {
    return fmd.getDefiningMetaData();
  }

  

 
}