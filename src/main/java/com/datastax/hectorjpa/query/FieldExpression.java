package com.datastax.hectorjpa.query;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;

import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.UnsupportedException;

/**
 * Class to encapsulate an equality expression on a field.  Does not care about asc or descending.  Start can be thought of as Value.MIN and end can be thought of as Value.MAX
 * 
 * A < or <= expression will set the end value
 * a = > or >= will set the start value
 * 
 * @author Todd Nine
 *
 */
public class FieldExpression {

  private FieldMetaData field;
  
  private ComponentEquality startEquality;
  
  private Object start;
  
  private ComponentEquality endEquality;
  
  //the end value in a range scan
  private Object end;
  
  
  
  public FieldExpression(FieldMetaData field){
    this.field = field;
    
    startEquality = ComponentEquality.EQUAL;
    endEquality = ComponentEquality.EQUAL;
    
  }

  /**
   * @return the start
   */
  public Object getStart() {
    return start;
  }
  



  /**
   * @param start the start to set
   * @param inclusive True if this is contains an equality operand I.E =, <=, >=
   */
  public void setStart(Object start, ComponentEquality equality) {
    if(this.start != null){
     throw new UnsupportedException(String.format("You attempted to define the start value on field %s twice.  You must use the || operand to combine the use of the same operand on the same field with 2 values", field));
    }
    
    this.start = start;
    this.startEquality = equality;
  }

  /**
   * @return the end
   */
  public Object getEnd() {
    return end;
  }
  
 

  /**
   * @return the startEquality
   */
  public ComponentEquality getStartEquality() {
    return startEquality;
  }

  /**
   * @return the endEquality
   */
  public ComponentEquality getEndEquality() {
    return endEquality;
  }

  /**
   * @param end the end to set
   * @param inclusive True if this is contains an equality operand I.E =, <=, >=
   */
  public void setEnd(Object end, ComponentEquality equality) {
    if(this.end != null){
      throw new UnsupportedException(String.format("You attempted to define the end value on field %s twice.  You must use the || operand to combine the use of the same operand on the same field with 2 values", field));
     }
    this.end = end;
    this.endEquality = equality;
  }

  /**
   * @return the fieldName
   */
  public FieldMetaData getField() {
    return field;
  }
  

  /**
   * This is a temp hack until inclusive and exclusive bits are set in the DynamicCompositeType.  It sucks and doesn't support much, but it will be gone soon after Ed and Sylvain's changes
   * @param value
   * @return
   */
  private Object increment(Object value){
    if(value instanceof String){
      return ((String)value)+ "\u0000";
    }
    
    if(value instanceof Long){
      return ((Long)value)+1;
    }
    
    if(value instanceof Integer){
      return ((Integer)value)+1;
    }
    
    if(value instanceof Float){
      return ((Float)value)+.1;
    }
    
    if(value instanceof Double){
      return ((Double)value)+.1;
    }
    
    throw new UnsupportedException(String.format("Sorry the type %s could not be incremement to generate a slice query", value.getClass()));
  }
  
  
  
  
  
}
