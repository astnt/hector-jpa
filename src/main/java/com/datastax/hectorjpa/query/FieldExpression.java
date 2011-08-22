package com.datastax.hectorjpa.query;

import java.math.BigInteger;
import java.util.Date;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;

import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.JavaTypes;
import org.apache.openjpa.util.UnsupportedException;

/**
 * Class to encapsulate an equality expression on a field. Does not care about
 * asc or descending. Start can be thought of as Value.MIN and end can be
 * thought of as Value.MAX
 * 
 * A < or <= expression will set the end value a = > or >= will set the start
 * value
 * 
 * @author Todd Nine
 * 
 */
public class FieldExpression {

  private FieldMetaData field;

  private ComponentEquality startEquality;

  private Object start;

  private boolean startSet = false;
  
  private ComponentEquality endEquality;

  // the end value in a range scan
  private Object end;
  
  private boolean endSet = false;
  

  public FieldExpression(FieldMetaData field) {
    this.field = field;

    startEquality = ComponentEquality.EQUAL;
    endEquality = ComponentEquality.EQUAL;

  }

  /**
   * @return the start
   */
  public Object getStart() {
    if (startSet) {
      return start;
    }

    // check if we're a primitive, if we are and we're undefined, we need to
    // return the MAX value for each one
    switch (field.getDeclaredTypeCode()) {
    case JavaTypes.BOOLEAN:
    case JavaTypes.BOOLEAN_OBJ:
      return false;
    case JavaTypes.BYTE:
    case JavaTypes.BYTE_OBJ:
      return Byte.MIN_VALUE;
    case JavaTypes.CHAR:
    case JavaTypes.CHAR_OBJ:
      return Character.MIN_VALUE;
    case JavaTypes.DOUBLE:
    case JavaTypes.DOUBLE_OBJ:
      return Double.MIN_VALUE;
    case JavaTypes.FLOAT:
    case JavaTypes.FLOAT_OBJ:
      return Float.MIN_VALUE;
    case JavaTypes.INT:
    case JavaTypes.INT_OBJ:
      return Integer.MIN_VALUE;
    case JavaTypes.LONG:
    case JavaTypes.LONG_OBJ:
      return Long.MIN_VALUE;
    case JavaTypes.SHORT:
    case JavaTypes.SHORT_OBJ:
      return Short.MAX_VALUE;
    case JavaTypes.STRING:
      return new String(new char[] { Character.MIN_VALUE });
    case JavaTypes.DATE:
      return new Date(Long.MIN_VALUE);
    case JavaTypes.NUMBER:    
    case JavaTypes.BIGDECIMAL:
    case JavaTypes.BIGINTEGER:
      return BigInteger.valueOf(Long.MIN_VALUE);
    case JavaTypes.LOCALE:
    case JavaTypes.OBJECT:
    case JavaTypes.OID:
      return end;
    }

    return null;

  }

  /**
   * @param start
   *          the start to set
   * @param inclusive
   *          True if this is contains an equality operand I.E =, <=, >=
   */
  public void setStart(Object start, ComponentEquality equality) {
    if (this.startSet) {
      throw new UnsupportedException(
          String
              .format(
                  "You attempted to define the start value on field %s twice.  You must use the || operand to combine the use of the same operand on the same field with 2 values",
                  field));
    }

    this.start = start;
    this.startEquality = equality;
    this.startSet = true;
  }

  /**
   * @return the end
   */
  public Object getEnd() {
    // we need to get the min if we're null
    if (this.endSet) {
      return end;
    }

    // check if we're a primitive, if we are and we're undefined, we need to
    // return the MAX value for each one
    switch (field.getDeclaredTypeCode()) {
    case JavaTypes.BOOLEAN:
    case JavaTypes.BOOLEAN_OBJ:
      return true;
    case JavaTypes.BYTE:
    case JavaTypes.BYTE_OBJ:
      return Byte.MAX_VALUE;
    case JavaTypes.CHAR:
    case JavaTypes.CHAR_OBJ:
      return Character.MAX_VALUE;
    case JavaTypes.DOUBLE:
    case JavaTypes.DOUBLE_OBJ:
      return Double.MAX_VALUE;
    case JavaTypes.FLOAT:
    case JavaTypes.FLOAT_OBJ:
      return Float.MAX_VALUE;
    case JavaTypes.INT:
    case JavaTypes.INT_OBJ:
      return Integer.MAX_VALUE;
    case JavaTypes.LONG:
    case JavaTypes.LONG_OBJ:
      return Long.MAX_VALUE;
    case JavaTypes.SHORT:
    case JavaTypes.SHORT_OBJ:
      return Short.MAX_VALUE;
    case JavaTypes.STRING:
      return new String(new char[] { Character.MAX_VALUE });
    case JavaTypes.DATE:
      return new Date(Long.MAX_VALUE);
    case JavaTypes.NUMBER:
    case JavaTypes.BIGDECIMAL:
    case JavaTypes.BIGINTEGER:
      return BigInteger.valueOf(Long.MAX_VALUE);
    case JavaTypes.LOCALE:
    case JavaTypes.OBJECT:
    case JavaTypes.OID:
      return end;
    }

    return null;

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
   * @param end
   *          the end to set
   * @param inclusive
   *          True if this is contains an equality operand I.E =, <=, >=
   */
  public void setEnd(Object end, ComponentEquality equality) {
    if (this.endSet) {
      throw new UnsupportedException(
          String
              .format(
                  "You attempted to define the end value on field %s twice.  You must use the || operand to combine the use of the same operand on the same field with 2 values",
                  field));
    }
    this.end = end;
    this.endEquality = equality;
    this.endSet = true;
  }

  /**
   * @return the fieldName
   */
  public FieldMetaData getField() {
    return field;
  }

  /**
   * This is a temp hack until inclusive and exclusive bits are set in the
   * DynamicCompositeType. It sucks and doesn't support much, but it will be
   * gone soon after Ed and Sylvain's changes
   * 
   * @param value
   * @return
   */
  private Object increment(Object value) {
    if (value instanceof String) {
      return ((String) value) + "\u0000";
    }

    if (value instanceof Long) {
      return ((Long) value) + 1;
    }

    if (value instanceof Integer) {
      return ((Integer) value) + 1;
    }

    if (value instanceof Float) {
      return ((Float) value) + .1;
    }

    if (value instanceof Double) {
      return ((Double) value) + .1;
    }

    throw new UnsupportedException(String.format(
        "Sorry the type %s could not be incremement to generate a slice query",
        value.getClass()));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("start=").append(start).append(",end=").append(end)
        .append(",startEquality=").append(startEquality)
        .append(",endEquality=").append(endEquality).append(",field=")
        .append(field.getName());
    return sb.toString();
  }

}
