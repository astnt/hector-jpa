/**
 * 
 */
package com.datastax.hectorjpa.meta;

import me.prettyprint.hector.api.Serializer;

/**
 * Base class for field metadata serialization information
 * @author Todd Nine
 *
 */
public class FieldSerializer<V> {
  
  protected int fieldId;
  protected Serializer<V> serializer;

  public FieldSerializer(int fieldId, Serializer<V> serializer) {
    this.fieldId = fieldId;
    this.serializer = serializer;
  }
  
  public FieldSerializer(int fieldId){
    this.fieldId = fieldId;
  }
  

  /**
   * @return the fieldId
   */
  public int getFieldId() {
    return fieldId;
  }

  /**
   * @return the serializer
   */
  public Serializer<V> getSerializer() {
    return serializer;
  }
  
  protected void setSerializer(Serializer<V> serializer){
    this.serializer = serializer;
  }
}
