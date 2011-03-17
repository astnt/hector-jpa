/**
 * 
 */
package com.datastax.hectorjpa.meta;

import me.prettyprint.hector.api.Serializer;

/**
 * Writes an index column name with the static value supplied
 * @author Todd Nine
 *
 */
public class StaticIndexField<V> extends IndexField<V> {

  private static final int FIELD_IDX_BLANK = -1;
  
  private final V value;
  
  public StaticIndexField(V value, Serializer<V> serializer, String indexFieldName) {
    super(FIELD_IDX_BLANK, serializer, indexFieldName);
    
    this.value = value;
  }

}
