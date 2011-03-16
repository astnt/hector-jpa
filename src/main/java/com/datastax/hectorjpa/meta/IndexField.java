/**
 * 
 */
package com.datastax.hectorjpa.meta;

import me.prettyprint.hector.api.Serializer;

/**
 * Class with data to represent an index's single field
 * 
 * @author Todd Nine
 * 
 */
public class IndexField<V> extends FieldSerializer<V> {

  private String indexFieldName;

  private int fieldIndex;

  public IndexField(int fieldIndex, Serializer<V> serializer, String indexFieldName ) {
    super(fieldIndex);
    this.indexFieldName = indexFieldName;
    this.fieldIndex = fieldIndex;

  }
}
