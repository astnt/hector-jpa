package com.datastax.hectorjpa.meta;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.openjpa.kernel.OpenJPAStateManager;

public class ColumnMeta<V> extends FieldSerializer<V> {

  public ColumnMeta(int fieldId, Serializer<V> serializer) {
    super(fieldId, serializer);
  }

  /**
   * Fetch the field from the column meta data. Will return null if this is the
   * placeholder field
   * 
   * @param stateManager
   * @param metaData
   * @return
   */
  public Object fetchField(OpenJPAStateManager stateManager) {
    return stateManager.fetch(fieldId);
  }

  /**
   * Retreive the bytes and store them in the object passed by state manager
   * 
   * @param stateManager
   * @param column
   */
  public void storeObject(OpenJPAStateManager stateManager,
      HColumn<?, byte[]> column) {

    stateManager.storeObject(fieldId, serializer.fromBytes(column.getValue()));
  }

}
