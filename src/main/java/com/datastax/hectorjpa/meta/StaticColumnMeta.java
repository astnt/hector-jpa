package com.datastax.hectorjpa.meta;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.openjpa.kernel.OpenJPAStateManager;

/**
 * Class that always holds static information.  Used for the placeholder column.  This column is required
 * so we can differentiate between a tombstone row and a row where only the id column was requested in the
 * BitSet
 * @author Todd Nine
 *
 * @param <V>
 */
public class StaticColumnMeta<V> extends ColumnMeta<byte[]> {


  public static final int HOLDER_FIELD_ID = -1;


  private static final byte[] EMPTY_VAL = new byte[] { 0 };
  
 public StaticColumnMeta() {
    super(HOLDER_FIELD_ID, BytesArraySerializer.get());
  }

  /**
   * Fetch the field from the column meta data. Will return null if this is
   * the placeholder field
   * 
   * @param stateManager
   * @param metaData
   * @return
   */
  public Object fetchField(OpenJPAStateManager stateManager) {
    return EMPTY_VAL;
  }

  /**
   * Retreive the bytes and store them in the object passed by state manager
   * 
   * @param stateManager
   * @param column
   */
  public void storeObject(OpenJPAStateManager stateManager,
      HColumn<?, byte[]> column) {
    // no op
  }



}