package com.datastax.hectorjpa.meta;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.FieldMetaData;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * Class for serializing columns
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class ColumnField<V> extends Field<V> {

  protected Serializer<V> serializer;
  protected String name;

  public ColumnField(FieldMetaData fmd, Serializer<V> serializer) {
    this(fmd.getIndex(), fmd.getName(), serializer);
  }

  public ColumnField(int fieldId, String fieldName, Serializer<V> serializer) {
    super(fieldId);
    this.name = fieldName;
    this.serializer = serializer;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Adds this field to the mutation with the given clock
   * 
   * @param stateManager
   * @param mutator
   * @param clock
   * @param key
   *          The row key
   * @param cfName
   *          the column family name
   */
  public void addField(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock, byte[] key, String cfName) {

    Object value = stateManager.fetch(fieldId);

    mutator.addInsertion(key, cfName, new HColumnImpl(name, value, clock,
        StringSerializer.get(), serializer));
  }

  /**
   * Read the field from the query result into the opject within the state
   * manager.
   * 
   * @param stateManager
   * @param result
   */
  public void readField(OpenJPAStateManager stateManager,
      QueryResult<ColumnSlice<String, byte[]>> result) {

    HColumn<String, byte[]> column = result.get().getColumnByName(name);

    if (column == null) {
      return;
    }

    stateManager.storeObject(fieldId, serializer.fromBytes(column.getValue()));
  }

  // /**
  // * Fetch the field from the column meta data. Will return null if this is
  // the
  // * placeholder field
  // *
  // * @param stateManager
  // * @param metaData
  // * @return
  // */
  // public Object fetchField(OpenJPAStateManager stateManager) {
  // return stateManager.fetch(fieldId);
  // }
  //
  // /**
  // * Retreive the bytes and store them in the object passed by state manager
  // *
  // * @param stateManager
  // * @param column
  // */
  // public void storeObject(OpenJPAStateManager stateManager,
  // HColumn<?, byte[]> column) {
  //
  // stateManager.storeObject(fieldId, serializer.fromBytes(column.getValue()));
  // }

}
