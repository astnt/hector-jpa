package com.datastax.hectorjpa.meta;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.FieldMetaData;

import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Class for serializing columns
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class SimpleColumnField<V> extends StringColumnField<V> {

  protected Serializer<?> serializer;
  protected boolean indexed;
  protected boolean ordered;

  public SimpleColumnField(FieldMetaData fmd) {
    this(fmd.getIndex(), fmd.getName(), false, fmd.isUsedInOrderBy(),
        MappingUtils.getSerializer(fmd.getTypeCode()));
  }

  public SimpleColumnField(int fieldId, String fieldName, boolean indexed,
      boolean ordered, Serializer<?> serializer) {
    super(fieldId, fieldName);
    this.indexed = indexed;
    this.ordered = ordered;
    this.serializer = serializer;
  }
  
  
  public SimpleColumnField(int fieldId, String fieldName){
    super(fieldId, fieldName);
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
    
    if(value == null){
      mutator.addDeletion(key, cfName, name, StringSerializer.get(), clock);
      return;
    }

    mutator.addInsertion(key, cfName, new HColumnImpl(name, value, clock,
        StringSerializer.get(), serializer));

  }

  /**
   * Read the field from the query result into the opject within the state
   * manager.
   * 
   * @param stateManager
   * @param result
   * @return True if the field was loaded. False otherwise
   */
  public boolean readField(OpenJPAStateManager stateManager,
      QueryResult<ColumnSlice<String, byte[]>> result) {

    HColumn<String, byte[]> column = result.get().getColumnByName(name);

    if (column == null) {
      return false;
    }

    Object value = serializer.fromBytes(column.getValue());

    stateManager.storeObject(fieldId, value);

    return true;
  }

}
