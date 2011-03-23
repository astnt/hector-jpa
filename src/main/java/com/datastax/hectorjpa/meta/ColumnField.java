package com.datastax.hectorjpa.meta;

import java.util.Collection;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.Proxy;

import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Class for serializing columns
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class ColumnField<V> extends Field<V> {

  protected Serializer<?> serializer;
  protected String name;
  protected boolean indexed;
  protected boolean ordered;

  public ColumnField(FieldMetaData fmd) {
    //TODO finish this for indexing
    this(fmd.getIndex(), fmd.getName(), false,fmd.isUsedInOrderBy(), MappingUtils.getSerializer(fmd.getTypeCode()));
  }

  public ColumnField(int fieldId, String fieldName, boolean indexed, boolean ordered, Serializer<?> serializer) {
    super(fieldId);
    this.name = fieldName;
    this.indexed = indexed;
    this.ordered = ordered;
    this.serializer = serializer;
  }
  
  /**
   * Only invoked from subclasses that can't determine serializer
   * @param fieldId
   * @param name
   */
  protected ColumnField(int fieldId, String name){
    super(fieldId);
    this.name = name;
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
    
    Object value = serializer.fromBytes(column.getValue());
    
    if(ordered || indexed){
      //TODO TN generate a proxy here.
    }


    stateManager.storeObject(fieldId, value);
  }

 

}
