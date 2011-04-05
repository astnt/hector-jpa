package com.datastax.hectorjpa.meta;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.FieldMetaData;

import com.datastax.hectorjpa.serialize.EmbeddedSerializer;

/**
 * Class for serializing columns
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class EmbeddedColumnField<V> extends StringColumnField<V> {

  protected static final Serializer<?> serializer = ByteBufferSerializer.get();
  
  protected final EmbeddedSerializer embeddedSerializer;

  public EmbeddedColumnField(FieldMetaData fmd, EmbeddedSerializer embeddedSerializer) {
    super(fmd.getIndex(), fmd.getName());
    this.embeddedSerializer = embeddedSerializer;
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
    
    ByteBuffer bytes = embeddedSerializer.getBytes(value);

    mutator.addInsertion(key, cfName, new HColumnImpl(name, bytes, clock,
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

    ByteBuffer bytes = (ByteBuffer) serializer.fromBytes(column.getValue());
    
    Object value = embeddedSerializer.getObject(bytes);

    stateManager.store(fieldId, value);

    return true;
  }

}
