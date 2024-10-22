package com.datastax.hectorjpa.meta.embed;

import java.util.List;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;

import com.datastax.hectorjpa.meta.StringColumnField;
import com.datastax.hectorjpa.serialize.EmbeddedSerializer;
import com.datastax.hectorjpa.service.IndexQueue;

/**
 * Class for serializing a single Embedded entity to a column value
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class EmbeddedCollectionColumnField extends StringColumnField {

  private final StringColumnField embeddedDelegate;

  public EmbeddedCollectionColumnField(FieldMetaData fmd, EmbeddedSerializer serializer) {
    super(fmd.getIndex(), fmd.getName());

    ClassMetaData declaredClass = fmd.getElement().getDeclaredTypeMetaData();

    if (declaredClass == null) {
      embeddedDelegate = new SerializedCollectionColumnField(fmd, serializer);
    } else {
      embeddedDelegate = new EmbeddedableCollectionColumnField(fmd,  declaredClass);
    }

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
      Mutator<byte[]> mutator, long clock, byte[] key, String cfName,
      IndexQueue queue) {

    embeddedDelegate.addField(stateManager, mutator, clock, key, cfName, queue);

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

    return embeddedDelegate.readField(stateManager, result);
  }
  
  @Override
  public void addFieldNames(List<String> fields) {
    embeddedDelegate.addFieldNames(fields);
  }
  

}
