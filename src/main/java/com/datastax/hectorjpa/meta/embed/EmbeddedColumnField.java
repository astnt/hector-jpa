package com.datastax.hectorjpa.meta.embed;

import static com.datastax.hectorjpa.serializer.CompositeUtils.newComposite;

import java.util.List;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;

import com.datastax.hectorjpa.meta.StringColumnField;
import com.datastax.hectorjpa.service.IndexQueue;

/**
 * Class for serializing a single Embedded entity to a column value
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class EmbeddedColumnField extends StringColumnField {

  protected static final DynamicCompositeSerializer serializer = new DynamicCompositeSerializer();

  protected final FieldMetaData embeddedField;

  private final EmbeddedEntityValue entityValue;

  public EmbeddedColumnField(FieldMetaData fmd) {
    super(fmd.getIndex(), fmd.getName());

    embeddedField = fmd;

    ClassMetaData declaredClass = fmd.getDeclaredTypeMetaData();

    entityValue = new EmbeddedEntityValue(declaredClass);

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

    Object value = stateManager.fetch(fieldId);

    if (value == null) {
      mutator.addDeletion(key, cfName, name, StringSerializer.get(), clock);
      return;
    }

    OpenJPAStateManager em = stateManager.getContext().getStateManager(value);

    DynamicComposite c = newComposite();

    entityValue.writeToComposite(em, c);

    mutator.addInsertion(key, cfName, new HColumnImpl<String, DynamicComposite>(name, c, clock,
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
    	stateManager.store(fieldId, null);
      return false;
    }

    DynamicComposite composite = serializer.fromBytes(column.getValue());

    // The entity itself is embedded read the de-serialized object and set
    // it's state manager
    OpenJPAStateManager embeddedSm = stateManager.getContext().embed(null,
        null, stateManager, embeddedField);

    stateManager.store(fieldId, embeddedSm.getManagedInstance());
    
    // now load from the composite
    entityValue.getFromComposite(embeddedSm, composite, 0);

    

    return true;
  }
  
  @Override
  public void addFieldNames(List<String> fields) {
   fields.add(name);    
  }
  

}
