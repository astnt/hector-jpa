package com.datastax.hectorjpa.meta;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.UserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.service.IndexQueue;
import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Class for serializing columns that reprsent many to one and one to one
 * relationships
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class ToOneColumn<V> extends SimpleColumnField<V> {

  private static final Logger log = LoggerFactory
      .getLogger(ToOneColumn.class);

  protected Class<?> targetClass;

  public ToOneColumn(FieldMetaData fmd) {
    super(fmd.getIndex(), fmd.getName());

    targetClass = fmd.getDeclaredType();

    ClassMetaData targetClass = fmd.getDeclaredTypeMetaData();

    serializer = MappingUtils
        .getSerializerForPk(targetClass);


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
  @Override
  public void addField(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock, byte[] key, String cfName, IndexQueue queue) {

    Object instance = stateManager.fetch(fieldId);

    
    //TODO TN remove from opposite index 
    
    // value is null remove it
    if (instance == null) {
      mutator
          .addDeletion(key, cfName, this.name, StringSerializer.get(), clock);
      return;
    }
    
    if (log.isDebugEnabled()) {
      log.debug(
          "Instance: {} fieldId: {} clss: {}",
          new Object[] { instance.getClass().getName(), fieldId,
              stateManager.getManagedInstance() });
    }
    
    
    OpenJPAStateManager targetStateManager = stateManager.getContext()
        .getStateManager(instance);

    // no state, we can't get the order value
    if (targetStateManager == null) {
      throw new UserException(
          String
              .format(
                  "You attempted to store field '%s' on entity '%s'.  However the entity does not have a state manager.  Make sure you enable cascade for this operation or explicity persist it with the entity manager",
                  name, stateManager.getManagedInstance()));
    }
    
    
    Object targetId = MappingUtils.getTargetObject(targetStateManager
        .getObjectId());

    mutator.addInsertion(key, cfName, new HColumnImpl(name, targetId, clock,
        StringSerializer.get(), serializer));
  }

  /**
   * Read the field from the query result into the opject within the state
   * manager.
   * 
   * @param stateManager
   * @param result
   */
  public boolean readField(OpenJPAStateManager stateManager,
      QueryResult<ColumnSlice<String, byte[]>> result) {

    HColumn<String, byte[]> column = result.get().getColumnByName(name);
    if ( log.isDebugEnabled() ) {
      log.debug("Read column: {}", column);
    }
    
    if (column == null) {
      stateManager.store(fieldId, null);
      return false;
    }

    Object id = serializer.fromBytes(column.getValue());

    StoreContext context = stateManager.getContext();

    Object entityId = context.newObjectId(targetClass, id);

    Object returned = context.find(entityId, true, null);

    stateManager.store(fieldId, returned);
    
    return true;
  }

}
