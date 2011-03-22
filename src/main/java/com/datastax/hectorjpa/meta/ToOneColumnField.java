package com.datastax.hectorjpa.meta;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;

import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Class for serializing columns that reprsent many to one and one to one relationships
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class ToOneColumnField<V> extends ColumnField<V> {

  protected Class<?> targetClass;
  protected MappingUtils mappingUtils;
  

  public ToOneColumnField(FieldMetaData fmd, MappingUtils mappingUtils) {
    super(fmd.getIndex(), fmd.getName());
    
    targetClass = fmd.getDeclaredType();
    
    ClassMetaData targetClass = fmd.getDeclaredTypeMetaData();
        
    serializer = MappingUtils.getSerializer(targetClass.getPrimaryKeyFields()[0]);
    
    this.mappingUtils = mappingUtils;
    

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

    Object instance = stateManager.fetch(fieldId);

    //value is null remove it
    if(instance == null){
      mutator.addDeletion(key, cfName, this.name, StringSerializer.get(), clock);
      return;
    }

    Object targetId = mappingUtils.getTargetObject(stateManager.getContext().getObjectId(instance));
  
    
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
  public void readField(OpenJPAStateManager stateManager,
      QueryResult<ColumnSlice<String, byte[]>> result) {

    HColumn<String, byte[]> column = result.get().getColumnByName(name);

    if (column == null) {
      stateManager.storeObject(fieldId, null);
      return;
    }

    Object id = serializer.fromBytes(column.getValue());
        
    StoreContext context = stateManager.getContext();

    Object entityId = context.newObjectId(targetClass, id);
    
    Object returned = context.find(entityId, true, null);
    
    stateManager.storeObject(fieldId, returned);
  }

 

}
