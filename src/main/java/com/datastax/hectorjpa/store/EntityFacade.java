package com.datastax.hectorjpa.store;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.meta.ColumnField;
import com.datastax.hectorjpa.meta.StaticColumn;
import com.datastax.hectorjpa.meta.ToOneColumn;
import com.datastax.hectorjpa.meta.collection.AbstractCollectionField;
import com.datastax.hectorjpa.meta.collection.OrderedCollectionField;
import com.datastax.hectorjpa.meta.collection.UnorderedCollectionField;

public class EntityFacade implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(EntityFacade.class);

  private static final long serialVersionUID = 4777260639119126462L;

  private final String columnFamilyName;
  private final Class<?> clazz;
  private final Serializer<?> keySerializer;
  private final MappingUtils mappingUtils;

  /**
   * Fields indexed by id
   */
  private final Map<Integer, ColumnField<?>> columnFieldIds;
  private final Map<Integer, AbstractCollectionField<?>> collectionFieldIds;

  /**
   * Default constructor
   * 
   * @param classMetaData
   *          The class meta data
   * @param mappingUtils
   *          The mapping utils to use for byte mapping
   */
  public EntityFacade(ClassMetaData classMetaData, MappingUtils mappingUtils) {

    clazz = classMetaData.getDescribedType();

    this.columnFamilyName = mappingUtils.getColumnFamily(clazz);

   

    this.keySerializer = MappingUtils.getSerializerForPk(classMetaData);

    columnFieldIds = new HashMap<Integer, ColumnField<?>>();
    collectionFieldIds = new HashMap<Integer, AbstractCollectionField<?>>();

    this.mappingUtils = mappingUtils;

    // indexMetaData = new HashSet<AbstractEntityIndex>();

    FieldMetaData[] fmds = classMetaData.getFields();
    ColumnField<?> field = null;

    // parse all columns, we only want to do this on the first inti
    for (int i = 0; i < fmds.length; i++) {

      // not in the bit set to use, isn't managed or saved, or is a primary key,
      // ignore
      if (fmds[i].getManagement() == FieldMetaData.MANAGE_NONE
          || fmds[i].isPrimaryKey()) {
        continue;
      }

      if (fmds[i].getAssociationType() == FieldMetaData.ONE_TO_MANY
          || fmds[i].getAssociationType() == FieldMetaData.MANY_TO_MANY) {

        AbstractCollectionField<?> collection = null;

        if (fmds[i].getOrders().length > 0) {
          collection = new OrderedCollectionField(fmds[i], mappingUtils);
        } else {
          collection = new UnorderedCollectionField(fmds[i], mappingUtils);
        }

        // TODO if fmds[i].getAssociationType() > 0 .. we found an attached
        // entity
        // and need to find it's entityFacade
        collectionFieldIds.put(collection.getFieldId(), collection);

        // indexMetaData.add(new ManyEntityIndex(fmds[i], mappingUtils));

        continue;
      }

      if (fmds[i].getAssociationType() == FieldMetaData.MANY_TO_ONE
          || fmds[i].getAssociationType() == FieldMetaData.ONE_TO_ONE) {

        ToOneColumn<?> toOne = new ToOneColumn(fmds[i], mappingUtils);

        columnFieldIds.put(i, toOne);

        continue;
      }

      if (log.isDebugEnabled()) {
        log.debug(
            "field name {} typeCode {} associationType: {} declaredType: {} embeddedMetaData: {}",
            new Object[] { fmds[i].getName(), fmds[i].getTypeCode(),
                fmds[i].getAssociationType(),
                fmds[i].getDeclaredType().getName(),
                fmds[i].getElement().getDeclaredTypeMetaData() });
      }

      field = new ColumnField(fmds[i]);

      // TODO if fmds[i].getAssociationType() > 0 .. we found an attached entity
      // and need to find it's entityFacade
      columnFieldIds.put(field.getFieldId(), field);

    }

    field = new StaticColumn();

    // always add the default "empty val" for read and write. This way if the pk
    // is the only field requested, we'll always get a result from cassandra
    columnFieldIds.put(field.getFieldId(), field);
  }

  // public String getColumnFamilyName() {
  // return columnFamilyName;
  // }
  //
  // public Class<?> getClazz() {
  // return clazz;
  // }
  //
  // public Serializer<?> getKeySerializer() {
  // return keySerializer;
  // }

  /**
   * Delete the entity with the given statemanager.  The given clock time is used for the delete of indexes
   * @param stateManager
   * @param mutator
   * @param clock
   */
  public void delete(OpenJPAStateManager stateManager, Mutator mutator, long clock) {
    mutator.addDeletion(mappingUtils.getKeyBytes(stateManager.getObjectId()),
        columnFamilyName, null, StringSerializer.get());
  }

  /**
   * Load all columns for this class specified in the bit set
   * 
   * @param stateManager
   * @param fieldSet
   * @return true if the entity was present (I.E the marker column was found)
   *         otherwise false is returned.
   */
  public boolean loadColumns(OpenJPAStateManager stateManager, BitSet fieldSet,
      Keyspace keyspace) {

    List<String> fields = new ArrayList<String>();

    ColumnField<?> field = null;
    AbstractCollectionField<?> collectionField = null;
    Object entityId = stateManager.getObjectId();
    
    //This entity has never been persisted, we can't possibly load it
    if(mappingUtils.getTargetObject(entityId) == null){
      return false;
    }

    // load all collections as we encounter them since they're seperate row
    // reads and construct columns for sliceQuery in primary CF
    for (int i = fieldSet.nextSetBit(0); i >= 0; i = fieldSet.nextSetBit(i + 1)) {
      field = columnFieldIds.get(i);

      if (field == null) {

        collectionField = collectionFieldIds.get(i);
        if ( log.isDebugEnabled() ) {
          log.debug("loadColumns called on collection field: {}", collectionField);
        }
        // nothting to do
        if (collectionField == null) {
          continue;
        }

        int size = stateManager.getContext().getFetchConfiguration()
            .getFetchBatchSize();

        // now query and load this field
        SliceQuery<byte[], DynamicComposite, byte[]> query = collectionField
            .createQuery(entityId, keyspace, size);
        if ( log.isDebugEnabled()) {
          log.debug("constructed sliceQuery for collection: {}", query);
        }
        collectionField.readField(stateManager, query.execute());

        continue;
      }

      fields.add(field.getName());
    }

    fields.add(columnFieldIds.get(StaticColumn.HOLDER_FIELD_ID).getName());

    // now load all the columns in the CF.
    SliceQuery<byte[], String, byte[]> query = mappingUtils.buildSliceQuery(
        entityId, fields, columnFamilyName, keyspace);

    QueryResult<ColumnSlice<String, byte[]>> result = query.execute();

    // read the field
    for (int i = fieldSet.nextSetBit(0); i >= 0; i = fieldSet.nextSetBit(i + 1)) {
      field = columnFieldIds.get(i);

      if (field == null) {
        continue;
      }

      field.readField(stateManager, result);
    }

    // only need to check > 0. If the entity wasn't tombstoned then we would
    // have loaded the static jpa marker column

    return result.get().getColumns().size() > 0;

  }
  

  /**
   * Loads only the jpa marker column to check for cassandra existence
   * 
   * @param stateManager
   * @param fieldSet
   * @return true if the entity was present (I.E the marker column was found)
   *         otherwise false is returned.
   */
  public boolean exists(OpenJPAStateManager stateManager,  Keyspace keyspace) {

    List<String> fields = new ArrayList<String>();

    Object entityId = stateManager.getObjectId();
    
    //This entity has never been persisted, we can't possibly load it
    if(mappingUtils.getTargetObject(entityId) == null){
      return false;
    }

    fields.add(columnFieldIds.get(StaticColumn.HOLDER_FIELD_ID).getName());


    // now load all the columns in the CF.
    SliceQuery<byte[], String, byte[]> query = mappingUtils.buildSliceQuery(
        entityId, fields, columnFamilyName, keyspace);

    QueryResult<ColumnSlice<String, byte[]>> result = query.execute();
    
   
    // only need to check > 0. If the entity wasn't tombstoned then we would
    // have loaded the static jpa marker column

    return result.get().getColumns().size() > 0;

  }


  /**
   * Add the columns from the bit set to the mutator with the given time
   * 
   * @param stateManager
   * @param fieldSet
   * @param m
   * @param clockTime The time to use for write and deletes
   */
  public void addColumns(OpenJPAStateManager stateManager, BitSet fieldSet,
      Mutator<byte[]> m, long clockTime) {

    byte[] keyBytes = mappingUtils.getKeyBytes(stateManager.getObjectId());
    
    for (int i = fieldSet.nextSetBit(0); i >= 0; i = fieldSet.nextSetBit(i + 1)) {
      ColumnField field = columnFieldIds.get(i);

      if (field == null) {

        AbstractCollectionField<?> collection = collectionFieldIds.get(i);

        // nothing to do
        if (collection == null) {
          continue;
        }

        // we have a collection, persist it
        collection.addField(stateManager, m, clockTime, keyBytes,
            this.columnFamilyName);

        continue;

      }

      field.addField(stateManager, m, clockTime, keyBytes,
          this.columnFamilyName);

    }

    // always add the placeholder field
    columnFieldIds.get(StaticColumn.HOLDER_FIELD_ID).addField(stateManager, m,
        clockTime, keyBytes, this.columnFamilyName);

  }

  @Override
  public String toString() {

    return String.format(
        "EntityFacade[class: %s, columnFamily: %s, columnNames: %s]",
        clazz.getName(), columnFamilyName,
        Arrays.toString(columnFieldIds.entrySet().toArray()));

  }

}
