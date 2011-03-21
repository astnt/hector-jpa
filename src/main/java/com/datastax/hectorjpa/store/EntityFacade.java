package com.datastax.hectorjpa.store;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Table;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.meta.CollectionField;
import com.datastax.hectorjpa.meta.ColumnField;
import com.datastax.hectorjpa.meta.StaticColumn;

public class EntityFacade implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(EntityFacade.class);

  private static final long serialVersionUID = 4777260639119126462L;

  // required so we always have at least 1 column to select

  private final String columnFamilyName;
  private final Class<?> clazz;
  private final Serializer<?> keySerializer;
  private final MappingUtils mappingUtils;

  /**
   * Fields indexed by id
   */
  private final Map<Integer, ColumnField<?>> columnFieldIds;
  private final Map<Integer, CollectionField<?>> collectionFieldIds;

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

    if (log.isDebugEnabled()) {
      log.debug("PK field name: {} and typeCode: {}",
          classMetaData.getPrimaryKeyFields()[0].getType(),
          classMetaData.getPrimaryKeyFields()[0].getObjectIdFieldTypeCode());
    }

    this.keySerializer = MappingUtils.getSerializer(classMetaData
        .getPrimaryKeyFields()[0]);

    columnFieldIds = new HashMap<Integer, ColumnField<?>>();
    collectionFieldIds = new HashMap<Integer, CollectionField<?>>();

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

        CollectionField<?> collection = new CollectionField(fmds[i], mappingUtils);

        // TODO if fmds[i].getAssociationType() > 0 .. we found an attached
        // entity
        // and need to find it's entityFacade
        collectionFieldIds.put(collection.getFieldId(), collection);

        // indexMetaData.add(new ManyEntityIndex(fmds[i], mappingUtils));

        continue;
      }

      if (fmds[i].getAssociationType() == FieldMetaData.MANY_TO_ONE
          || fmds[i].getAssociationType() == FieldMetaData.ONE_TO_ONE) {

        // TODO one to many
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

      field = new ColumnField(fmds[i], MappingUtils.getSerializer(fmds[i]
          .getTypeCode()));

      // TODO if fmds[i].getAssociationType() > 0 .. we found an attached entity
      // and need to find it's entityFacade
      columnFieldIds.put(field.getFieldId(), field);

    }

    field = new StaticColumn();

    // always add the default "empty val" for read and write. This way if the pk
    // is the only field requested, we'll always get a result from cassandra
    columnFieldIds.put(field.getFieldId(), field);
  }

  public String getColumnFamilyName() {
    return columnFamilyName;
  }

  public Class<?> getClazz() {
    return clazz;
  }

  public Serializer<?> getKeySerializer() {
    return keySerializer;
  }

  /**
   * Get a string array of columns that are within this entity's CF. May not
   * contain all fields if they are stored in a *to-many relationship
   * 
   * @param fields
   * @return
   */
  public String[] getCfColumns(BitSet fieldSet) {

    List<String> fields = new ArrayList<String>();

    ColumnField<?> field = null;

    for (int i = fieldSet.nextSetBit(0), j = 0; i >= 0; i = fieldSet
        .nextSetBit(i + 1), j++) {
      field = columnFieldIds.get(i);

      if (field == null) {
        continue;
      }

      fields.add(field.getName());
    }

    // always add the static column
    fields.add(columnFieldIds.get(StaticColumn.HOLDER_FIELD_ID).getName());

    String[] result = new String[fields.size()];

    fields.toArray(result);

    return result;
  }

  /**
   * Read the result set from column fields into our meta data
   * 
   * @param stateManager
   * @param query
   * @param fields
   */
  public void readColumnResults(OpenJPAStateManager stateManager,
      QueryResult query, BitSet fieldSet) {

    for (int i = fieldSet.nextSetBit(0); i >= 0; i = fieldSet.nextSetBit(i + 1)) {
      ColumnField field = columnFieldIds.get(i);

      if (field == null) {
        continue;
      }

      field.readField(stateManager, query);
    }

  }

  /**
   * Add the columns from the bit set to the mutator with the given time
   * 
   * @param stateManager
   * @param fieldSet
   * @param m
   * @param clockTime
   */
  public void addColumns(OpenJPAStateManager stateManager, BitSet fieldSet,
      Mutator<byte[]> m, long clockTime) {

    byte[] keyBytes = mappingUtils.getKeyBytes(stateManager.getObjectId());

    for (int i = fieldSet.nextSetBit(0); i >= 0; i = fieldSet.nextSetBit(i + 1)) {
      ColumnField field = columnFieldIds.get(i);

      if (field == null) {
        
        
        CollectionField<?> collection = collectionFieldIds.get(i);
        
        //nothing to do
        if(collection == null){
          continue;
        }
        
        //we have a collection, persist it
        collection.addField(stateManager, m, clockTime, keyBytes, this.columnFamilyName);
        
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
