package com.datastax.hectorjpa.store;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.persistence.Table;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.index.AbstractEntityIndex;
import com.datastax.hectorjpa.index.ManyEntityIndex;
import com.datastax.hectorjpa.meta.ColumnMeta;
import com.datastax.hectorjpa.meta.StaticColumnMeta;

public class EntityFacade implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(EntityFacade.class);

  private static final long serialVersionUID = 4777260639119126462L;


  // required so we always have at least 1 column to select
  private static final String EMPTY_COL = "jpaholder";
  
  private final String columnFamilyName;
  private final Class<?> clazz;
  private final Serializer<?> keySerializer;
  private final Map<String, ColumnMeta<?>> columnMetas;
  private final Set<AbstractEntityIndex> indexMetaData;

  public EntityFacade(ClassMetaData classMetaData, BitSet fields, MappingUtils mappingUtils) {

    clazz = classMetaData.getDescribedType();
    this.columnFamilyName = clazz.getAnnotation(Table.class) != null ? clazz
        .getAnnotation(Table.class).name() : clazz.getSimpleName();
    if (log.isDebugEnabled()) {
      log.debug("PK field name: {} and typeCode: {}",
          classMetaData.getPrimaryKeyFields()[0].getType(),
          classMetaData.getPrimaryKeyFields()[0].getObjectIdFieldTypeCode());
    }
    this.keySerializer = MappingUtils.getSerializer(classMetaData
        .getPrimaryKeyFields()[0]);

    columnMetas = new HashMap<String, ColumnMeta<?>>();

    indexMetaData = new HashSet<AbstractEntityIndex>();

    FieldMetaData[] fmds = classMetaData.getFields();

    for (int i = fields.nextSetBit(0); i >= 0; i = fields.nextSetBit(i + 1)) {

      // not in the bit set to use, isn't managed or saved, or is a primary key,
      // ignore
      if (fmds[i].getManagement() == FieldMetaData.MANAGE_NONE
          || fmds[i].isPrimaryKey()) {
        continue;
      }

      if (fmds[i].getAssociationType() == FieldMetaData.ONE_TO_MANY
          || fmds[i].getAssociationType() == FieldMetaData.MANY_TO_MANY) {

        indexMetaData.add(new ManyEntityIndex(fmds[i], mappingUtils));

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

      // TODO if fmds[i].getAssociationType() > 0 .. we found an attached entity
      // and need to find it's entityFacade
      columnMetas.put(fmds[i].getName(), new ColumnMeta(fmds[i].getIndex(),
          MappingUtils.getSerializer(fmds[i].getTypeCode())));
    }

    // always add the default "empty val" for read and write. This way if the pk
    // is the only field requested, we'll always get a result from cassandra
    columnMetas.put(EMPTY_COL, new StaticColumnMeta());
  }

  public String[] getColumnNames() {
    return columnMetas.keySet().toArray(new String[] {});
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

  public int getFieldId(String columnName) {
    return columnMetas.get(columnName).getFieldId();
  }

  public Serializer<?> getSerializer(String columnName) {
    return columnMetas.get(columnName).getSerializer();
  }

  public Map<String, ColumnMeta<?>> getColumnMeta() {
    return columnMetas;
  }

  /**
   * @return the indexMetaData
   */
  public Set<AbstractEntityIndex> getIndexMetaData() {
    return indexMetaData;
  }

  @Override
  public String toString() {
    return String.format(
        "EntityFacade[class: %s, columnFamily: %s, columnNames: %s]",
        clazz.getName(), columnFamilyName, Arrays.toString(getColumnNames()));

  }

}
