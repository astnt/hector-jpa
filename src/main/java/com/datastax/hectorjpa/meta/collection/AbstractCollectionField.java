package com.datastax.hectorjpa.meta.collection;

import java.nio.ByteBuffer;
import java.util.Collection;

import me.prettyprint.cassandra.model.thrift.ThriftSliceQuery;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.DynamicCompositeSerialzier;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.MetaDataException;

import com.datastax.hectorjpa.meta.Field;
import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Base class for all collection operations
 * 
 * @author Todd Nine
 * 
 */
public abstract class AbstractCollectionField<V> extends Field<V> {

  public static final String CF_NAME = "Collection_Container";

  protected static byte[] HOLDER = new byte[] { 0 };

  // the default batch size when it hasn't been set into the context
  protected int DEFAULT_FETCH_SIZE = 100;

  protected static final DynamicCompositeSerialzier compositeSerializer = new DynamicCompositeSerialzier();

  protected String name;
  protected Class<?> targetClass;
  protected MappingUtils mappingUtils;

  // The name of this entity serialzied as bytes
  protected byte[] entityName;

  // the name of the field serialzied as bytes
  protected byte[] fieldName;
  
 
  

  public AbstractCollectionField(FieldMetaData fmd, MappingUtils mappingUtils) {
    super(fmd.getIndex());

    this.mappingUtils = mappingUtils;

    Class<?> clazz = fmd.getDeclaredType();

    if (!Collection.class.isAssignableFrom(clazz)) {
      throw new MetaDataException("Only collections are currently supported");
    }

    this.name = fmd.getName();

    ClassMetaData elementClassMeta = fmd.getElement().getDeclaredTypeMetaData();

    // set the class of the collection elements
    targetClass = elementClassMeta.getDescribedType();

    // create our cached bytes for better performance
    fieldName = StringSerializer.get().toBytes(name);

    // write our column family name of the owning side to our rowkey for
    // scanning
    String columnFamilyName = mappingUtils.getColumnFamily(fmd
        .getDeclaringType());

    entityName = StringSerializer.get().toBytes(columnFamilyName);
  }
  
  /**
   * Create our key byte array
   * 
   * @param entityIdBytes
   * @return
   */
  protected byte[] constructKey(byte[] entityIdBytes, byte[] marker) {

    byte[] key = new byte[entityName.length + fieldName.length
        + entityIdBytes.length + marker.length];

    ByteBuffer buff = ByteBuffer.wrap(key);

    buff.put(entityName);
    buff.put(entityIdBytes);
    buff.put(fieldName);
    buff.put(marker);

    return key;

  }
 
  /**
   * Create the slice query for this field
   * @param objectId
   * @param keyspace
   * @param columnFamilyName
   * @param count
   * @return
   */
  public SliceQuery<byte[], DynamicComposite, byte[]> createQuery(Object objectId,
      Keyspace keyspace, String columnFamilyName, int count) {
    
    //undefined value set it to something realistic
    if(count < 0){
      count = DEFAULT_FETCH_SIZE;
    }
    
    SliceQuery<byte[], DynamicComposite, byte[]> query = new ThriftSliceQuery(
        keyspace, BytesArraySerializer.get(), compositeSerializer,
        BytesArraySerializer.get());

    query.setRange(null, null, false, count);
    query.setKey(constructKey(mappingUtils.getKeyBytes(objectId), getDefaultSearchmarker()));
    query.setColumnFamily(columnFamilyName);
    return query;

  }
  
  /**
   * Read this field from the data store from the queryresult
   * @param stateManager
   * @param result
   */
  public abstract void readField(OpenJPAStateManager stateManager,   QueryResult<ColumnSlice<DynamicComposite, byte[]>> result);
  
  
  /**
   * Return the default end bytes on the rowkey for searching an index
   * @return
   */
  protected abstract byte[] getDefaultSearchmarker();
 

  
}
