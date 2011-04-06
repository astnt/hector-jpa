package com.datastax.hectorjpa.meta.collection;

import java.nio.ByteBuffer;
import java.util.Collection;

import me.prettyprint.cassandra.model.thrift.ThriftSliceQuery;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.ChangeTracker;
import org.apache.openjpa.util.MetaDataException;
import org.apache.openjpa.util.Proxy;

import com.datastax.hectorjpa.meta.Field;
import com.datastax.hectorjpa.service.IndexQueue;
import com.datastax.hectorjpa.store.CassandraClassMetaData;
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

  protected static final DynamicCompositeSerializer compositeSerializer = new DynamicCompositeSerializer();

  protected Serializer<Object> idSerializer;
  protected Class<?> targetClass;

  // The name of this entity serialzied as bytes
  protected byte[] entityName;

  // the name of the field serialzied as bytes
  protected byte[] fieldName;
  

  protected int compositeFieldLength = 0;
  
 
  

  public AbstractCollectionField(FieldMetaData fmd) {
    super(fmd.getIndex(), fmd.getName());

    Class<?> clazz = fmd.getDeclaredType();

    if (!Collection.class.isAssignableFrom(clazz)) {
      throw new MetaDataException("Only collections are currently supported");
    }

    this.name = fmd.getName();

    CassandraClassMetaData elementClassMeta = (CassandraClassMetaData) fmd.getElement().getDeclaredTypeMetaData();
    
    if(elementClassMeta == null){
      throw new MetaDataException(String.format("You defined type %s in a collection, but it is not a persistable entity", fmd.getElement().getDeclaredType()));
    }
    
    this.idSerializer = MappingUtils.getSerializerForPk(elementClassMeta);

    // set the class of the collection elements
    targetClass = elementClassMeta.getDescribedType();

    // create our cached bytes for better performance
    fieldName = StringSerializer.get().toBytes(name);

    // write our column family name of the owning side to our rowkey for
    // scanning
    String columnFamilyName = MappingUtils.getColumnFamily(elementClassMeta);

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
      Keyspace keyspace, int count) {
    
    //undefined value set it to something realistic
    if(count < 0){
      count = DEFAULT_FETCH_SIZE;
    }
    
    byte[] rowKey = constructKey(MappingUtils.getKeyBytes(objectId), getDefaultSearchmarker());
    
    SliceQuery<byte[], DynamicComposite, byte[]> query = new ThriftSliceQuery(
        keyspace, BytesArraySerializer.get(), compositeSerializer,
        BytesArraySerializer.get());

    query.setRange(null, null, false, count);
    query.setKey(rowKey);
    query.setColumnFamily(CF_NAME);
    return query;

  }
  

  /**
   * Useful for proxy fields.  The index 0 is the original value the index 1 is the new value.
   * If no proxy is present only index 1 will contain a value
   * 
   * @param value
   * @return
   */
  protected Object[] getDelta(Object value){
    Object[] values = new Object[2];
    
    if(!(value instanceof Proxy)){
      values[1] = value;
      return values;
    }
    
    ChangeTracker tracker = ((Proxy) value).getChangeTracker();
    
    Collection<?> objects = tracker.getRemoved();
    
    for(Object removed: objects){
      values[0] = removed;
      break;
    }

    objects = tracker.getAdded();
    
    for(Object added: objects){
      values[1] = added;
      break;
    }
    
    
    return values;
  
  }
  /**
   * Read this field from the data store from the queryresult
   * @param stateManager
   * @param result
   * @return true if this field had values
   */
  public abstract boolean readField(OpenJPAStateManager stateManager,   QueryResult<ColumnSlice<DynamicComposite, byte[]>> result);
  
  
  /**
   * Remove this collection by removing the row key represending it
   * @param stateManager 
   * @param mutator
   * @param long clock
   * @param key The key of the stateManager's entity
   */
  public abstract void removeCollection(OpenJPAStateManager stateManager, Mutator<byte[]> mutator, long clock, byte[] key);
  
  /**
   * Return the default end bytes on the rowkey for searching an index
   * @return
   */
  protected abstract byte[] getDefaultSearchmarker();
  
  

  @Override
  public String toString() {  
    return String.format("AbstractCollectionField(fieldId: %d, name: %s)", fieldId, name);
  }
 
  
  
  
  
}
