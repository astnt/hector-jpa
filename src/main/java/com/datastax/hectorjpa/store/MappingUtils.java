package com.datastax.hectorjpa.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.cassandra.model.MutatorImpl;
import me.prettyprint.cassandra.model.thrift.ThriftSliceQuery;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.JavaTypes;
import org.apache.openjpa.util.LongId;
import org.apache.openjpa.util.OpenJPAId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for bridging Hector/OpenJPA functionality 
 *
 * @author zznate
 *
 */
public class MappingUtils {
  private static final Logger log = LoggerFactory.getLogger(MappingUtils.class);
  
  private static final Map<Integer, Serializer<?>> typeSerializerMap = new HashMap<Integer, Serializer<?>>();
  private static final Map<Class<?>, Serializer<?>> classSerializerMap = new HashMap<Class<?>, Serializer<?>>();
  
  // TODO need to figure out UUID
  static {
    typeSerializerMap.put(JavaTypes.STRING, StringSerializer.get());
    typeSerializerMap.put(JavaTypes.INT, IntegerSerializer.get());
    typeSerializerMap.put(JavaTypes.INT_OBJ, IntegerSerializer.get());
    typeSerializerMap.put(JavaTypes.DATE, DateSerializer.get());
    typeSerializerMap.put(JavaTypes.LONG, LongSerializer.get());
    typeSerializerMap.put(JavaTypes.LONG_OBJ, LongSerializer.get());
    typeSerializerMap.put(JavaTypes.DOUBLE, DoubleSerializer.get());
    typeSerializerMap.put(JavaTypes.DOUBLE_OBJ, DoubleSerializer.get());
    
    classSerializerMap.put(UUID.class, UUIDSerializer.get());
    classSerializerMap.put(byte[].class, BytesArraySerializer.get());
    classSerializerMap.put(ByteBuffer.class, ByteBufferSerializer.get());
    
  }
  
  public static Serializer<?> getSerializer(int javaType) {
    return typeSerializerMap.get(javaType) != null ? typeSerializerMap.get(javaType) : ObjectSerializer.get(); 
  }
  
  public static Serializer<?> getSerializer(FieldMetaData fieldMetaData) {
    Serializer serializer = getSerializer(fieldMetaData.getTypeCode());
    if ( serializer instanceof ObjectSerializer ) {
      Class<?> clazz = fieldMetaData.getType();
      if ( classSerializerMap.get(clazz) != null ) {
        serializer = classSerializerMap.get(clazz);
      }
    }
    return serializer;
  }
  
  public SliceQuery<byte[], String, byte[]> buildSliceQuery(Object idObj, EntityFacade entityFacade, Keyspace keyspace) {
    SliceQuery<byte[], String, byte[]> query = new ThriftSliceQuery(keyspace, BytesArraySerializer.get(), StringSerializer.get(), BytesArraySerializer.get());
    query.setColumnNames(entityFacade.getColumnNames());
    query.setKey(getKeyBytes(idObj));
    query.setColumnFamily(entityFacade.getColumnFamilyName());
    return query;
  }
  
  /**
   * Get the byte[] representing this Id object
   * @param idObj
   * @return
   */
  public byte[] getKeyBytes(Object idObj) {
    Serializer serializer = getSerializer(idObj);
    if ( idObj instanceof OpenJPAId )
      return serializer.toBytes(((OpenJPAId)idObj).getIdObject());
    return serializer.toBytes(idObj);
  }
  
  /**
   * Retrieve the {@link Serializer} implementation for the id Object. 
   * @see {@link SerializerTypeInferer} for specifics.
   * @param idObj
   */
  public Serializer getSerializer(Object idObj) {
    Serializer serializer;
    if ( idObj instanceof OpenJPAId ) {
      serializer = SerializerTypeInferer.getSerializer(((OpenJPAId)idObj).getIdObject());
    } else {
      serializer = SerializerTypeInferer.getSerializer(idObj);
    }    
    return serializer;
  }
  
  /**
   * Return a list of field names for which there are a 1-1 mapping 
   * to Column names
   * 
   * @param metaData
   * @return
   */
  List<String> buildColumnList(ClassMetaData metaData) {    
    FieldMetaData[] fmds = metaData.getFields();
    List<String> cols = new ArrayList<String>(fmds.length);
    for (int i = 0; i < fmds.length; i++) {
      if (fmds[i].getManagement() != fmds[i].MANAGE_PERSISTENT || fmds[i].isPrimaryKey())
        continue;

      log.debug("fmd.name: {}",fmds[i].getName());
      cols.add(fmds[i].getName());
    }
    return cols;
  }
  
  /**
   * Map the column names to their respective serializers
   * TODO cache this
   * @param metaData
   * @return
   */
  Map<String, Serializer> buildColumnSerializerMap(ClassMetaData metaData) {
    Map<String, Serializer> serMap = new HashMap<String, Serializer>();
    
    FieldMetaData[] fmds = metaData.getFields();
    for (int i = 0; i < fmds.length; i++) {
      if (fmds[i].getManagement() != fmds[i].MANAGE_PERSISTENT || fmds[i].isPrimaryKey())
        continue;
      switch (fmds[i].getTypeCode()) {
      case JavaTypes.STRING:
        serMap.put(fmds[i].getName(), StringSerializer.get());
        break;
      case JavaTypes.INT:
        serMap.put(fmds[i].getName(), IntegerSerializer.get());
        break;
      case JavaTypes.INT_OBJ:
        serMap.put(fmds[i].getName(), IntegerSerializer.get());
        break;
      case JavaTypes.DATE:
        serMap.put(fmds[i].getName(), DateSerializer.get());
        break;
      case JavaTypes.LONG:
        serMap.put(fmds[i].getName(), LongSerializer.get());
        break;
      case JavaTypes.LONG_OBJ:
        serMap.put(fmds[i].getName(), LongSerializer.get());
        break;
      case JavaTypes.DOUBLE:
        serMap.put(fmds[i].getName(), DoubleSerializer.get());
        break;
      case JavaTypes.DOUBLE_OBJ:
        serMap.put(fmds[i].getName(), DoubleSerializer.get());
        break;        
      default:
        serMap.put(fmds[i].getName(), ObjectSerializer.get());
        break;
      }
    }    
    return serMap;
  }
  
  

  public Mutator addMutation(Mutator mutator, Object idObj, OpenJPAStateManager stateManager, Keyspace keyspace) {
    ClassMetaData metaData = stateManager.getMetaData();       
    Map<String,Serializer> serMap = buildColumnSerializerMap(metaData);
    for (Map.Entry<String, Serializer> entry : serMap.entrySet()) {
      mutator.addInsertion(getKeyBytes(idObj), "TestBeanColumnFamily", 
          HFactory.createColumn(entry.getKey(), stateManager.fetch(metaData.getField(entry.getKey()).getIndex()), 
              StringSerializer.get(), entry.getValue()));
    }
    return mutator;
  }
}
