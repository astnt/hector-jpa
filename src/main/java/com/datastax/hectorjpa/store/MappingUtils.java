package com.datastax.hectorjpa.store;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.cassandra.model.thrift.ThriftSliceQuery;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.ShortSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.JavaTypes;
import org.apache.openjpa.util.MetaDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.meta.key.CompositeKeyStrategy;
import com.datastax.hectorjpa.meta.key.KeyStrategy;
import com.datastax.hectorjpa.meta.key.SingleKeyStrategy;
import com.datastax.hectorjpa.serializer.CharSerializer;
import com.datastax.hectorjpa.serializer.FloatSerializer;
import com.datastax.hectorjpa.serializer.TimeUUIDSerializer;

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

 
  static {
    typeSerializerMap.put(JavaTypes.STRING, StringSerializer.get());
    
    typeSerializerMap.put(JavaTypes.BOOLEAN, BooleanSerializer.get());
    typeSerializerMap.put(JavaTypes.BOOLEAN_OBJ, BooleanSerializer.get());
    
    
    typeSerializerMap.put(JavaTypes.CHAR, CharSerializer.get());
    typeSerializerMap.put(JavaTypes.CHAR_OBJ, CharSerializer.get());
    
    typeSerializerMap.put(JavaTypes.SHORT, ShortSerializer.get());
    typeSerializerMap.put(JavaTypes.SHORT_OBJ, ShortSerializer.get());
    
    typeSerializerMap.put(JavaTypes.INT, IntegerSerializer.get());
    typeSerializerMap.put(JavaTypes.INT_OBJ, IntegerSerializer.get());
    
    
    typeSerializerMap.put(JavaTypes.FLOAT, FloatSerializer.get());
    typeSerializerMap.put(JavaTypes.FLOAT_OBJ, FloatSerializer.get());
    
   
    typeSerializerMap.put(JavaTypes.DATE, DateSerializer.get());
    
    typeSerializerMap.put(JavaTypes.LONG, LongSerializer.get());
    typeSerializerMap.put(JavaTypes.LONG_OBJ, LongSerializer.get());
    
    typeSerializerMap.put(JavaTypes.DOUBLE, DoubleSerializer.get());
    typeSerializerMap.put(JavaTypes.DOUBLE_OBJ, DoubleSerializer.get());

    classSerializerMap.put(UUID.class, UUIDSerializer.get());
    classSerializerMap.put(byte[].class, BytesArraySerializer.get());
    classSerializerMap.put(ByteBuffer.class, ByteBufferSerializer.get());
    classSerializerMap.put(com.eaio.uuid.UUID.class, TimeUUIDSerializer.get());

  }

  public static Serializer<?> getSerializer(int javaType) {
    return typeSerializerMap.get(javaType) != null ? typeSerializerMap
        .get(javaType) : ObjectSerializer.get();
  }

  public static Serializer<Object> getSerializer(FieldMetaData fieldMetaData) {
    Serializer serializer = getSerializer(fieldMetaData.getTypeCode());
    if (serializer instanceof ObjectSerializer) {
      Class<?> clazz = fieldMetaData.getType();
      if (classSerializerMap.get(clazz) != null) {
        serializer = classSerializerMap.get(clazz);
      }
    }
    return serializer;
  }
  

  public static SliceQuery<byte[], String, byte[]> buildSliceQuery(byte[] key,
      List<String> columns, String cfName, Keyspace keyspace) {
    SliceQuery<byte[], String, byte[]> query = new ThriftSliceQuery(keyspace,
        BytesArraySerializer.get(), StringSerializer.get(),
        BytesArraySerializer.get());

    String[] colArray = new String[columns.size()];

    columns.toArray(colArray);

    query.setColumnNames(colArray);
    query.setKey(key);
    query.setColumnFamily(cfName);
    return query;
  }

  /**
   * Get the column family for this class
   * 
   * @param clazz
   * @return
   */
  public static String getColumnFamily(CassandraClassMetaData metaData) {
    
    String name = metaData.getColumnFamily();
    
    if(name != null){
      return name;
    }
    
    CassandraClassMetaData parent = (CassandraClassMetaData) metaData.getPCSuperclassMetaData();
    
    if(parent != null){
      return getColumnFamily(parent);
    }
    
    throw new MetaDataException(String.format("You have not defined a column family for the entity %s", metaData.getDescribedType()));
    
  }

//  /**
//   * Get the byte[] representing this Id object
//   * 
//   * @param idObj
//   * @return
//   */
//  public static byte[] getKeyBytes(Object idObj) {
//    Object target = getTargetObject(idObj);
//
//    Serializer serializer = getSerializer(target);
//
//    return serializer.toBytes(target);
//  }
  
  public static KeyStrategy getKeyStrategy(CassandraClassMetaData metaData){
    if(metaData.getPrimaryKeyFields().length > 1){
      return new CompositeKeyStrategy(metaData);
    }
    
    return new SingleKeyStrategy(metaData);
  }

  /**
   * Retrieve the {@link Serializer} implementation for the id Object.
   * 
   * @see {@link SerializerTypeInferer} for specifics.
   * @param idObj
   */
  public static Serializer getSerializer(Object target) {

//    Object target = getTargetObject(idObj);

    Serializer serializer = classSerializerMap.get(target.getClass());

    if (serializer != null) {
      return serializer;
    }

    return SerializerTypeInferer.getSerializer(target);
  }

//  /**
//   * If the object is an OpenJPAId, it will return the underlying identity
//   * object, if not, the passed value is returned
//   * 
//   * @param idObj
//   * @return
//   */
//  public static Object getTargetObject(Object idObj) {
//    if (idObj instanceof OpenJPAId) {
//      return ((OpenJPAId) idObj).getIdObject();
//    }
//
//    return idObj;
//  }

}
