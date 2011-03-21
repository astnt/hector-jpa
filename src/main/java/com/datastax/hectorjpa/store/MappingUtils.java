package com.datastax.hectorjpa.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.Table;

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
    return typeSerializerMap.get(javaType) != null ? typeSerializerMap
        .get(javaType) : ObjectSerializer.get();
  }

  public static Serializer<?> getSerializer(FieldMetaData fieldMetaData) {
    Serializer serializer = getSerializer(fieldMetaData.getTypeCode());
    if (serializer instanceof ObjectSerializer) {
      Class<?> clazz = fieldMetaData.getType();
      if (classSerializerMap.get(clazz) != null) {
        serializer = classSerializerMap.get(clazz);
      }
    }
    return serializer;
  }

  public SliceQuery<byte[], String, byte[]> buildSliceQuery(Object idObj,
      String[] columns, String cfName, Keyspace keyspace) {
    SliceQuery<byte[], String, byte[]> query = new ThriftSliceQuery(keyspace,
        BytesArraySerializer.get(), StringSerializer.get(),
        BytesArraySerializer.get());

    query.setColumnNames(columns);
    query.setKey(getKeyBytes(idObj));
    query.setColumnFamily(cfName);
    return query;
  }

  /**
   * Get the column family for this class
   * 
   * @param clazz
   * @return
   */
  public String getColumnFamily(Class<?> clazz) {
    return clazz.getAnnotation(Table.class) != null ? clazz.getAnnotation(
        Table.class).name() : clazz.getSimpleName();
  }

  /**
   * Get the byte[] representing this Id object
   * 
   * @param idObj
   * @return
   */
  public byte[] getKeyBytes(Object idObj) {
    Object target = getTargetObject(idObj);

    Serializer serializer = getSerializer(target);

    return serializer.toBytes(target);
  }

  /**
   * Retrieve the {@link Serializer} implementation for the id Object.
   * 
   * @see {@link SerializerTypeInferer} for specifics.
   * @param idObj
   */
  public Serializer getSerializer(Object idObj) {
    return SerializerTypeInferer.getSerializer(getTargetObject(idObj));
  }

  /**
   * If the object is an OpenJPAId, it will return the underlying identity
   * object, if not, the passed value is returned
   * 
   * @param idObj
   * @return
   */
  public Object getTargetObject(Object idObj) {
    if (idObj instanceof OpenJPAId) {
      return ((OpenJPAId) idObj).getIdObject();
    }

    return idObj;
  }

}
