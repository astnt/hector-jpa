/**
 * 
 */
package com.datastax.hectorjpa.meta.key;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.enhance.Reflection;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.MetaDataException;
import org.apache.openjpa.util.OpenJPAId;
import org.apache.openjpa.util.StoreException;

import com.datastax.hectorjpa.store.CassandraClassMetaData;
import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Used to store composite keys.
 * 
 * @author Todd Nine
 * 
 */
public class CompositeKeyStrategy implements KeyStrategy {

  protected static final DynamicCompositeSerializer dynamicSerializer = new DynamicCompositeSerializer();

  protected static final StringSerializer stringSerializer = StringSerializer
      .get();

  protected static final IntegerSerializer intSerializer = IntegerSerializer
      .get();

  private final String[] fieldNames;
  
  @SuppressWarnings("rawtypes")
  private final Serializer[] serializers;

  /**
   * Mapping from field name to serializer
   */
  protected Map<String, Serializer<?>> serializerMapping = new HashMap<String, Serializer<?>>();

  /**
   * Mapping for string to field
   */
  protected Map<String, Integer> fieldNumMapping = new HashMap<String, Integer>();

  private final Class<?> keyClass;

  public CompositeKeyStrategy(CassandraClassMetaData classMetaData) {
    FieldMetaData[] fields = classMetaData.getPrimaryKeyFields();

    if (fields.length == 1) {
      throw new MetaDataException(
          "Composite key strategy cannot have only  1 primary key field");
    }

    fieldNames = new String[fields.length];
    serializers = new Serializer[fields.length];
   
    for (int i = 0; i < fields.length; i++) {
      fieldNames[i] = fields[i].getName();
      serializers[i] = MappingUtils.getSerializer(fields[i]);

      serializerMapping.put(fieldNames[i], serializers[i]);
    }

    keyClass = classMetaData.getObjectIdType();
  }


  @Override
  public ByteBuffer toByteBuffer(Object oid) {
    return dynamicSerializer.toByteBuffer(createComposite(((OpenJPAId)oid).getIdObject()));
  }
  

  @Override
  public byte[] toByteArray(Object oid) {
    return dynamicSerializer.toBytes(createComposite(((OpenJPAId)oid).getIdObject()));
  }

  


  /**
   * Uses reflection to directly access the fields that are the composite
   * @param obj
   * @return
   */
  @SuppressWarnings("unchecked")
  private DynamicComposite createComposite(Object obj){
    DynamicComposite c = new DynamicComposite();

    // write the number of fields for this embedded object
    c.addComponent(fieldNames.length, intSerializer);

    for (int i = 0; i < fieldNames.length; i++) {
      // write the key
      c.addComponent(fieldNames[i], stringSerializer);

      Field field = Reflection.findField(keyClass, fieldNames[i], true);

      Object value = Reflection.get(obj, field);
      
      // write the value
      c.addComponent(value, serializers[i]);
    }
    
    return c;
  }
  

  @Override
  public Object getInstance(ByteBuffer buffer) {
    return readDynamicComposite(dynamicSerializer.fromByteBuffer(buffer));
  }

  @Override
  public Object getInstance(byte[] bytes) {
    return readDynamicComposite(dynamicSerializer.fromBytes(bytes));
  }
  
  /**
   * Read the dynamic composite and return the object
   * @param c
   * @return
   */
  private Object readDynamicComposite(DynamicComposite c){
 // now have the number of fields
    int length = c.get(0, intSerializer);

    Object id;
    try {
      id = keyClass.newInstance();
    } catch (Exception e) {
      throw new StoreException(
          String.format(
              "Unable to instanciate class of type %s.  Please make sure is has a no argument constructor and is publicly accessable.",
              keyClass), e);
    }

    // every field is in a pair, so increment read by 2
    for (int i = 1; i <= length * 2; i += 2) {
      String name = c.get(i, stringSerializer);

      Object fieldValue = c.get(i + 1, serializerMapping.get(name));

      Field field = Reflection.findField(keyClass, name, true);

      Reflection.set(id, fieldValue, field);

    }

    return id;
  }

}
