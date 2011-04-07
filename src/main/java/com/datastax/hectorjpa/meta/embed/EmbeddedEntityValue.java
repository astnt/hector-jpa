package com.datastax.hectorjpa.meta.embed;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;

import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Class for serializing Embeddable objects into a single value byte stream.  Values are written in the following format
 * 
 * <int total field count>+(<field name>+<field value>)*
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class EmbeddedEntityValue {

  protected static final DynamicCompositeSerializer dynamicSerializer = new DynamicCompositeSerializer();
  
  protected static final StringSerializer stringSerializer = StringSerializer.get();
  
  protected static final IntegerSerializer intSerializer = IntegerSerializer.get();

  protected int[] fieldIds;

  /**
   * The field names for the embedded object
   */
  protected String[] fieldNames;

  /**
   * The serializers for each field
   */
  @SuppressWarnings("rawtypes")
  protected Serializer[] serializers;
  
  
  /**
   * Mapping from field name to serializer
   */
  protected Map<String, Serializer<?>> serializerMapping = new HashMap<String, Serializer<?>>();
  
  /**
   * Mapping for string to field
   */
  protected Map<String, Integer> fieldNumMapping = new HashMap<String, Integer>();

  public EmbeddedEntityValue(ClassMetaData entity) {

    FieldMetaData[] fields = entity.getFields();
    fieldNames = new String[fields.length];
    serializers = new Serializer<?>[fields.length];
    fieldIds = new int[fields.length];

    // only supports @Persistent annotations at the moment
    for (int i = 0; i < fields.length; i++) {
      fieldIds[i] = fields[i].getIndex();
      fieldNames[i] = fields[i].getName();
      serializers[i] = MappingUtils.getSerializer(fields[i]);
      
      serializerMapping.put(fieldNames[i], serializers[i]);
      fieldNumMapping.put(fieldNames[i], fieldIds[i]);
    }

  }

  /**
   * Write this field to the dynamic composite.
   * 
   * @param c
   */
  @SuppressWarnings("unchecked")
  public void writeToComposite(OpenJPAStateManager sm, DynamicComposite c) {
    
    //write the number of fields for this embedded object
    c.addComponent(fieldIds.length, intSerializer);
    
    for(int i = 0; i < fieldIds.length; i ++){
      //write the key
      c.addComponent(fieldNames[i], stringSerializer);
      
      Object value = sm.fetch(fieldIds[i]);
      
      //write the value
      c.addComponent(value, serializers[i]);
    }
    
  }

  /**
   * Read the next object from this dynamic composite
   * 
   * @param sm The state manager of the embedded object
   * @param c The dynamic composite containing the values
   * @return The next index in the composite that should be read
   */
  public int getFromComposite(OpenJPAStateManager sm, DynamicComposite c, int startIndex) {
    
    //now have the number of fields
    int length = c.get(startIndex, intSerializer);
    
    int offset = startIndex+1;
    
    //every field is in a pair, so increment read by 2
    for(int i = 0; i <  length*2; i+=2){
      String name = c.get(i+offset, stringSerializer);
      
      Object fieldValue = c.get(i+offset+1, serializerMapping.get(name));
      
      sm.store(fieldNumMapping.get(name), fieldValue);
      
    }
    
    return offset+length*2;   
    
  }
  
  
  
  
}
