/**
 * 
 */
package com.datastax.hectorjpa.serializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.ddl.ComparatorType;

/**
 * A factory utility for generating composites that support the additional serializers
 * and types this plugin uses
 * @author Todd Nine
 *
 */
public class CompositeUtils {

  private static Map<Class<? extends Serializer>, String>  mapping = new HashMap<Class<? extends Serializer>, String>();
  
  
  static{
    DynamicComposite dynamic = new DynamicComposite();
    
    mapping.putAll(dynamic.getSerializerToComparatorMapping()); 
    
    mapping.put(TimeUUIDSerializer.class, ComparatorType.TIMEUUIDTYPE.getTypeName());
    
  }
  
  /**
   * Allocate a dynamic composite with the given length and sets all serializers at the posistions
   * to null
   * 
   * @param length
   * @return
   */
  public static DynamicComposite newComposite(int length){
    
    DynamicComposite composite = newComposite();

    List<Serializer<?>> serializers = new ArrayList<Serializer<?>>(length);

    for (int i = 0; i < length; i++) {
      serializers.add(null);
    }
    
    composite.setSerializersByPosition(serializers);
    
    return composite;
    
  }
  

  /**
   * Allocate a dynamic composite and intialiaze all the serializer methods
   * 
   * @param length
   * @return
   */
  public static DynamicComposite newComposite(){
    
    DynamicComposite composite = new DynamicComposite();
    composite.setSerializerToComparatorMapping(mapping);
        
    return composite;
    
  }
  
  
  /**
   * Set the given value and serializer at the supplied index  Assumes that this
   * composite was allocated with the newComposite factory method
   * @param index
   * @param value
   * @param serializer
   */
  public static void set(DynamicComposite composite, int index, Object value, Serializer<?> serializer){
    composite.getSerializersByPosition().set(index, serializer);
    composite.set(index, value);    
  }
}
