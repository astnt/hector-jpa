/**
 * 
 */
package com.datastax.hectorjpa.serializer;

import java.util.HashMap;
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
  
}
