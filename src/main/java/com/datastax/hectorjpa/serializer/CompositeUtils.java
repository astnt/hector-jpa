/**
 * 
 */
package com.datastax.hectorjpa.serializer;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.UUIDSerializer;
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

    mapping.put(UUIDSerializer.class,  ComparatorType.LEXICALUUIDTYPE.getTypeName());
    
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
   * Get a cass type for the given serializer
   * @param instance
   * @return
   */
  public static String getCassType(Serializer<?> instance){
    String type= mapping.get(instance.getClass());
    
    if(type != null){
      return type;
    }
    
   return ComparatorType.BYTESTYPE.getTypeName();
  }
  
  /**
   * Get the Cassandra component serializer, optionally adding the reversed parameter (reverse=true) if ascending == false
   * @param instance
   * @param reversed
   * @return
   */
  public static String getCassType(Serializer<?> instance, boolean ascending){
	  
	  if(ascending){
		  return getCassType(instance);
	  }
	  
	  StringBuffer buffer = new StringBuffer();
	  buffer.append(getCassType(instance));
	  buffer.append("(reversed=true)");
	  return buffer.toString();
	  
	  
	  
  }
  
}
