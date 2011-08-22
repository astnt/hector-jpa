/**
 * 
 */
package com.datastax.hectorjpa.index.field;

import static com.datastax.hectorjpa.serializer.CompositeUtils.getCassType;

import java.math.BigDecimal;

import org.apache.openjpa.meta.Order;

import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

/**
 * Base class to perform serialization strategy for indexing operations.
 * This should encapsulate all logic from runtime type to dynamic composite type when writing and querying.
 * 
 * 
 * @author Todd Nine
 *
 */
public class BigDecimalFieldSerializerStrategy implements IndexSerializationStrategy{

  private static final BigIntegerSerializer SER = BigIntegerSerializer.get();
  private String cassandraType;
  
 
  
  public BigDecimalFieldSerializerStrategy(boolean ascending){
    cassandraType = getCassType(SER, ascending);
  }
  
  /**
   * Add the value to the component.
   * @param source
   * @return
   */
  public void addToComponent(DynamicComposite composite, Object value){
    if(value == null){
      composite.addComponent(null, SER, cassandraType);
      return;
    }
    
    composite.addComponent(((BigDecimal)value).unscaledValue(), SER, cassandraType);
    
  }

  @Override
  public void addToComponent(DynamicComposite composite, int index,
      Object value, ComponentEquality equality) {
    
    if(value == null){
      composite.addComponent(index, null, SER, cassandraType, equality);
      return;
    }
    
    
    composite.addComponent(index, ((BigDecimal)value).unscaledValue(), SER, cassandraType, equality);
    
  }
  

  @Override
  public Object get(DynamicComposite composite, int index) {
    return composite.get(index, SER);
  }

 



}
