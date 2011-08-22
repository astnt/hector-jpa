/**
 * 
 */
package com.datastax.hectorjpa.index.field;

import static com.datastax.hectorjpa.serializer.CompositeUtils.getCassType;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.meta.FieldMetaData;

import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Base class to perform serialization strategy for indexing operations. This
 * should encapsulate all logic from runtime type to dynamic composite type when
 * writing and querying.
 * 
 * 
 * @author Todd Nine
 * 
 */
public class FieldSerializerStrategy implements IndexSerializationStrategy {

  protected Serializer<Object> serializer;
  // protected FieldMetaData targetField;
  protected String compositeComparator;
  
  
  public FieldSerializerStrategy(FieldMetaData field, boolean ascending) {
    this.serializer = MappingUtils.getSerializer(field);
    this.compositeComparator = getCassType(serializer, ascending);
  }

  /**
   * Add the value to the component.
   * 
   * @param source
   * @return
   */
  public void addToComponent(DynamicComposite composite, Object value) {
    composite.addComponent(value, serializer, compositeComparator);
  }

  @Override
  public void addToComponent(DynamicComposite composite, int index,
      Object value, ComponentEquality equality) {

    composite.addComponent(index, value, serializer, compositeComparator,
        equality);

  }
  
  @Override
  public Object get(DynamicComposite composite, int index) {
   return composite.get(index, serializer);
  }
  

}
