package com.datastax.hectorjpa.meta;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;

/**
 * Inner class to encapsulate order field logic and meta data
 * 
 * @author Todd Nine
 * 
 */
public class QueryIndexField extends AbstractIndexField {

  public QueryIndexField(FieldMetaData fmd) {
    super(fmd, fmd.getName());
  }

  @Override
  protected ClassMetaData getContainerClassMetaData(FieldMetaData fmd) {
    return fmd.getDefiningMetaData();
  }

  /**
   * Add the object to the composite at the specified index using the serializer
   * for this value
   * 
   * @param composite
   * @param index
   * @param value
   */
  public void addToComposite(DynamicComposite composite, int index, int length, Object value) {

    ensureCapacity(composite, index, length);
    composite.add(index, value);
  }

  private void ensureCapacity(DynamicComposite composite, int index, int length) {

    List<Serializer<?>> serializers = composite.getSerializersByPosition();

    if (serializers == null) {
      serializers = new ArrayList<Serializer<?>>(length);
      
      for(int i = 0; i < length; i ++){
        serializers.add(null);
      }
      
    }
    
    serializers.set(index, serializer);

    composite.setSerializersByPosition(serializers);

  }

}