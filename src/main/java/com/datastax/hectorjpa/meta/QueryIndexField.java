package com.datastax.hectorjpa.meta;

import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.apache.cassandra.db.marshal.BytesType;
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
  public void addToComposite(DynamicComposite composite, int index, Object value, boolean inclusive) {
    //TODO TN, this feels a bit hacky.  Should the abstract composite default to Bytes if the serializer is null
    String type = composite.getSerializerToComparatorMapping().get(serializer.getClass());
    
    if(type == null){
      type = ComparatorType.BYTESTYPE.getTypeName();
    }
    
    composite.addComponent(index, value, serializer, type, inclusive);
  }


}