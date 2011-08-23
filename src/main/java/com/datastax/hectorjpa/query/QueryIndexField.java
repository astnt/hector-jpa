package com.datastax.hectorjpa.query;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.index.AbstractIndexField;
import com.datastax.hectorjpa.index.field.IndexSerializationStrategy;
import com.datastax.hectorjpa.index.field.IndexSerializationStrategyFactory;

/**
 * Inner class to encapsulate order field logic and meta data
 * 
 * @author Todd Nine
 * 
 */
public class QueryIndexField extends AbstractIndexField {

  private static final Logger log = LoggerFactory
      .getLogger(QueryIndexField.class);

  protected IndexSerializationStrategy serializer;

  public QueryIndexField(FieldMetaData fmd) {
    super(fmd, fmd.getName());

    this.serializer = IndexSerializationStrategyFactory
        .getFieldSerializationStrategy(targetField, true);
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
  public void addToComposite(DynamicComposite composite, int index,
      Object value, ComponentEquality equality) {
    log.debug("Adding value {} to composite: {} ", value, composite);
    this.serializer.addToComponent(composite, index, value, equality);
  }

  @Override
  protected IndexSerializationStrategy getSerializationStrategy() {
    return serializer;
  }

}