/**
 * 
 */
package com.datastax.hectorjpa.index.field;

import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.JavaTypes;

/**
 * Factory for getting the indexing serialization strategy based on the field
 * meta data
 * 
 * @author Todd Nine
 * 
 */
public class IndexSerializationStrategyFactory {

  /**
   * Get the field serialization strategy for indexing based on the field meta
   * data
   * 
   * @param field
   * @return
   */
  public static IndexSerializationStrategy getFieldSerializationStrategy(
      FieldMetaData field, boolean ascending) {

    switch (field.getDeclaredTypeCode()) {
    case JavaTypes.BIGDECIMAL:
      return new BigDecimalFieldSerializerStrategy(ascending);
    default:
      return new FieldSerializerStrategy(field, ascending);
    }
  }

}
