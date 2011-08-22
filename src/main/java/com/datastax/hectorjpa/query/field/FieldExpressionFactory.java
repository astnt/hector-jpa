/**
 * 
 */
package com.datastax.hectorjpa.query.field;

import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.JavaTypes;

/**
 * Factory for generating field expressions based on the java type
 * 
 * @author Todd Nine
 * 
 */
public class FieldExpressionFactory {

  /**
   * Get the right type of expression based on the field
   * 
   * @param field
   * @return
   */
  public static FieldExpression createFieldExpression(FieldMetaData field) {

    switch (field.getDeclaredTypeCode()) {
    case JavaTypes.BOOLEAN:
    case JavaTypes.BOOLEAN_OBJ:
      return new BooleanFieldExpression(field);
    case JavaTypes.BYTE:
    case JavaTypes.BYTE_OBJ:
      return new ByteFieldExpression(field);
    case JavaTypes.CHAR:
    case JavaTypes.CHAR_OBJ:
      return new StringFieldExpression(field);
    case JavaTypes.DOUBLE:
    case JavaTypes.DOUBLE_OBJ:
      return new DoubleFieldExpression(field);
    case JavaTypes.FLOAT:
    case JavaTypes.FLOAT_OBJ:
      return new FloatFieldExpression(field);
    case JavaTypes.INT:
    case JavaTypes.INT_OBJ:
      return new IntegerFieldExpression(field);
    case JavaTypes.LONG:
    case JavaTypes.LONG_OBJ:
      return new LongFieldExpression(field);
    case JavaTypes.SHORT:
    case JavaTypes.SHORT_OBJ:
      return new ShortFieldExpression(field);
    case JavaTypes.STRING:
      return new StringFieldExpression(field);
    case JavaTypes.DATE:
      return new DateFieldExpression(field);
    case JavaTypes.BIGDECIMAL:
      return new BigDecimalFieldExpression(field);
    case JavaTypes.BIGINTEGER:
      return new BigIntegerFieldExpression(field);
    default:
      return new ObjectFieldExpression(field);
    }

  }
}
