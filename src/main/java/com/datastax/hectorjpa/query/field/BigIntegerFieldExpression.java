/**
 * 
 */
package com.datastax.hectorjpa.query.field;

import java.math.BigInteger;

import org.apache.openjpa.meta.FieldMetaData;

/**
 * @author Todd Nine
 *
 */
public class BigIntegerFieldExpression extends FieldExpression {

  public BigIntegerFieldExpression(FieldMetaData field) {
    super(field);
  }

  /* (non-Javadoc)
   * @see com.datastax.hectorjpa.query.field.FieldExpression#getStart()
   */
  @Override
  public Object getStart() {
    if(startSet){
      return start;
    }
    
    return BigInteger.valueOf(Long.MIN_VALUE);
  }

  /* (non-Javadoc)
   * @see com.datastax.hectorjpa.query.field.FieldExpression#getEnd()
   */
  @Override
  public Object getEnd() {
   if(endSet){
     return end;
   }
   
   return BigInteger.valueOf(Long.MAX_VALUE);
  }

}
