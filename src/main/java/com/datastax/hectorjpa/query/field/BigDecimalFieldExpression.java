/**
 * 
 */
package com.datastax.hectorjpa.query.field;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.openjpa.meta.FieldMetaData;

/**
 * @author Todd Nine
 *
 */
public class BigDecimalFieldExpression extends FieldExpression {

  public BigDecimalFieldExpression(FieldMetaData field) {
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
    
    return new BigDecimal(BigInteger.valueOf(Long.MIN_VALUE), 0);
  }

  /* (non-Javadoc)
   * @see com.datastax.hectorjpa.query.field.FieldExpression#getEnd()
   */
  @Override
  public Object getEnd() {
   if(endSet){
     return end;
   }
   
   return new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE), 0);
  }

}
