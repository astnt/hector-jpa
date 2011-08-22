/**
 * 
 */
package com.datastax.hectorjpa.query.field;

import org.apache.openjpa.meta.FieldMetaData;

/**
 * @author Todd Nine
 *
 */
public class DoubleFieldExpression extends FieldExpression {

  public DoubleFieldExpression(FieldMetaData field) {
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
    
    return Float.MIN_VALUE;
  }

  /* (non-Javadoc)
   * @see com.datastax.hectorjpa.query.field.FieldExpression#getEnd()
   */
  @Override
  public Object getEnd() {
   if(endSet){
     return end;
   }
   
   return Float.MAX_VALUE;
  }

}
