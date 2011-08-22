/**
 * 
 */
package com.datastax.hectorjpa.query.field;

import org.apache.openjpa.meta.FieldMetaData;

/**
 * @author Todd Nine
 *
 */
public class CharFieldExpression extends FieldExpression {

  public CharFieldExpression(FieldMetaData field) {
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
    
    return Character.MIN_VALUE;
  }

  /* (non-Javadoc)
   * @see com.datastax.hectorjpa.query.field.FieldExpression#getEnd()
   */
  @Override
  public Object getEnd() {
   if(endSet){
     return end;
   }
   
   return Character.MAX_VALUE;
  }

}
