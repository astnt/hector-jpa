/**
 * 
 */
package com.datastax.hectorjpa.meta;

import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;


/**
 * Base class for field serialization logic
 * 
 * @author Todd Nine
 *
 */
public abstract class Field<V> {
  
  protected int fieldId;

  
  public Field(int fieldId){
    this.fieldId = fieldId;
  }
  

  /**
   * @return the fieldId
   */
  public int getFieldId() {
    return fieldId;
  }

  
}
