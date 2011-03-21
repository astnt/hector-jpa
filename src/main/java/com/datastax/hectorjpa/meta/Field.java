/**
 * 
 */
package com.datastax.hectorjpa.meta;

import me.prettyprint.hector.api.beans.ColumnSlice;
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

  
  /**
   * Add this field the the mutator
   * @param stateManager
   * @param mutator
   * @param clock
   * @param key
   * @param cfName
   */
  public abstract void addField(OpenJPAStateManager stateManager, Mutator<byte[]> mutator, long clock, byte[] key, String cfName);
  
  
  /**
   * Read the field from the query
   * @param stateManager
   * @param result
   */
  public abstract void readField(OpenJPAStateManager stateManager,QueryResult<ColumnSlice<String, byte[]>> result);
  
}
