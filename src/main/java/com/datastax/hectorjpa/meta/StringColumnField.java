/**
 * 
 */
package com.datastax.hectorjpa.meta;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;

/**
 * Abstract class for operations that are serialized to a static string column name
 * 
 * @author Todd Nine
 *
 */
public abstract class StringColumnField extends Field {

  public StringColumnField(int fieldId, String name) {
    super(fieldId, name);
  }

  /**
   * Read the fields from the results and set it into the state manager
   * @param stateManager
   * @param result
   * @return
   */
  public abstract boolean readField(OpenJPAStateManager stateManager, QueryResult<ColumnSlice<String, byte[]>> result);
 
}
