/**
 * 
 */
package com.datastax.hectorjpa.index.field;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

/**
 * @author Todd Nine
 *
 */
public interface IndexSerializationStrategy {

  /**
   * Add the value in the next component position of the composite.  Performs all serialization internally in the instance  
   * @param composite
   * @param value
   */
  public void addToComponent(DynamicComposite composite, Object value);
  
  
  /**
   * Add the value in the next component position of the composite.  Performs all serialization internally in the instance  
   * Used for constructing composites for range scans
   * @param composite
   * @param value
   */
  public void addToComponent(DynamicComposite composite, int index, Object value, ComponentEquality equality);
  
  /**
   * Get the value from the specified dynamic composite
   * @param composite
   * @param index
   * @return
   */
  public Object get(DynamicComposite composite, int index);
}
