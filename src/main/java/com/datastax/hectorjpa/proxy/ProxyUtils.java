/**
 * 
 */
package com.datastax.hectorjpa.proxy;

import java.util.Collection;
import java.util.Iterator;

import org.apache.openjpa.util.Proxy;

/**
 * Utility class for dealing with changes and proxied objects
 * @author Todd Nine
 *
 */
public class ProxyUtils {

  /**
   * Return the collection of deleted objects from the proxy. If none is preset
   * then null is returned
   * 
   * @param field
   * @return
   */
  public static Collection<?> getRemoved(Collection<?> field) {
    if (field instanceof Proxy) {
      return ((Proxy) field).getChangeTracker().getRemoved();
    }

    return null;

  }

  /**
   * Get changed values. Null is returned if nothing has changed
   * 
   * @param field
   * @return
   */
  public static Collection<?>  getChanged(Collection<?>  field) {
    if (field instanceof Proxy) {
      return ((Proxy) field).getChangeTracker().getChanged();
    }
    
    return null;

  }
  

  /**
   * Get added values. If the item is not a proxy it is returned as a Collection<?> 
   * 
   * @param field
   * @return
   */
  public static Collection<?>  getAdded(Collection<?>  field) {
    if (field instanceof Proxy) {
      return ((Proxy) field).getChangeTracker().getAdded();
    }

    return field;

  }
  
  /**
   * Return the first object that was removed if the collection is a proxy
   * preset then null is returned
   * 
   * @param field
   * @return
   */
  public static Object getRemoved(Object field) {
    if (field instanceof Proxy) {
      Iterator<?> resultItr = ((Proxy) field).getChangeTracker().getRemoved()
          .iterator();

      if (resultItr.hasNext()) {
        return resultItr.next();
      }

    }

    return null;

  }

  /**
   * Get added values. If the item is not a proxy it is returned as a
   * collection
   * 
   * @param field
   * @return
   */
  public static Object getAdded(Object field) {
    if (field instanceof Proxy) {
      Iterator<?> resultItr = ((Proxy) field).getChangeTracker().getAdded()
          .iterator();

      if (resultItr.hasNext()) {
        return resultItr.next();
      }
    }

    return field;

  }
  
  /**
   * Get added values. If the item is not a proxy it is returned as a
   * collection
   * 
   * @param field
   * @return
   */
  public static Object getChanged(Object field) {
    if (field instanceof Proxy) {
      Iterator<?> resultItr = ((Proxy) field).getChangeTracker().getChanged()
          .iterator();

      if (resultItr.hasNext()) {
        return resultItr.next();
      }
    }

    return field;

  }
}
