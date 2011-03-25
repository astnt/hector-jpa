package com.datastax.hectorjpa.meta;

import java.util.Iterator;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.Order;
import org.apache.openjpa.util.Proxy;
import org.apache.openjpa.util.UserException;

import com.datastax.hectorjpa.proxy.ProxyUtils;
import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Inner class to encapsulate order field logic and meta data
 * 
 * @author Todd Nine
 * 
 */
public abstract class AbstractOrderField {

  private Order order;
  private Serializer<Object> serializer;
  private int targetFieldIndex;
  private String targetFieldName;

  public AbstractOrderField(Order order, FieldMetaData fmd) {
    super();
    this.order = order;
    
    FieldMetaData targetField = fmd.getMappedByField(getContainerClassMetaData(fmd), order.getName());

        
    this.serializer = MappingUtils.getSerializer(targetField);

    this.targetFieldIndex = targetField.getIndex();
    this.targetFieldName = targetField.getName();

  }

  /**
   * Get the value for the ordered field off the instance. Could return null
   * if the instance is null or the field is null
   * 
   * @param manager
   * @param instance
   * @return
   */
  public Object getValue(OpenJPAStateManager manager, Object instance) {
    if (instance == null) {
      return null;
    }

    OpenJPAStateManager stateManager = manager.getContext().getStateManager(
        instance);

    // no state, we can't get the order value
    if (stateManager == null) {
      throw new UserException(
          String
              .format(
                  "You attempted to specify field '%s' on entity '%s'.  However the entity does not have a state manager.  Make sure you enable cascade for this operation or explicity persist it with the entity manager",
                  targetFieldName, instance));
    }

    return stateManager.fetch(targetFieldIndex);
  }

  /**
   * Create the write composite.
   * 
   * @param manager
   *          The state manager
   * @param composite
   *          The composite to write to
   * @param instance
   *          The field instance
   */
  public void addFieldWrite(DynamicComposite composite, Object instance) {
    // write the current value from the proxy
    Object current = ProxyUtils.getAdded(instance);

    composite.add(current, serializer);

  }

  /**
   * Create the write composite.
   * 
   * @param composite
   *          The composite to write to
   * @param instance
   *          The field instance
   */
  public boolean addFieldDelete(DynamicComposite composite, Object instance) {

    // check if there was an original value. If so write it to the composite
    Object original = ProxyUtils.getRemoved(instance);

    // value was changed, add the old value
    if (original != null) {
      composite.add(original, serializer);
      return true;
    }
    
    original = ProxyUtils.getChanged(instance);

    // value was changed, add the old value
    if (original != null) {
      composite.add(original, serializer);
      return true;
    }

    // write the current value from the proxy. This one didn't change but
    // other fields could.
    Object current = ProxyUtils.getAdded(instance);

    composite.add(current, serializer);

    return false;

  }
  
  /**
   * Get the class meta data for the class that owns the ordered field
   * @param fmd
   * @return
   */
  protected abstract ClassMetaData getContainerClassMetaData(FieldMetaData fmd);
 
 
}