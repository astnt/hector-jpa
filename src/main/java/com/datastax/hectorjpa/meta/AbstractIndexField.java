package com.datastax.hectorjpa.meta;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.MetaDataException;
import org.apache.openjpa.util.UserException;

import com.datastax.hectorjpa.proxy.ProxyUtils;
import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Inner class to encapsulate order field logic and meta data
 * 
 * @author Todd Nine
 * 
 */
public abstract class AbstractIndexField {

  
  protected Serializer<Object> serializer;
//  protected int targetFieldIndex;
//  protected String targetFieldName;
  protected FieldMetaData targetField;

  
  public AbstractIndexField(FieldMetaData owningField, String fieldName) {
    super();
    
    ClassMetaData owningClass = getContainerClassMetaData(owningField);
    
    targetField = owningField.getMappedByField(owningClass, fieldName);

    if(targetField == null){
      throw new MetaDataException(String.format("You specified field '%s' to be used in a order by on class '%s' but couldn't find it", fieldName, owningClass));
    }
        
    this.serializer = MappingUtils.getSerializer(targetField);

  }
  
  /**
   * Default constructor
   */
  public AbstractIndexField(){
    
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
                  targetField.getIndex(), instance));
    }

    return stateManager.fetch(targetField.getIndex());
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

    composite.addComponent(current, serializer);

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
      composite.addComponent(original, serializer);
      return true;
    }
    
    original = ProxyUtils.getChanged(instance);

    // value was changed, add the old value
    if (original != null) {
      composite.addComponent(original, serializer);
      return true;
    }

    // write the current value from the proxy. This one didn't change but
    // other fields could.
    Object current = ProxyUtils.getAdded(instance);

    composite.addComponent(current, serializer);

    return false;

  }
  

  
  /**
   * Get the class meta data for the class that owns the ordered field
   * @param fmd
   * @return
   */
  protected abstract ClassMetaData getContainerClassMetaData(FieldMetaData fmd);
  
  

  
  /**
   * @return the serializer
   */
  public Serializer<Object> getSerializer() {
    return serializer;
  }

//  /**
//   * @return the targetFieldIndex
//   */
//  public int getTargetFieldIndex() {
//    return targetFieldIndex;
//  }
//
//  /**
//   * @return the targetFieldName
//   */
//  public String getTargetFieldName() {
//    return targetFieldName;
//  }

  /**
   * Prints information about this field
   */
  @Override
  public String toString() {
    return String.format("AbstractIndexField(targetFieldName:%s, targetFieldIndex:%d, serializer: %s)", 
        new Object[]{targetField.getName(), targetField.getIndex(), serializer.getClass().getName()});
  }
  
  
 
 
}