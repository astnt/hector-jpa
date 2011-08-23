package com.datastax.hectorjpa.index;

import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.MetaDataException;
import org.apache.openjpa.util.UserException;

import com.datastax.hectorjpa.index.field.IndexSerializationStrategy;
import com.datastax.hectorjpa.proxy.ProxyUtils;

/**
 * Inner class to encapsulate order field logic and meta data
 * 
 * @author Todd Nine
 * 
 */
public abstract class AbstractIndexField {

  protected FieldMetaData targetField;

  
  public AbstractIndexField(FieldMetaData owningField, String fieldName) {
    super();
    
    ClassMetaData owningClass = getContainerClassMetaData(owningField);
    
    targetField = owningField.getMappedByField(owningClass, fieldName);

    if(targetField == null){
      throw new MetaDataException(String.format("You specified field '%s' to be used in a order by on class '%s' but couldn't find it", fieldName, owningClass));
    }
        
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
    Object current = null;
    
    if(instance != null){
      current = ProxyUtils.getAdded(instance);
    }

    getSerializationStrategy().addToComponent(composite, current);

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
      getSerializationStrategy().addToComponent(composite, original);
      return true;
    }
    
    original = ProxyUtils.getChanged(instance);

    // value was changed, add the old value
    if (original != null) {
      getSerializationStrategy().addToComponent(composite, original);
      return true;
    }

    // write the current value from the proxy. This one didn't change but
    // other fields could.
    Object current = ProxyUtils.getAdded(instance);

    getSerializationStrategy().addToComponent(composite, current);

    return false;

  }
  

  /**
   * Compare the two dynamic composites at the given index
   * @param first
   * @param second
   * @param index
   * @return
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public int compare(DynamicComposite first, DynamicComposite second, int index) {
    Comparable c1Value = (Comparable)getSerializationStrategy().get(first, index);
    Comparable c2Value = (Comparable)getSerializationStrategy().get(second, index);
    
    if (c1Value == null && c2Value == null) {
      return 0;
    }
    if (c1Value == null) {
      return 1;
    }
    if (c2Value == null) {
      return -1;
    }
    
    return c1Value.compareTo(c2Value);
  }

  
  
  /**
   * Get the class meta data for the class that owns the ordered field
   * @param fmd
   * @return
   */
  protected abstract ClassMetaData getContainerClassMetaData(FieldMetaData fmd);
  
  /**
   * Return the serialization strategy used for indexing this field
   * @return
   */
  protected abstract IndexSerializationStrategy getSerializationStrategy();

  

  
  public FieldMetaData getMetaData(){
    return targetField;
  }


  /**
   * Prints information about this field
   */
  @Override
  public String toString() {
    return String.format("AbstractIndexField(targetFieldName:%s, targetFieldIndex:%d, serializer: %s)", 
        new Object[]{targetField.getName(), targetField.getIndex(), getSerializationStrategy().getClass().getName()});
  }
  
  
 
 
}