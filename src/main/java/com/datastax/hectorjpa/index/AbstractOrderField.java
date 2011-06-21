package com.datastax.hectorjpa.index;

import static com.datastax.hectorjpa.serializer.CompositeUtils.getCassType;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.Order;
import org.apache.openjpa.util.MetaDataException;

import com.datastax.hectorjpa.proxy.ProxyUtils;

/**
 * Inner class to encapsulate order field logic and meta data
 * 
 * @author Todd Nine
 * 
 */
public abstract class AbstractOrderField extends AbstractIndexField {

  private Order order;
  
  private int invert;

  public AbstractOrderField(Order order, FieldMetaData fmd) {
    super(fmd, order.getName());
    this.order = order;
    
    //used for in-memory comparison of types
    invert = this.order.isAscending() ? 1 : -1;
    
    if(!Comparable.class.isAssignableFrom(targetField.getDeclaredType())){
      throw new MetaDataException(String.format("You specified the field '%s' on class '%s' as an order field, but it does not implement the '%s' interface ", fmd.getName(), fmd.getDeclaringMetaData().getDescribedType(), Comparable.class));
    }

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

    composite.addComponent(current, serializer, getCassType(serializer, order.isAscending()));

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
      composite.addComponent(original, serializer, getCassType(serializer, order.isAscending()));
      return true;
    }
    
    original = ProxyUtils.getChanged(instance);

    // value was changed, add the old value
    if (original != null) {
      composite.addComponent(original, serializer, getCassType(serializer, order.isAscending()));
      return true;
    }

    // write the current value from the proxy. This one didn't change but
    // other fields could.
    Object current = ProxyUtils.getAdded(instance);

    composite.addComponent(current, serializer, getCassType(serializer, order.isAscending()));

    return false;

  }

/**
   * Compare the values at the given index in c1 and c2. Will return 0 if equal
   * < 0 if c1 is less > 0 if c1 is greater
   * 
   * @param c1
   * @param c2
   * @param index
   * @return
   */
  public int compare(DynamicComposite c1, int c1Index, DynamicComposite c2, int c2index) {

    Comparable<Object> c1Value = (Comparable<Object>) c1.get(c1Index,
        this.serializer);

    Comparable<Object> c2Value = (Comparable<Object>) c2.get(c2index,
        this.serializer);


    return c1Value.compareTo(c2Value) * invert;

  }

}