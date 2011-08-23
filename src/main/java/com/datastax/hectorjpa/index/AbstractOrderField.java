package com.datastax.hectorjpa.index;

import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.Order;
import org.apache.openjpa.util.MetaDataException;

import com.datastax.hectorjpa.index.field.IndexSerializationStrategy;
import com.datastax.hectorjpa.index.field.IndexSerializationStrategyFactory;
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

  private IndexSerializationStrategy orderSerializer;

  /**
   * 
   * @param order
   *          The order object
   * @param fmd
   *          The field meta data
   * @param index
   *          The 0 based position in the index
   */
  public AbstractOrderField(Order order, FieldMetaData fmd) {
    super(fmd, order.getName());
    this.order = order;

    // used for in-memory comparison of types
    invert = this.order.isAscending() ? 1 : -1;

    if (!Comparable.class.isAssignableFrom(targetField.getDeclaredType())) {
      throw new MetaDataException(
          String
              .format(
                  "You specified the field '%s' on class '%s' as an order field, but it does not implement the '%s' interface ",
                  fmd.getName(), fmd.getDeclaringMetaData().getDescribedType(),
                  Comparable.class));
    }

    orderSerializer = IndexSerializationStrategyFactory.getFieldSerializationStrategy(targetField, order.isAscending());

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

    if (instance != null) {
      current = ProxyUtils.getAdded(instance);
    }

    orderSerializer.addToComponent(composite, current);
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
      orderSerializer.addToComponent(composite, original);
      return true;
    }

    original = ProxyUtils.getChanged(instance);

    // value was changed, add the old value
    if (original != null) {
      orderSerializer.addToComponent(composite, original);
      return true;
    }

    // write the current value from the proxy. This one didn't change but
    // other fields could.
    Object current = ProxyUtils.getAdded(instance);

    orderSerializer.addToComponent(composite, current);

    return false;

  }

  @Override
  public int compare(DynamicComposite first, DynamicComposite second, int index) {
    return super.compare(first, second, index) * invert;
  }

  @Override
  protected IndexSerializationStrategy getSerializationStrategy() {
    return orderSerializer;
  }

}