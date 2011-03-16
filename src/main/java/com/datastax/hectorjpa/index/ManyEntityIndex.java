/**
 * 
 */
package com.datastax.hectorjpa.index;

import java.util.Collection;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.Order;
import org.apache.openjpa.util.Proxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.meta.OrderField;
import com.datastax.hectorjpa.store.EntityFacade;
import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Represents a many relationship to another root entity. I.E the referenced
 * entity is stored in it's own CF.
 * 
 * @author Todd Nine
 * 
 */
public class ManyEntityIndex extends AbstractEntityIndex {

  private static final Logger log = LoggerFactory.getLogger(EntityFacade.class);

  /**
   * 
   * @param fieldNumber
   *          The absolute position of this field in the class meta data
   */
  public ManyEntityIndex(FieldMetaData fmd) {
    super(fmd);
    
    //TODO TN get fields we'll be querying on and create indexes here.
    
    //always create a default index that is the other entities 
    //create the default collection index
    Index defaultIndex = new Index();
    
    
    Order[] orders = fmd.getOrders();

    if(orders != null){
      for (Order order : orders) {
        defaultIndex.addOrderField(new OrderField(order, fmd ));
      }
    }
    
    addIndex(defaultIndex);

    

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.datastax.hectorjpa.relation.AbstractEntityIndex#writeIndex(org.apache
   * .openjpa.kernel.OpenJPAStateManager)
   */
  @Override
  public void writeIndex(OpenJPAStateManager stateManager) {

    // TODO, will this only load from l1 cache (I.E. what's changed) or will
    // this load everything
    // we only want values from the scope of this transaction
    Object value = stateManager.fetch(fieldIndex);

    if (value == null) {
      return;
    }

    // currently only doing collections
    if (!(value instanceof Collection)) {
      log.warn("Only collections are currently supported");
      return;
    }

    // it's an instance of a proxy, use the change tracker to perform the op
    if (value instanceof Proxy) {
      return;
    }

    // finally, just add all entities since it's a collection.
    addEntries((Collection<?>) value,
        stateManager.getMetaData().getField(fieldIndex),
        stateManager.getContext());

  }

  private void addEntries(Collection<?> values, FieldMetaData fmd,
      StoreContext ctx) {

    for (Object entity : (Collection<?>) values) {
      // get the id of the object

      Object id = ctx.getObjectId(entity);

      // TODO get all properties that have been defined in the other value as
      // indexed/searchable in target object

      // get all Order properties no the field and add the values from the
      // target objects

    }
  }

  /**
   * TODO finish this Get the value from the order by clause, will recurse if
   * required
   * 
   * @param order
   * @param entity
   * @return
   */
  private Object getOrderValue(Order order, Object entity) {
    String name = order.getName();

    return entity;

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.datastax.hectorjpa.relation.AbstractEntityIndex#deleteIndex(org.apache
   * .openjpa.kernel.OpenJPAStateManager)
   */
  @Override
  public void deleteIndex(OpenJPAStateManager stateManager) {
    // TODO Auto-generated method stub

  }

}
