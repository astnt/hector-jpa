/**
 * 
 */
package com.datastax.hectorjpa.meta.collection;

import java.util.Collection;
import java.util.Iterator;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.model.thrift.ThriftSliceQuery;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.Order;
import org.apache.openjpa.util.Proxy;
import org.apache.openjpa.util.UserException;

import com.datastax.hectorjpa.store.MappingUtils;

/**
 * A class that performs all operations related to collections for saving and
 * updating
 * 
 * @author Todd Nine
 * 
 */
public class OrderedCollectionField<V> extends AbstractCollectionField<V> {

  // represents the end "ordered" in the key
  private static final byte[] orderedMarker = StringSerializer.get().toBytes("o");

  // represents the end "id" in the key
  private static final byte[] idMarker = StringSerializer.get().toBytes("i");

  private OrderField[] orderBy;

  public OrderedCollectionField(FieldMetaData fmd, MappingUtils mappingUtils) {
    super(fmd, mappingUtils);

    Order[] orders = fmd.getOrders();
    orderBy = new OrderField[orders.length];

    // create all our order by clauses
    for (int i = 0; i < orders.length; i++) {
      orderBy[i] = new OrderField(orders[i], fmd);
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.datastax.hectorjpa.meta.collection.AbstractCollectionField#
   * getDefaultSearchmarker()
   */
  @Override
  protected byte[] getDefaultSearchmarker() {
    return orderedMarker;
  }

  @Override
  public void addField(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock, byte[] key, String cfName) {

    Object field = stateManager.fetch(fieldId);

    // nothing to do
    if (field == null) {
      return;
    }

    // construct the keys
    byte[] orderKey = constructKey(key, orderedMarker);
    byte[] idKey = constructKey(key, idMarker);

    writeAdds(stateManager, (Collection<?>)field, mutator, clock, orderKey, idKey);
    writeDeletes(stateManager, (Collection<?>)field, mutator, clock, orderKey, idKey);
    writeChanged(stateManager, (Collection<?>)field, mutator, clock, orderKey, idKey);


  }

  /**
   * Read the field and load all ids found
   * 
   * @param stateManager
   * @param result
   */
  public void readField(OpenJPAStateManager stateManager,
      QueryResult<ColumnSlice<DynamicComposite, byte[]>> result) {

    Object[] fields = null;

    StoreContext context = stateManager.getContext();

    // TODO TN use our CollectionProxy here
    Collection<Object> collection = (Collection<Object>) stateManager.newProxy(fieldId);

    DynamicComposite dynamicCol = null;

    
    
    for (HColumn<DynamicComposite, byte[]> col : result.get().getColumns()) {

      // TODO TN set the serializers in the columns before deserailizing
      dynamicCol = col.getName();

      fields = dynamicCol.toArray();

      // the id will always be the last value in a composite type, we only care
      // about that value.
      Object nativeId = fields[fields.length - 1];

      collection.add(context.find(context.newObjectId(targetClass, nativeId),
          true, null));

    }

    // now load all the objects from the ids we were given.

    stateManager.storeObject(fieldId, collection);

  }

  /**
   * Create a SliceQuery for this collection
   * 
   * @param objectId
   * @param keyspace
   * @param count
   * @return
   */
  public SliceQuery<byte[], DynamicComposite, byte[]> createQuery(
      Object objectId, Keyspace keyspace, String columnFamilyName, int count) {

    // undefined value set it to something realistic
    if (count < 0) {
      count = DEFAULT_FETCH_SIZE;
    }

    byte[] key = constructKey(mappingUtils.getKeyBytes(objectId), orderedMarker);
    
    SliceQuery<byte[], DynamicComposite, byte[]> query = new ThriftSliceQuery(
        keyspace, BytesArraySerializer.get(), compositeSerializer,
        BytesArraySerializer.get());

    query.setRange(null, null, false, count);
    query.setKey(key);
    query.setColumnFamily(columnFamilyName);
    return query;

  }

 
  /**
   * Remove all indexes for elements
   * @param ctx
   * @param value
   * @param mutator
   * @param clock
   * @param orderKey
   * @param idKey
   * @param cfName
   */
  private void writeDeletes(OpenJPAStateManager stateManager, Collection value,
      Mutator<byte[]> mutator, long clock, byte[] orderKey, byte[] idKey) {

    Collection objects = getRemoved(value);

    if (objects == null) {
      return;
    }

    DynamicComposite orderComposite = null;
    DynamicComposite idComposite = null;
    Object currentId = null;
    Object field = null;
    
    StoreContext context = stateManager.getContext();

    // loop through all deleted object and create the deletes for them.
    for (Object current : objects) {

      currentId = mappingUtils.getTargetObject(context.getObjectId(current));

      // create our composite of the format of id+order*
      idComposite = new DynamicComposite();

      // create our composite of the format order*+id
      orderComposite = new DynamicComposite();

      // add our id to the beginning of our id based composite
      idComposite.add(currentId, idSerizlizer);

      // now construct the composite with order by the ids at the end.
      for (OrderField order : orderBy) {
        
        field = order.getValue(stateManager, current);
        
        // add this to all deletes for the order composite.
        order.addFieldDelete(orderComposite, current);

        // The deletes to teh is composite
        order.addFieldDelete(idComposite, current);
      }

      // add our id to the end of our order based composite
      orderComposite.add(currentId, idSerizlizer);

      mutator.addDeletion(orderKey, CF_NAME, orderComposite,
          compositeSerializer, clock);
      mutator.addDeletion(idKey, CF_NAME, idComposite, compositeSerializer,
          clock);

    }

  }

  /**
   * Write all indexes for newly added elements
   * @param ctx
   * @param value
   * @param mutator
   * @param clock
   * @param orderKey
   * @param idKey
   * @param cfName
   */
  private void writeAdds(OpenJPAStateManager stateManager, Collection value,
      Mutator<byte[]> mutator, long clock, byte[] orderKey, byte[] idKey) {

    Collection objects = getAdded(value);

    if (objects == null) {
      return;
    }

    DynamicComposite orderComposite = null;
    DynamicComposite idComposite = null;
    Object currentId = null;
    Object field = null;
    

    StoreContext context = stateManager.getContext();

    // loop through all added objects and create the writes for them.
    for (Object current : objects) {

      currentId = mappingUtils.getTargetObject(context.getObjectId(current));

      // create our composite of the format of id+order*
      idComposite = new DynamicComposite();

      // create our composite of the format order*+id
      orderComposite = new DynamicComposite();

      // add our id to the beginning of our id based composite
      idComposite.add(currentId, idSerizlizer);

      // now construct the composite with order by the ids at the end.
      for (OrderField order : orderBy) {
        
        field = order.getValue(stateManager, current);
        
        // add this to all deletes for the order composite.
        order.addFieldWrite(orderComposite, field);

        // The deletes to teh is composite
        order.addFieldWrite(idComposite, field);
      }

      // add our id to the end of our order based composite
      orderComposite.add(currentId, idSerizlizer);

      mutator.addInsertion(orderKey, CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(orderComposite, HOLDER,
              clock, compositeSerializer, BytesArraySerializer.get()));

      mutator.addInsertion(idKey, CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(idComposite, HOLDER, clock,
              compositeSerializer, BytesArraySerializer.get()));

    }
  }

  /**
   * Write all changes columns.  Also writes the delete for the original values from the proxys
   * @param ctx
   * @param value
   * @param mutator
   * @param clock
   * @param orderKey
   * @param idKey
   * @param cfName
   */
  private void writeChanged(OpenJPAStateManager stateManager, Collection value,
      Mutator<byte[]> mutator, long clock, byte[] orderKey, byte[] idKey) {

    Collection objects = getChanged(value);

    if (objects == null) {
      return;
    }

    DynamicComposite orderComposite = null;
    DynamicComposite idComposite = null;
    Object currentId = null;

    boolean changed;

    DynamicComposite deleteOrderComposite;
    DynamicComposite deleteIdComposite;
    
    Object field = null;
    
    StoreContext context = stateManager.getContext();
    

    // loop through all added objects and create the writes for them.
    for (Object current : objects) {
      
      //if any of the fields are dirty we need to set our changed flag so we can delete the oiginal columns later
      changed = false;

      currentId = mappingUtils.getTargetObject(context.getObjectId(current));

      // create our composite of the format of id+order*
      idComposite = new DynamicComposite();
      deleteIdComposite = new DynamicComposite();

      // create our composite of the format order*+id
      orderComposite = new DynamicComposite();
      deleteOrderComposite = new DynamicComposite();

      // add our id to the beginning of our id based composite
      idComposite.add(currentId, idSerizlizer);

      // now construct the composite with order by the ids at the end.
      for (OrderField order : orderBy) {
        
        field = order.getValue(stateManager, current);
        
        // add this to all deletes for the order composite.
        order.addFieldWrite(orderComposite, field);
        changed |= order.addFieldDelete(deleteOrderComposite, field);

        // The deletes to teh is composite
        order.addFieldWrite(idComposite, current);
        changed |= order.addFieldDelete(deleteIdComposite, field);
      }

      // add our id to the end of our order based composite
      orderComposite.add(currentId, idSerizlizer);

      // add our order based column to the columns

      mutator.addInsertion(orderKey, CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(orderComposite, HOLDER,
              clock, compositeSerializer, BytesArraySerializer.get()));

      mutator.addInsertion(idKey, CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(idComposite, HOLDER, clock,
              compositeSerializer, BytesArraySerializer.get()));

      if (changed) {
        mutator.addDeletion(orderKey, CF_NAME, deleteOrderComposite,
            compositeSerializer, clock);
        mutator.addDeletion(idKey, CF_NAME, deleteIdComposite,
            compositeSerializer, clock);

      }

    }

  }

  /**
   * Return the collection of deleted objects from the proxy. If none is preset
   * then null is returned
   * 
   * @param field
   * @return
   */
  private Collection getRemoved(Collection field) {
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
  private Collection getChanged(Collection field) {
    if (field instanceof Proxy) {
      return ((Proxy) field).getChangeTracker().getChanged();
    }

    return null;

  }

  /**
   * Get added values. If the item is not a proxy it is returned as a collection
   * 
   * @param field
   * @return
   */
  private Collection getAdded(Collection field) {
    if (field instanceof Proxy) {
      return ((Proxy) field).getChangeTracker().getAdded();
    }

    return field;

  }

  /**
   * Inner class to encapsulate order field logic and meta data
   * 
   * @author Todd Nine
   * 
   */
  protected static class OrderField {

    private Order order;
    private Serializer<Object> serializer;
    private int targetFieldIndex;
    private String targetFieldName;

    public OrderField(Order order, FieldMetaData fmd) {
      super();
      this.order = order;

      FieldMetaData targetField = fmd.getMappedByField(fmd.getElement()
          .getTypeMetaData(), order.getName());

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
    protected Object getValue(OpenJPAStateManager manager, Object instance) {
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
    protected void addFieldWrite(DynamicComposite composite, Object instance) {
      // write the current value from the proxy
      Object current = getAdded(instance);

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
    protected boolean addFieldDelete(DynamicComposite composite, Object instance) {

      // check if there was an original value. If so write it to the composite
      Object original = getRemoved(instance);

      // value was changed, add the old value
      if (original != null) {
        composite.add(original, serializer);
        return true;
      }

      // write the current value from the proxy. This one didn't change but
      // other fields could.
      Object current = getAdded(instance);

      composite.add(current, serializer);

      return false;

    }

    /**
     * Return the collection of deleted objects from the proxy. If none is
     * preset then null is returned
     * 
     * @param field
     * @return
     */
    private Object getRemoved(Object field) {
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
    private Object getAdded(Object field) {
      if (field instanceof Proxy) {
        Iterator<?> resultItr = ((Proxy) field).getChangeTracker().getAdded()
            .iterator();

        if (resultItr.hasNext()) {
          return resultItr.next();
        }
      }

      return field;

    }
  }

}
