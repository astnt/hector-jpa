/**
 * 
 */
package com.datastax.hectorjpa.meta.collection;

import java.util.Collection;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.model.thrift.ThriftSliceQuery;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
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

import com.datastax.hectorjpa.meta.AbstractOrderField;
import com.datastax.hectorjpa.meta.CollectionOrderField;
import com.datastax.hectorjpa.proxy.ProxyUtils;
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

  private AbstractOrderField[] orderBy;
  

  public OrderedCollectionField(FieldMetaData fmd, MappingUtils mappingUtils) {
    super(fmd, mappingUtils);

    Order[] orders = fmd.getOrders();
    orderBy = new AbstractOrderField[orders.length];

    // create all our order by clauses
    for (int i = 0; i < orders.length; i++) {
      orderBy[i] = new CollectionOrderField(orders[i], fmd);
    }

    //orders +1 for length
    compositeFieldLength = orders.length+1;
    
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
  public boolean readField(OpenJPAStateManager stateManager,
      QueryResult<ColumnSlice<DynamicComposite, byte[]>> result) {


    StoreContext context = stateManager.getContext();

    // TODO TN use our CollectionProxy here
    Collection<Object> collection = (Collection<Object>) stateManager.newFieldProxy(fieldId);
    
    for (HColumn<DynamicComposite, byte[]> col : result.get().getColumns()) {

      // TODO TN set the serializers in the columns before deserailizing
      Object nativeId = col.getName().get(compositeFieldLength-1, this.idSerizlizer);

      
      Object oid = context.newObjectId(targetClass, nativeId);
      
      Object found = context.find(oid, true, null);
      
      //object was not found, what do we do with it?
      if(found == null){
        continue;
      }
      
      collection.add(found);

    }

    // now load all the objects from the ids we were given.

    stateManager.storeObject(fieldId, collection);

    return result.get().getColumns().size() > 0;
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

    Collection objects = ProxyUtils.getRemoved(value);

    if (objects == null) {
      return;
    }

    DynamicComposite orderComposite = null;
    DynamicComposite idComposite = null;
    Object currentId = null;
    Object field = null;
    
    StoreContext context = stateManager.getContext();
    
    //TODO TN remove from opposite index 

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
      for (AbstractOrderField order : orderBy) {
        
        field = order.getValue(stateManager, current);
        
        // add this to all deletes for the order composite.
        order.addFieldDelete(orderComposite, field);

        // The deletes to teh is composite
        order.addFieldDelete(idComposite, field);
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

    Collection objects = ProxyUtils.getAdded(value);

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
      for (AbstractOrderField order : orderBy) {
        
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

    Collection objects = ProxyUtils.getChanged(value);

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
      for (AbstractOrderField order : orderBy) {
        
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



  

}
