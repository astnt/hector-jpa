/**
 * 
 */
package com.datastax.hectorjpa.meta.collection;


import static com.datastax.hectorjpa.serializer.CompositeUtils.newComposite;

import java.nio.ByteBuffer;
import java.util.Collection;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.index.AbstractIndexField;
import com.datastax.hectorjpa.meta.CollectionOrderField;
import com.datastax.hectorjpa.proxy.ProxyUtils;
import com.datastax.hectorjpa.service.IndexAudit;
import com.datastax.hectorjpa.service.IndexQueue;

/**
 * A class that performs all operations related to collections for saving and
 * updating
 * 
 * @author Todd Nine
 * 
 */
public class OrderedCollectionField extends AbstractCollectionField {
  private static Logger log = LoggerFactory.getLogger(OrderedCollectionField.class);

  // represents the end "ordered" in the key
  private static final byte[] orderedMarker = StringSerializer.get().toBytes("o");

  // represents the end "id" in the key
  private static final byte[] idMarker = StringSerializer.get().toBytes("i");

  private AbstractIndexField[] orderBy;

  public OrderedCollectionField(FieldMetaData fmd) {
    super(fmd);

    Order[] orders = fmd.getOrders();
    orderBy = new AbstractIndexField[orders.length];

    // create all our order by clauses
    for (int i = 0; i < orders.length; i++) {
      orderBy[i] = new CollectionOrderField(orders[i], fmd);
      if (log.isDebugEnabled()) {
        log.debug("adding orderyBY: {}", orderBy[i]);
      }
    }

    // orders +1 for length
    compositeFieldLength = orders.length + 1;

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
      Mutator<byte[]> mutator, long clock, byte[] key, String cfName,
      IndexQueue queue) {

    Object field = stateManager.fetch(fieldId);

    // construct the keys
    byte[] orderKey = constructKey(key, orderedMarker);
    byte[] idKey = constructKey(key, idMarker);

    // could have been removed, blitz everything from the index
    if (field == null || ((Collection<?>)field).isEmpty()) {
      mutator.addDeletion(orderKey, CF_NAME, null, null);
      mutator.addDeletion(idKey, CF_NAME, null, null);
      return;
    }

    writeAdds(stateManager, (Collection<?>) field, mutator, clock, orderKey,
        idKey, queue);
    writeDeletes(stateManager, (Collection<?>) field, mutator, clock, orderKey,
        idKey, queue);
    writeChanged(stateManager, (Collection<?>) field, mutator, clock, orderKey,
        idKey, queue);

  }

  /**
   * Read the field and load all ids found
   * 
   * @param stateManager
   * @param result
   */
  @SuppressWarnings("unchecked")
  public boolean readField(OpenJPAStateManager stateManager,
      QueryResult<ColumnSlice<DynamicComposite, byte[]>> result) {

    if (log.isDebugEnabled()) {
      log.debug("readField returned {} columns in OrderedCollection", result
          .get().getColumns().size());
    }

    StoreContext context = stateManager.getContext();

    
    Collection<Object> collection = (Collection<Object>) stateManager
        .newFieldProxy(fieldId);

    for (HColumn<DynamicComposite, byte[]> col : result.get().getColumns()) {

      ByteBuffer idBuff = col.getName().get(compositeFieldLength - 1,
          buffSerializer);

      Object nativeId = elementKeyStrategy.getInstance(idBuff);

      Object oid = context.newObjectId(targetClass, nativeId);

      Object found = context.find(oid, true, null);

      // object was not found, what do we do with it?
      if (found == null) {
        continue;
      }

      collection.add(found);

    }

    // now load all the objects from the ids we were given.

    stateManager.store(fieldId, collection);

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
  // @Override
  // public SliceQuery<byte[], DynamicComposite, byte[]> createQuery(
  // Object objectId, Keyspace keyspace, String columnFamilyName, int count) {
  //
  // // undefined value set it to something realistic
  // if (count < 0) {
  // count = DEFAULT_FETCH_SIZE;
  // }
  //
  // byte[] key = constructKey(MappingUtils.toByteBuffer(objectId),
  // orderedMarker);
  //
  // SliceQuery<byte[], DynamicComposite, byte[]> query = new ThriftSliceQuery(
  // keyspace, BytesArraySerializer.get(), compositeSerializer,
  // BytesArraySerializer.get());
  //
  // query.setRange(null, null, false, count);
  // query.setKey(key);
  // query.setColumnFamily(columnFamilyName);
  // return query;
  //
  // }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.datastax.hectorjpa.meta.collection.AbstractCollectionField#removeCollection
   * (org.apache.openjpa.kernel.OpenJPAStateManager,
   * me.prettyprint.hector.api.mutation.Mutator, long)
   */
  @Override
  public void removeCollection(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock, byte[] key) {
    // construct the keys
    byte[] orderKey = constructKey(key, orderedMarker);
    byte[] idKey = constructKey(key, idMarker);

    // could have been removed, blitz everything from the index
    mutator.addDeletion(orderKey, CF_NAME, null, null);
    mutator.addDeletion(idKey, CF_NAME, null, null);

  }

  /**
   * Remove all indexes for elements
   * 
   * @param ctx
   * @param value
   * @param mutator
   * @param clock
   * @param orderKey
   * @param idKey
   * @param cfName
   */
  @SuppressWarnings("rawtypes")
  private void writeDeletes(OpenJPAStateManager stateManager, Collection value,
      Mutator<byte[]> mutator, long clock, byte[] orderKey, byte[] idKey,
      IndexQueue queue) {
    if (log.isDebugEnabled()) {
      log.debug("OrderedCollection.writeDeletes");
    }

    Collection objects = ProxyUtils.getRemoved(value);

    if (objects == null || objects.size() == 0) {
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("assembling deletes for {} objects", objects.size());
    }

    DynamicComposite orderComposite = null;
    DynamicComposite idComposite = null;

    Object field = null;
    ByteBuffer currentId;

    StoreContext context = stateManager.getContext();

    OpenJPAStateManager currentSm = null;
    Object oid = null;

    // TODO TN remove from opposite index

    // loop through all deleted object and create the deletes for them.
    for (Object current : objects) {

      currentSm = context.getStateManager(current);
      oid = currentSm.fetchObjectId();
      currentId = elementKeyStrategy.toByteBuffer(oid);

      if (log.isDebugEnabled()) {
        log.debug("deleting object with id {}", currentId);
      }

      // create our composite of the format of id+order*
      idComposite = newComposite();

      // create our composite of the format order*+id
      orderComposite = newComposite();

      // add our id to the beginning of our id based composite
      idComposite.addComponent(currentId, buffSerializer);

      // now construct the composite with order by the ids at the end.
      for (AbstractIndexField order : orderBy) {
        if (log.isDebugEnabled()) {
          log.debug("deleting ordered field {}", order);
        }
        field = order.getValue(stateManager, current);

        // add this to all deletes for the order composite.
        order.addFieldDelete(orderComposite, field);

        // The deletes to teh is composite
        order.addFieldDelete(idComposite, field);
      }

      // add our id to the end of our order based composite
      orderComposite.addComponent(currentId, buffSerializer);

      mutator.addDeletion(orderKey, CF_NAME, orderComposite,
          compositeSerializer, clock);
      mutator.addDeletion(idKey, CF_NAME, idComposite, compositeSerializer,
          clock);

      DynamicComposite idAudit = newComposite();
      idAudit.addComponent(currentId, buffSerializer);

      // add the check to the audit queue
      queue.addDelete(new IndexAudit(orderKey, idKey, idAudit, clock, CF_NAME,
          true));

    }

  }

  /**
   * Write all indexes for newly added elements
   * 
   * @param ctx
   * @param value
   * @param mutator
   * @param clock
   * @param orderKey
   * @param idKey
   * @param cfName
   */
  @SuppressWarnings("rawtypes")
  private void writeAdds(OpenJPAStateManager stateManager, Collection value,
      Mutator<byte[]> mutator, long clock, byte[] orderKey, byte[] idKey,
      IndexQueue queue) {

    Collection objects = ProxyUtils.getAdded(value);

    if (objects == null) {
      return;
    }

    DynamicComposite orderComposite = null;
    DynamicComposite idComposite = null;
    ByteBuffer currentId = null;
    Object field = null;

    StoreContext context = stateManager.getContext();

    OpenJPAStateManager currentSm = null;
    Object oid = null;

    // loop through all added objects and create the writes for them.
    for (Object current : objects) {

      currentSm = context.getStateManager(current);
      oid = currentSm.fetchObjectId();
      currentId = elementKeyStrategy.toByteBuffer(oid);

      // create our composite of the format of id+order*
      idComposite = newComposite();

      // create our composite of the format order*+id
      orderComposite = newComposite();

      // add our id to the beginning of our id based composite
      idComposite.addComponent(currentId, buffSerializer, compositeComparator);

      // now construct the composite with order by the ids at the end.
      for (AbstractIndexField order : orderBy) {

        field = order.getValue(stateManager, current);

        // add this to all deletes for the order composite.
        order.addFieldWrite(orderComposite, field);

        // add the deletes to the composite
        order.addFieldWrite(idComposite, field);
      }

      // add our id to the end of our order based composite
      orderComposite.addComponent(currentId, buffSerializer, compositeComparator);

      mutator.addInsertion(orderKey, CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(orderComposite, HOLDER,
              clock, compositeSerializer, BytesArraySerializer.get()));

      mutator.addInsertion(idKey, CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(idComposite, HOLDER, clock,
              compositeSerializer, BytesArraySerializer.get()));

      DynamicComposite idAudit = newComposite();
      idAudit.addComponent(currentId, buffSerializer);

      // add the check to the audit queue
      queue.addAudit(new IndexAudit(orderKey, idKey, idAudit, clock, CF_NAME,
          true));

    }
  }

  /**
   * Write all changes columns. Also writes the delete for the original values
   * from the proxys
   * 
   * @param ctx
   * @param value
   * @param mutator
   * @param clock
   * @param orderKey
   * @param idKey
   * @param cfName
   */
  @SuppressWarnings("rawtypes")
  private void writeChanged(OpenJPAStateManager stateManager, Collection value,
      Mutator<byte[]> mutator, long clock, byte[] orderKey, byte[] idKey,
      IndexQueue queue) {

    Collection objects = ProxyUtils.getChanged(value);

    if (objects == null) {
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("writeChanged {} items in OrderedCOllection", objects.size());
    }
    DynamicComposite orderComposite = null;
    DynamicComposite idComposite = null;
    ByteBuffer currentId = null;

    Object field = null;

    StoreContext context = stateManager.getContext();
    

    OpenJPAStateManager currentSm = null;
    Object oid = null;


    // loop through all added objects and create the writes for them.
    for (Object current : objects) {

      // if any of the fields are dirty we need to set our changed flag so we
      // can delete the oiginal columns later
    
      currentSm = context.getStateManager(current);
      oid = currentSm.fetchObjectId();
      currentId = elementKeyStrategy.toByteBuffer(oid);

      // create our composite of the format of id+order*
      idComposite = newComposite();
    
      // create our composite of the format order*+id
      orderComposite = newComposite();
    
      // add our id to the beginning of our id based composite
      idComposite.addComponent(currentId, buffSerializer);

      // now construct the composite with order by the ids at the end.
      for (AbstractIndexField order : orderBy) {

        field = order.getValue(stateManager, current);

        // add this to all deletes for the order composite.
        order.addFieldWrite(orderComposite, field);
    
        // The deletes to teh is composite
        order.addFieldWrite(idComposite, current);
      }

      // add our id to the end of our order based composite
      orderComposite.addComponent(currentId, buffSerializer);

      // add our order based column to the columns

      mutator.addInsertion(orderKey, CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(orderComposite, HOLDER,
              clock, compositeSerializer, BytesArraySerializer.get()));

      mutator.addInsertion(idKey, CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(idComposite, HOLDER, clock,
              compositeSerializer, BytesArraySerializer.get()));

      DynamicComposite idAudit = newComposite();
      idAudit.addComponent(currentId, buffSerializer);

      // add the check to the audit queue
      queue.addAudit(new IndexAudit(orderKey, idKey, idAudit, clock, CF_NAME,
          true));

    

    }

  }



}
