/**
 * 
 */
package com.datastax.hectorjpa.meta.collection;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

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
import org.apache.openjpa.util.ChangeTracker;
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

    Set<HColumn<DynamicComposite, byte[]>> newOrderColumns = new HashSet<HColumn<DynamicComposite, byte[]>>();
    Set<HColumn<DynamicComposite, byte[]>> newIdColumns = new HashSet<HColumn<DynamicComposite, byte[]>>();
    Set<HColumn<DynamicComposite, byte[]>> deletedColumns = new HashSet<HColumn<DynamicComposite, byte[]>>();
    Set<HColumn<DynamicComposite, byte[]>> deletedIdColumns = new HashSet<HColumn<DynamicComposite, byte[]>>();

    // not a proxy, it's the first time this has been saved
    if (field instanceof Proxy) {

      Proxy proxy = (Proxy) field;
      ChangeTracker changes = proxy.getChangeTracker();

      createColumns(stateManager, changes.getAdded(), newOrderColumns,
          newIdColumns, clock);

      // TODO TN need to get the original value to delete old index on change
      createColumns(stateManager, changes.getChanged(), newOrderColumns,
          newIdColumns, clock);

      // add everything that needs removed
      createColumns(stateManager, changes.getRemoved(), deletedColumns,
          deletedIdColumns, clock);
    }
    // new item that hasn't been proxied, just write them as new columns
    else {
      createColumns(stateManager, (Collection<?>) field, newOrderColumns,
          newIdColumns, clock);
    }

    // construct the key
    byte[] orderKey = constructKey(key, orderedMarker);
    byte[] idKey = constructKey(key, idMarker);

    // write our updates and out deletes
    for (HColumn<DynamicComposite, byte[]> current : deletedColumns) {

      // same column exists on write, don't issue the delete since we don't want
      // to remove the write
      if (newOrderColumns.contains(current)) {
        continue;
      }

      mutator.addDeletion(orderKey, CF_NAME, current.getName(),
          compositeSerializer, clock);
    }

    for (HColumn<DynamicComposite, byte[]> current : deletedIdColumns) {

      // same column exists on write, don't issue the delete since we don't want
      // to remove the write
      if (newIdColumns.contains(current)) {
        continue;
      }

      mutator.addDeletion(idKey, CF_NAME, current.getName(),
          compositeSerializer, clock);
    }

    // write our order updates
    for (HColumn<DynamicComposite, byte[]> current : newOrderColumns) {

      mutator.addInsertion(orderKey, CF_NAME, current);
    }

    // write our key updates
    for (HColumn<DynamicComposite, byte[]> current : newIdColumns) {

      mutator.addInsertion(idKey, CF_NAME, current);
    }

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

    for (HColumn<DynamicComposite, byte[]> col : result.get().getColumns()) {
      fields = col.getName().toArray();

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
  public SliceQuery<byte[], DynamicComposite, byte[]> createQuery(Object objectId,
      Keyspace keyspace, String columnFamilyName, int count) {

    // undefined value set it to something realistic
    if (count < 0) {
      count = DEFAULT_FETCH_SIZE;
    }

    SliceQuery<byte[], DynamicComposite, byte[]> query = new ThriftSliceQuery(
        keyspace, BytesArraySerializer.get(), compositeSerializer,
        BytesArraySerializer.get());

    query.setRange(null, null, false, count);
    query
        .setKey(constructKey(mappingUtils.getKeyBytes(objectId), orderedMarker));
    query.setColumnFamily(columnFamilyName);
    return query;

  }

  /**
   * Create columns and add them to the collection of columns.
   * 
   * @param stateManager
   * @param objects
   * @param orders
   * @param clock
   */
  private void createColumns(OpenJPAStateManager stateManager,
      Collection<?> objects, Set<HColumn<DynamicComposite, byte[]>> orders,
      Set<HColumn<DynamicComposite, byte[]>> keys, long clock) {

    StoreContext ctx = stateManager.getContext();

    DynamicComposite orderComposite = null;
    DynamicComposite idComposite = null;

    for (Object current : objects) {

      Object currentId = mappingUtils.getTargetObject(ctx.getObjectId(current));

      // create our composite of the format of id+order*
      idComposite = new DynamicComposite();

      // create our composite of the format order*+id
      orderComposite = new DynamicComposite();

         // add our id to the beginning of our id based composite
      idComposite.add(currentId, idSerizlizer);

      // now construct the composite with order by the ids at the end.
      for (OrderField order : orderBy) {
        order.addField(stateManager, idComposite, current);
        order.addField(stateManager, orderComposite, current);
      }

      // add our id to the end of our order based composite
      orderComposite.add(currentId, idSerizlizer);

      // add our order based column to the columns
      orders.add(new HColumnImpl<DynamicComposite, byte[]>(orderComposite, HOLDER,
          clock, compositeSerializer, BytesArraySerializer.get()));

      // add our key based column to the key columns
      keys.add(new HColumnImpl<DynamicComposite, byte[]>(idComposite, HOLDER, clock,
          compositeSerializer, BytesArraySerializer.get()));

    }

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
     * Add the field this order represents to the composite
     * @param composite
     * @param instance
     */
    protected void addField(OpenJPAStateManager manager, DynamicComposite composite, Object instance) {

      if (instance == null) {
        return;
      }

      
      OpenJPAStateManager stateManager =  manager.getContext().getStateManager(instance);
      
      //no state, we can't get the order value
      if(stateManager == null){
        throw new UserException(String.format("You attempted to specify field '%s' on entity '%s'.  However the entity does not have a state manager.  Make sure you enable cascade for this operation or explicity persist it with the entity manager", targetFieldName, instance));
      }
      
      Object value = stateManager.fetch(targetFieldIndex);

      composite.add(value, serializer);

    }
  }

}
