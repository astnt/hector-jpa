/**
 * 
 */
package com.datastax.hectorjpa.meta.collection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.model.thrift.ThriftSliceQuery;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.openjpa.enhance.PersistenceCapable;
import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.Order;
import org.apache.openjpa.util.ChangeTracker;
import org.apache.openjpa.util.MetaDataException;
import org.apache.openjpa.util.Proxy;

import com.datastax.hectorjpa.store.MappingUtils;
import compositecomparer.Composite;

/**
 * A class that performs all operations related to collections for saving and
 * updating
 * 
 * @author Todd Nine
 * 
 */
public class OrderedCollectionField<V> extends AbstractCollectionField<V> {

  // represents the end "ordered" in the key
  private static final byte[] orderedMarker = StringSerializer.get().toBytes(
      "o");

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

    Set<HColumn<Composite, byte[]>> newOrderColumns = new HashSet<HColumn<Composite, byte[]>>();
    Set<HColumn<Composite, byte[]>> newIdColumns = new HashSet<HColumn<Composite, byte[]>>();
    Set<HColumn<Composite, byte[]>> deletedColumns = new HashSet<HColumn<Composite, byte[]>>();
    Set<HColumn<Composite, byte[]>> deletedIdColumns = new HashSet<HColumn<Composite, byte[]>>();

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
    for (HColumn<Composite, byte[]> current : deletedColumns) {

      // same column exists on write, don't issue the delete since we don't want
      // to remove the write
      if (newOrderColumns.contains(current)) {
        continue;
      }

      mutator.addDeletion(orderKey, CF_NAME, current.getName(),
          compositeSerializer, clock);
    }

    for (HColumn<Composite, byte[]> current : deletedIdColumns) {

      // same column exists on write, don't issue the delete since we don't want
      // to remove the write
      if (newIdColumns.contains(current)) {
        continue;
      }

      mutator.addDeletion(idKey, CF_NAME, current.getName(),
          compositeSerializer, clock);
    }

    // write our order updates
    for (HColumn<Composite, byte[]> current : newOrderColumns) {

      mutator.addInsertion(orderKey, CF_NAME, current);
    }

    // write our key updates
    for (HColumn<Composite, byte[]> current : newIdColumns) {

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
      QueryResult<ColumnSlice<Composite, byte[]>> result) {

    Object[] fields = null;

    StoreContext context = stateManager.getContext();

    // TODO TN use our CollectionProxy here
    List<Object> results = new ArrayList<Object>(result.get().getColumns()
        .size());

    for (HColumn<Composite, byte[]> col : result.get().getColumns()) {
      fields = col.getName().toArray();

      // the id will always be the last value in a composite type, we only care
      // about that value.
      Object nativeId = fields[fields.length - 1];

      results.add(context.find(context.newObjectId(targetClass, nativeId),
          true, null));

    }

    // now load all the objects from the ids we were given.

    stateManager.storeObject(fieldId, results);

  }

  /**
   * Create a SliceQuery for this collection
   * 
   * @param objectId
   * @param keyspace
   * @param count
   * @return
   */
  public SliceQuery<byte[], Composite, byte[]> createQuery(Object objectId,
      Keyspace keyspace, String columnFamilyName, int count) {

    // undefined value set it to something realistic
    if (count < 0) {
      count = DEFAULT_FETCH_SIZE;
    }

    SliceQuery<byte[], Composite, byte[]> query = new ThriftSliceQuery(
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
      Collection<?> objects, Set<HColumn<Composite, byte[]>> orders,
      Set<HColumn<Composite, byte[]>> keys, long clock) {

    StoreContext ctx = stateManager.getContext();

    Composite orderComposite = null;
    Composite idComposite = null;

    for (Object current : objects) {

      Object currentId = mappingUtils.getTargetObject(ctx.getObjectId(current));

      // create our composite of the format of id+order*
      idComposite = new Composite();

      // create our composite of the format order*+id
      orderComposite = new Composite();

         // add our id to the beginning of our id based composite
      idComposite.add(currentId);

      // now construct the composite with order by the ids at the end.
      for (OrderField order : orderBy) {
        order.addField(idComposite, current);
        order.addField(orderComposite, current);
      }

      // add our id to the end of our order based composite
      orderComposite.add(currentId);

      // add our order based column to the columns
      orders.add(new HColumnImpl<Composite, byte[]>(orderComposite, HOLDER,
          clock, compositeSerializer, BytesArraySerializer.get()));

      // add our key based column to the key columns
      keys.add(new HColumnImpl<Composite, byte[]>(idComposite, HOLDER, clock,
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
    private Serializer<?> serializer;
    private int targetFieldIndex;

    public OrderField(Order order, FieldMetaData fmd) {
      super();
      this.order = order;

      FieldMetaData targetField = fmd.getMappedByField(fmd.getElement()
          .getTypeMetaData(), order.getName());

      this.serializer = MappingUtils.getSerializer(targetField);

      this.targetFieldIndex = targetField.getIndex();

    }

    protected void addField(Composite composite, Object instance) {

      if (instance == null) {
        return;
      }

      if (!(instance instanceof PersistenceCapable)) {
        throw new MetaDataException(
            String
                .format(
                    "You specified class '%s' as your collection element but it does not implement %s",
                    instance.getClass(), PersistenceCapable.class));
      }

      OpenJPAStateManager stateManager = (OpenJPAStateManager) ((PersistenceCapable) instance)
          .pcGetStateManager();
      Object value = stateManager.fetch(targetFieldIndex);

      composite.add(value);

    }
  }

}
