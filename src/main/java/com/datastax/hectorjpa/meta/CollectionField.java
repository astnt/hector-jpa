/**
 * 
 */
package com.datastax.hectorjpa.meta;

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
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.Order;
import org.apache.openjpa.util.ChangeTracker;
import org.apache.openjpa.util.MetaDataException;
import org.apache.openjpa.util.Proxy;

import com.datastax.hectorjpa.proxy.CollectionProxy;
import com.datastax.hectorjpa.store.MappingUtils;
import compositecomparer.Composite;
import compositecomparer.hector.CompositeSerializer;

/**
 * A class that performs all operations related to collections for saving and
 * updating
 * 
 * @author Todd Nine
 * 
 */
public class CollectionField<V> extends Field<V> {

  public static final String CF_NAME = "Collection_Container";

  private static byte[] HOLDER = new byte[] { 0 };

  //the default batch size when it hasn't been set into the context
  private int DEFAULT_FETCH_SIZE = 100;
  

  private static final CompositeSerializer compositeSerializer = new CompositeSerializer();

  private OrderField[] orderBy;
  private String name;
  private Serializer<?> targetIdSerializer;
  private Class<?> targetClass;
  private MappingUtils mappingUtils;

  // The name of this entity serialzied as bytes
  private byte[] entityName;

  // the name of the field serialzied as bytes
  private byte[] fieldName;

  public CollectionField(FieldMetaData fmd, MappingUtils mappingUtils) {
    super(fmd.getIndex());
    
    this.mappingUtils = mappingUtils;
    
    Class<?> clazz = fmd.getDeclaredType();

    if (!Collection.class.isAssignableFrom(clazz)) {
      throw new MetaDataException("Only collections are currently supported");
    }
    

    this.name = fmd.getName();

    Order[] orders = fmd.getOrders();
    orderBy = new OrderField[orders.length];

    // create all our order by clauses
    for (int i = 0; i < orders.length; i++) {
      orderBy[i] = new OrderField(orders[i], fmd);
    }

    ClassMetaData elementClassMeta = fmd.getElement().getDeclaredTypeMetaData();
    
    //set the class of the collection elements
    targetClass = elementClassMeta.getDescribedType();

    
    targetIdSerializer = MappingUtils.getSerializer(elementClassMeta
        .getPrimaryKeyFields()[0]);

    // create our cached bytes for better performance
    fieldName = StringSerializer.get().toBytes(name);

    //write our column family name of the owning side to our rowkey for scanning
    String columnFamilyName = mappingUtils.getColumnFamily(fmd.getDeclaringType());

    entityName = StringSerializer.get().toBytes(columnFamilyName);

  }

  @Override
  public void addField(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock, byte[] key, String cfName) {

    Object field = stateManager.fetch(fieldId);

    // nothing to do
    if (field == null) {
      return;
    }

    Set<HColumn<Composite, byte[]>> newColumns = new HashSet<HColumn<Composite, byte[]>>();
    Set<HColumn<Composite, byte[]>> deletedColumns = new HashSet<HColumn<Composite, byte[]>>();

    // not a proxy, it's the first time this has been saved
    if (field instanceof Proxy) {

      Proxy proxy = (Proxy) field;
      ChangeTracker changes = proxy.getChangeTracker();

      createColumns(stateManager, changes.getAdded(), newColumns, clock);

      // TODO TN need to get the original value to delete old index on change
      createColumns(stateManager, changes.getChanged(), newColumns, clock);

      // add everything that needs removed
      createColumns(stateManager, changes.getRemoved(), deletedColumns, clock);
    }
    // new item that hasn't been proxied, just write them as new columns
    else {
      createColumns(stateManager, (Collection<?>) field, newColumns, clock);
    }

    // construct the key
    byte[] collectionKey = constructKey(key);

    // write our updates and out deletes
    for (HColumn<Composite, byte[]> current : deletedColumns) {

      // same column exists on write, don't issue the delete since we don't want
      // to remove the write
      if (newColumns.contains(current)) {
        continue;
      }

      mutator.addDeletion(collectionKey, CF_NAME, current.getName(),
          compositeSerializer, clock);
    }

    // write our updates
    for (HColumn<Composite, byte[]> current : deletedColumns) {

      mutator.addInsertion(collectionKey, CF_NAME, current);
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
    
    //fire up our proxy on load
    CollectionProxy proxy = new CollectionProxy(targetClass, true, this);
    proxy.stopTracking();
   
   

    for (HColumn<Composite, byte[]> col : result.get().getColumns()) {
      fields = col.getName().toArray();

      //the id will always be the last value in a composite type, we only care about that value.
      Object nativeId = fields[fields.length-1];
     
      //load our entity from cassandra
      proxy.add(context.find(context.newObjectId(targetClass, nativeId), true, null));
     
      
    }
    
    //now load all the objects from the ids we were given.

    //start tracking the proxy now that we have our intial result
    proxy.startTracking();
   
    
    
    stateManager.storeObject(fieldId, proxy);

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

    //undefined value set it to something realistic
    if(count < 0){
      count = DEFAULT_FETCH_SIZE;
    }
    
    SliceQuery<byte[], Composite, byte[]> query = new ThriftSliceQuery(
        keyspace, BytesArraySerializer.get(), compositeSerializer,
        BytesArraySerializer.get());

    query.setRange(null, null, false, count);
    query.setKey(constructKey(mappingUtils.getKeyBytes(objectId)));
    query.setColumnFamily(columnFamilyName);
    return query;

  }

  /**
   * Create columns and add them to the collection of columns.
   * 
   * @param stateManager
   * @param objects
   * @param columns
   * @param clock
   */
  private void createColumns(OpenJPAStateManager stateManager,
      Collection<?> objects, Set<HColumn<Composite, byte[]>> columns, long clock) {

    StoreContext ctx = stateManager.getContext();

    for (Object current : objects) {

      Object currentId = mappingUtils.getTargetObject(ctx.getObjectId(current));

      Composite composite = new Composite();

      // now construct the composite with order by the ids at the end.
      for (OrderField order : orderBy) {
        order.addField(composite, current);
      }

      composite.add(currentId);

      columns.add(new HColumnImpl<Composite, byte[]>(composite, HOLDER, clock,
          compositeSerializer, BytesArraySerializer.get()));

    }

  }

  /**
   * Create our key byte array
   * 
   * @param entityIdBytes
   * @return
   */
  private byte[] constructKey(byte[] entityIdBytes) {

    byte[] key = new byte[entityName.length + fieldName.length
        + entityIdBytes.length];

    ByteBuffer buff = ByteBuffer.wrap(key);

    buff.put(entityName);
    buff.put(entityIdBytes);
    buff.put(fieldName);

    return key;

  }

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

      // TODO, this will most likely need moved somewhere that is common access
      // for both order and index fields
      // since both could potentially be recursive

      // String name = order.getName();
      //
      // String[] props = name.split(".");
      //
      // fieldIds = new int[props.length];
      //
      // ClassMetaData current = fmd.getDeclaredTypeMetaData();
      //
      // FieldMetaData meta = null;
      //
      // for (int i = 0; i < props.length; i++) {
      // meta = current.getField(props[i]);
      //
      // // user has a value we can't find the field
      // if (meta == null) {
      // throw new MetaDataException(
      // String
      // .format(
      // "Could not find the field with name '%s' on class '%s' in the order clause '%s'",
      // props[i], current.getDescribedType().getName(), name));
      // }
      //
      // fieldIds[i] = meta.getIndex();
      //
      // current = meta.getDeclaredTypeMetaData();
      //
      // }

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
