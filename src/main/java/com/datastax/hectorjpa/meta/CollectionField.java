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

    Set<HColumn<Composite, byte[]>> newOrderColumns = new HashSet<HColumn<Composite, byte[]>>();
    Set<HColumn<Composite, byte[]>> newIdColumns = new HashSet<HColumn<Composite, byte[]>>();
    Set<HColumn<Composite, byte[]>> deletedColumns = new HashSet<HColumn<Composite, byte[]>>();
    Set<HColumn<Composite, byte[]>> deletedIdColumns = new HashSet<HColumn<Composite, byte[]>>();

    // not a proxy, it's the first time this has been saved
    if (field instanceof Proxy) {

      Proxy proxy = (Proxy) field;
      ChangeTracker changes = proxy.getChangeTracker();

      createColumns(stateManager, changes.getAdded(), newOrderColumns, newIdColumns,  clock);

      // TODO TN need to get the original value to delete old index on change
      createColumns(stateManager, changes.getChanged(), newOrderColumns, newIdColumns,  clock);

      // add everything that needs removed
      createColumns(stateManager, changes.getRemoved(), deletedColumns, deletedIdColumns, clock);
    }
    // new item that hasn't been proxied, just write them as new columns
    else {
      createColumns(stateManager, (Collection<?>) field, newOrderColumns ,newIdColumns,  clock);
    }

    // construct the key
    byte[] collectionKey = constructKey(key);

    // write our updates and out deletes
    for (HColumn<Composite, byte[]> current : deletedColumns) {

      // same column exists on write, don't issue the delete since we don't want
      // to remove the write
      if (newOrderColumns.contains(current)) {
        continue;
      }

      mutator.addDeletion(collectionKey, CF_NAME, current.getName(),
          compositeSerializer, clock);
    }
    
    for (HColumn<Composite, byte[]> current : deletedColumns) {

      // same column exists on write, don't issue the delete since we don't want
      // to remove the write
      if (newIdColumns.contains(current)) {
        continue;
      }

      mutator.addDeletion(collectionKey, CF_NAME, current.getName(),
          compositeSerializer, clock);
    }

    // write our order updates
    for (HColumn<Composite, byte[]> current : newOrderColumns) {

      mutator.addInsertion(collectionKey, CF_NAME, current);
    }
    
    // write our key updates
    for (HColumn<Composite, byte[]> current : newIdColumns) {

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
    
    //TODO TN use our CollectionProxy here
    List<Object> results = new ArrayList<Object>(result.get().getColumns().size());
   

    for (HColumn<Composite, byte[]> col : result.get().getColumns()) {
      fields = col.getName().toArray();

      //the id will always be the last value in a composite type, we only care about that value.
      Object nativeId = fields[fields.length-1];
      
      results.add(context.find(context.newObjectId(targetClass, nativeId), true, null));
       
    }
    
    //now load all the objects from the ids we were given.
   
    
    
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
   * @param orders
   * @param clock
   */
  private void createColumns(OpenJPAStateManager stateManager,
      Collection<?> objects, Set<HColumn<Composite, byte[]>> orders, Set<HColumn<Composite, byte[]>> keys, long clock) {

    StoreContext ctx = stateManager.getContext();
    
    Composite orderComposite = null;
    Composite idComposite = null;

    for (Object current : objects) {

      Object currentId = mappingUtils.getTargetObject(ctx.getObjectId(current));

      
      //create our composite of the format of id+order*
      idComposite = new Composite();
      
      //create our composite of the format order*+id
      orderComposite = new Composite();
      
      byte[] idbytes = mappingUtils.getSerializer(currentId).toBytes(currentId);
      
      //add our id to the beginning of our id based composite
      idComposite.addBytes(idbytes);
     
      // now construct the composite with order by the ids at the end.
      for (OrderField order : orderBy) {
        order.addField(idComposite, current);
        order.addField(orderComposite, current);
      }
      
      //add our id to the end of our order based composite
      orderComposite.addBytes(idbytes);
      
      //add our order based column to the columns
      orders.add(new HColumnImpl<Composite, byte[]>(orderComposite, HOLDER, clock,
          compositeSerializer, BytesArraySerializer.get()));
      
      //add our key based column to the key columns
      keys.add(new HColumnImpl<Composite, byte[]>(idComposite, HOLDER, clock,
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
