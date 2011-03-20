/**
 * 
 */
package com.datastax.hectorjpa.index;


/**
 * Represents a many relationship to another root entity. I.E the referenced
 * entity is stored in it's own CF.
 * 
 * @author Todd Nine
 * 
 */
public class ManyEntityIndex extends AbstractEntityIndex {

//  private static final Logger log = LoggerFactory.getLogger(EntityFacade.class);
//  
//  private static final byte[] DEFAULT_VALUE = new byte[]{0};
//  
//  private static final String DEFAULT_FIELD = "natural";
//  
//  private static final BytesArraySerializer SERIALIZER = BytesArraySerializer.get();
//  
//  
//  private final MappingUtils mappingUtils;
//  
//  private Index defaultIndex;
//  
//  
//
//  /**
//   * 
//   * @param fieldNumber
//   *          The absolute position of this field in the class meta data
//   */
//  public ManyEntityIndex(FieldMetaData fmd, MappingUtils mapping) {
//    super(fmd);
//
//    mappingUtils = mapping;
//    // TODO TN get fields we'll be querying on and create indexes here.
//
//    // always create a default index that is the other entities
//    // create the default collection index
//    defaultIndex = new Index();
//
//    defaultIndex.addIndexField(new StaticField<byte[]>(DEFAULT_VALUE, SERIALIZER, DEFAULT_FIELD));
//    
//    Order[] orders = fmd.getOrders();
//
//    if (orders != null) {
//      for (Order order : orders) {
//        defaultIndex.addOrderField(new OrderField(order, fmd));
//      }
//    }
//    
//
//    addIndex(defaultIndex);
//
//  }
//
//  @Override
//  public void loadIndex(OpenJPAStateManager stateManager, Keyspace keyspace) {
//    
//    Object target = mappingUtils.getTargetObject(stateManager.getObjectId());
//    
//    Serializer<Object> s =  mappingUtils.getSerializer(target);
//    
//    ContainerCollection<Object> container = new ContainerCollection<Object>(target, s, name);
//    
//    int size  = stateManager.getContext().getFetchConfiguration().getFetchBatchSize() < 1 ? 100 : stateManager.getContext().getFetchConfiguration().getFetchBatchSize(); 
//    
//    List<?> results =  IndexedCollections.searchContainer(keyspace, container, DEFAULT_FIELD, null, null, null, size, false, DEFAULT_CF_SET, defaultIndex.getIndexedFields().get(0).getSerializer(), StringSerializer.get());
//    
//    //TODO TN, loop through and load objects into proxy.
//    
//    stateManager.storeObject(fieldIndex, null);
//    
//  
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see
//   * com.datastax.hectorjpa.relation.AbstractEntityIndex#writeIndex(org.apache
//   * .openjpa.kernel.OpenJPAStateManager)
//   */
//  @Override
//  public void writeIndex(OpenJPAStateManager stateManager, Keyspace keyspace) {
//
//    // TODO, will this only load from l1 cache (I.E. what's changed) or will
//    // this load everything
//    // we only want values from the scope of this transaction
//    Object value = stateManager.fetch(fieldIndex);
//
//    if (value == null) {
//      return;
//    }
//
//    // currently only doing collections
//    if (!(value instanceof Collection)) {
//      log.warn("Only collections are currently supported");
//      return;
//    }
//
//    // it's an instance of a proxy, use the change tracker to perform the op
//    if (value instanceof Proxy) {
//      return;
//    }
//
//    // finally, just add all entities since it's a collection.
//    addEntries((Collection<?>) value,
//        stateManager.getMetaData().getField(fieldIndex),
//        stateManager.getContext());
//
//  }
//
//  private void addEntries(Collection<?> values, FieldMetaData fmd,
//      StoreContext ctx) {
//
//    for (Object entity : (Collection<?>) values) {
//      // get the id of the object
//
//      Object id = ctx.getObjectId(entity);
//
//      // TODO get all properties that have been defined in the other value as
//      // indexed/searchable in target object
//
//      // get all Order properties no the field and add the values from the
//      // target objects
//
//    }
//  }
//
//  
//
//  @Override
//  public void deleteIndex(OpenJPAStateManager stateManager, Keyspace keyspace) {
//    // TODO Auto-generated method stub
//    
//  }
//  



}
