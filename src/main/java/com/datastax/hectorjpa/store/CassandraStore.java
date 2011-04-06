package com.datastax.hectorjpa.store;

import java.util.BitSet;

import javax.persistence.Entity;
import javax.persistence.MappedSuperclass;

import me.prettyprint.cassandra.model.MutatorImpl;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.util.MetaDataException;
import org.apache.openjpa.util.OpenJPAId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.MetaDataAccessException;

import com.datastax.hectorjpa.consitency.JPAConsistency;
import com.datastax.hectorjpa.service.InMemoryIndexingService;
import com.datastax.hectorjpa.service.IndexQueue;
import com.datastax.hectorjpa.service.IndexingService;

/**
 * Holds the {@link Cluster} and {@link Keyspace} references needed for
 * accessing Cassandra
 * 
 * @author zznate
 * @author Todd Nine
 */
public class CassandraStore {

  private static final Logger log = LoggerFactory
      .getLogger(CassandraStore.class);

  // bitset used on deleting fields
  private static final BitSet NONE = new BitSet();

  private final Cluster cluster;
  private final CassandraStoreConfiguration conf;
  private Keyspace keyspace;
  private IndexingService indexingService;

  public CassandraStore(CassandraStoreConfiguration conf) {
    this.conf = conf;
    
    //need to create actual connection
        
    String clusterName =  conf.getValue(EntityManagerConfigurator.CLUSTER_NAME_PROP).getOriginalValue();
    String clusterConnection =  conf.getValue(EntityManagerConfigurator.HOST_LIST_PROP).getOriginalValue();
    
    this.cluster = HFactory.getOrCreateCluster(clusterName, new CassandraHostConfigurator(clusterConnection));
      
   
    // TODO needs passthrough of other configuration

    //TODO TN ugly as sin, fix this!
    String value = conf.getValue(EntityManagerConfigurator.SERIALIZER_PROP).getOriginalValue();
    
    conf.setSerializer(value);
    
    this.indexingService = new InMemoryIndexingService(this);
   
  }

  public CassandraStore open() {
    this.keyspace = HFactory.createKeyspace(
        conf.getValue(EntityManagerConfigurator.KEYSPACE_PROP)
            .getOriginalValue(), cluster);

    this.keyspace.setConsistencyLevelPolicy(new JPAConsistencyPolicy());

    return this;
  }

  /**
   * Create a clock value to be passed to all operations
   * 
   * @return
   */
  public long getClock() {
    return keyspace.createClock();
  }

  /**
   * Return a new mutator for the keyspace with a byte array.
   * 
   * @return
   */
  public Mutator<byte[]> createMutator() {
    return new MutatorImpl<byte[]>(keyspace, BytesArraySerializer.get());
  }

  public Keyspace getKeyspace() {
    return this.keyspace;
  }
  
  public IndexingService getIndexingService(){
    return this.indexingService;
  }

  /**
   * Load this object for the statemanager
   * 
   * @param stateManager
   * @param fields
   *          The bitset of fields to load
   * @return true if the object was found, false otherwise
   */
  public boolean getObject(OpenJPAStateManager stateManager, BitSet fields) {

    ClassMetaData metaData = stateManager.getMetaData();
    EntityFacade entityFacade = conf.getMetaCache().getFacade(metaData,
        conf.getSerializer());

    return entityFacade.loadColumns(stateManager, fields, keyspace,
        conf.getMetaCache());

  }

  /**
   * Load this object for the statemanager
   * 
   * @param stateManager
   * @param fields
   *          The bitset of fields to load
   * @return true if the object was found, false otherwise
   */
  public boolean exists(OpenJPAStateManager stateManager) {

    ClassMetaData metaData = stateManager.getMetaData();
    EntityFacade entityFacade = conf.getMetaCache().getFacade(metaData,
        conf.getSerializer());

    return entityFacade.exists(stateManager, keyspace, conf.getMetaCache());

  }

  /**
   * Find the persisted class in cassandra. If this is not a subclass, the oid
   * is always returned
   * 
   * @param oid
   * @return
   */
  public Class<?> getDataStoreId(Object oid, StoreContext ctx) {

    //If there's no id there's nothing to do, return null
    if(((OpenJPAId)oid).getIdObject() == null){
      return null;
    }
    
    Class<?> requested = ((OpenJPAId) oid).getType();

    ClassMetaData metaData = ctx.getConfiguration()
        .getMetaDataRepositoryInstance()
        .getMetaData(requested, ctx.getClassLoader(), true);

    EntityFacade entityFacade = conf.getMetaCache().getFacade(metaData,
        conf.getSerializer());
    
    if(entityFacade == null){
      throw new MetaDataException(String.format("You attempted to load an object that has no mapping.  Please check that class %s is mapped with a %s annotation, not a %s annoation", metaData.getDescribedType(), Entity.class.getName(), MappedSuperclass.class.getName()));
    }

    return entityFacade.getStoredEntityType(oid, keyspace, conf.getMetaCache());
  }

  /**
   * Store this object using the given mutator
   * 
   * @param mutator
   * @param stateManager
   * @param fields
   * @param queue 
   * @return
   */
  public void storeObject(Mutator mutator, OpenJPAStateManager stateManager,
      BitSet fields, long clock, IndexQueue queue) {

    if (log.isDebugEnabled()) {
      log.debug("Adding mutation (insertion) for class {}", stateManager
          .getManagedInstance().getClass().getName());
    }

    ClassMetaData metaData = stateManager.getMetaData();
    EntityFacade entityFacade = conf.getMetaCache().getFacade(metaData,
        conf.getSerializer());

    entityFacade.addColumns(stateManager, fields, mutator, clock, queue);

  }

  /**
   * Remove this object
   * 
   * @param mutator
   * @param stateManager
   * @param queue 
   * @return
   */
  public void removeObject(Mutator mutator, OpenJPAStateManager stateManager,
      long clock, IndexQueue queue) {

    if (log.isDebugEnabled()) {
      log.debug("Adding mutation (deletion) for class {}", stateManager
          .getManagedInstance().getClass().getName());
    }
    ClassMetaData metaData = stateManager.getMetaData();

    // TODO get collections to remove as well

    EntityFacade entityFacade = conf.getMetaCache().getFacade(metaData,
        conf.getSerializer());

    entityFacade.delete(stateManager, mutator, clock);

  }

  /**
   * Inner class that delegates to the JPAConsistency value for consistency
   * 
   * @author Todd Nine
   * 
   */
  private class JPAConsistencyPolicy implements ConsistencyLevelPolicy {

    @Override
    public HConsistencyLevel get(OperationType op) {
      return JPAConsistency.get();
    }

    @Override
    public HConsistencyLevel get(OperationType op, String cfName) {
      return JPAConsistency.get();
    }
  }

}
