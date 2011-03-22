package com.datastax.hectorjpa.store;

import java.util.BitSet;

import me.prettyprint.cassandra.model.MutatorImpl;
import me.prettyprint.cassandra.model.thrift.ThriftSliceQuery;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.meta.MetaCache;

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

  public CassandraStore(CassandraStoreConfiguration conf) {
    this.conf = conf;
    this.cluster = HFactory.getCluster(conf.getValue(
        EntityManagerConfigurator.CLUSTER_NAME_PROP).getOriginalValue());
    // TODO needs passthrough of other configuration

  }

  public CassandraStore open() {
    this.keyspace = HFactory.createKeyspace(
        conf.getValue(EntityManagerConfigurator.KEYSPACE_PROP)
            .getOriginalValue(), cluster);
    return this;
  }

  /**
   * Create a clock value to be passed to all operations
   * @return
   */
  public long getClock(){
    return keyspace.createClock();
  }
  
  /**
   * Return a new mutator for the keyspace with a byte array.
   * @return
   */
  public Mutator createMutator(){
    return new MutatorImpl(keyspace, BytesArraySerializer.get());
  }
  
  /**
   * Load this object for the statemanager
   * @param stateManager
   * @param fields
   *          The bitset of fields to load
   * @return true if the object was found, false otherwise
   */
  public boolean getObject(OpenJPAStateManager stateManager, BitSet fields) {

    ClassMetaData metaData = stateManager.getMetaData();
    EntityFacade entityFacade = conf.getMetaCache().getFacade(metaData);

    return entityFacade.loadColumns(stateManager, fields, keyspace);


  }

  /**
   * Store this object using the given mutator
   * @param mutator
   * @param stateManager
   * @param fields
   * @return
   */
  public void storeObject(Mutator mutator, OpenJPAStateManager stateManager,
      BitSet fields, long clock) {


    if (log.isDebugEnabled()) {
      log.debug("Adding mutation (insertion) for class {}", stateManager
          .getManagedInstance().getClass().getName());
    }

    ClassMetaData metaData = stateManager.getMetaData();
    EntityFacade entityFacade = conf.getMetaCache().getFacade(metaData);

    entityFacade.addColumns(stateManager, fields, mutator, clock);

  }

  /**
   * Remove this object
   * @param mutator
   * @param stateManager
   * @return
   */
  public void removeObject(Mutator mutator, OpenJPAStateManager stateManager, long clock) {
   

    if (log.isDebugEnabled()) {
      log.debug("Adding mutation (deletion) for class {}", stateManager
          .getManagedInstance().getClass().getName());
    }
    ClassMetaData metaData = stateManager.getMetaData();

    // TODO get collections to remove as well

    EntityFacade entityFacade = conf.getMetaCache().getFacade(metaData);

    entityFacade.delete(stateManager, mutator, clock);

  }



}
