package com.datastax.hectorjpa.store;

import java.util.BitSet;

import me.prettyprint.cassandra.model.MutatorImpl;
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
 */
public class CassandraStore {

  private static final Logger log = LoggerFactory
      .getLogger(CassandraStore.class);

  // bitset used on deleting fields
  private static final BitSet NONE = new BitSet();

  private final Cluster cluster;
  private final CassandraStoreConfiguration conf;
  private Keyspace keyspace;
  private MappingUtils mappingUtils;
  private MetaCache metaCache;

  public CassandraStore(CassandraStoreConfiguration conf) {
    this.conf = conf;
    this.cluster = HFactory.getCluster(conf.getValue(
        EntityManagerConfigurator.CLUSTER_NAME_PROP).getOriginalValue());
    // TODO needs passthrough of other configuration
    mappingUtils = new MappingUtils();
    metaCache = new MetaCache(mappingUtils);
  }

  public CassandraStore open() {
    this.keyspace = HFactory.createKeyspace(
        conf.getValue(EntityManagerConfigurator.KEYSPACE_PROP)
            .getOriginalValue(), cluster);
    return this;
  }

  /**
   * 
   * @param stateManager
   * @param fields
   *          The bitset of fields to load
   * @return true if the object was found, false otherwise
   */
  public boolean getObject(OpenJPAStateManager stateManager, BitSet fields) {

    ClassMetaData metaData = stateManager.getMetaData();
    EntityFacade entityFacade = metaCache.getFacade(metaData);
    Object idObj = stateManager.getId();

    
    String[] columns = entityFacade.getCfColumns(fields);
    
    SliceQuery<byte[], String, byte[]> sliceQuery = mappingUtils.buildSliceQuery(idObj, columns, entityFacade.getColumnFamilyName(), keyspace);

    // stateManager.storeObject(0, idObj);

    QueryResult<ColumnSlice<String, byte[]>> result = sliceQuery.execute();

    entityFacade.readColumnResults(stateManager, result, fields);
    
    return result.get().getColumns().size() > 0;
  }

  public Mutator storeObject(Mutator mutator, OpenJPAStateManager stateManager,  BitSet fields) {
    if (mutator == null)
      mutator = new MutatorImpl(keyspace, BytesArraySerializer.get());
    if (log.isDebugEnabled()) {
      log.debug("Adding mutation (insertion) for class {}", stateManager
          .getManagedInstance().getClass().getName());
    }

    ClassMetaData metaData = stateManager.getMetaData();
    EntityFacade entityFacade = metaCache.getFacade(metaData);
    
    long clock = keyspace.createClock();
    
    entityFacade.addColumns(stateManager, fields, mutator, clock);
    
    return mutator;
  }
  

  public Mutator removeObject(Mutator mutator, OpenJPAStateManager stateManager) {
    if (mutator == null)
      mutator = new MutatorImpl(keyspace, BytesArraySerializer.get());
    if (log.isDebugEnabled()) {
      log.debug("Adding mutation (deletion) for class {}", stateManager
          .getManagedInstance().getClass().getName());
    }
    ClassMetaData metaData = stateManager.getMetaData();
   

    //TODO get collections to remove as well
    
    EntityFacade entityFacade = metaCache.getFacade(metaData);
    
    mutator.addDeletion(mappingUtils.getKeyBytes(stateManager.getObjectId()),
        entityFacade.getColumnFamilyName(), null, StringSerializer.get());
    
    return mutator;
  }

  public Cluster getCluster() {
    return cluster;
  }

  public Keyspace getKeyspace() {
    return keyspace;
  }

}
