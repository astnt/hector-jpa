package com.datastax.hectorjpa.store;

import java.util.BitSet;
import java.util.Map;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.model.MutatorImpl;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.index.AbstractEntityIndex;
import com.datastax.hectorjpa.meta.ColumnMeta;

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

  public CassandraStore(CassandraStoreConfiguration conf) {
    this.conf = conf;
    this.cluster = HFactory.getCluster(conf.getValue(
        EntityManagerConfigurator.CLUSTER_NAME_PROP).getOriginalValue());
    // TODO needs passthrough of other configuration
    mappingUtils = new MappingUtils();
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
    EntityFacade entityFacade = new EntityFacade(metaData, fields, mappingUtils);
    Object idObj = stateManager.getId();

    SliceQuery<byte[], String, byte[]> sliceQuery = mappingUtils
        .buildSliceQuery(idObj, entityFacade, keyspace);

    // stateManager.storeObject(0, idObj);

    QueryResult<ColumnSlice<String, byte[]>> result = sliceQuery.execute();

    for (Map.Entry<String, ColumnMeta<?>> entry : entityFacade.getColumnMeta()
        .entrySet()) {
      HColumn<String, byte[]> column = result.get().getColumnByName(
          entry.getKey());
      if (column != null)

        entry.getValue().storeObject(stateManager, column);
    }

    // now get all associations
    for (AbstractEntityIndex index : entityFacade.getIndexMetaData()) {
      index.loadIndex(stateManager, keyspace);
    }

    return result.get().getColumns().size() > 0;
  }

  public Mutator storeObject(Mutator mutator, OpenJPAStateManager stateManager,
      BitSet fields) {
    if (mutator == null)
      mutator = new MutatorImpl(keyspace, BytesArraySerializer.get());
    if (log.isDebugEnabled()) {
      log.debug("Adding mutation (insertion) for class {}", stateManager
          .getManagedInstance().getClass().getName());
    }

    ClassMetaData metaData = stateManager.getMetaData();
    EntityFacade entityFacade = new EntityFacade(metaData, fields, mappingUtils);
    Object field;

    Object id = stateManager.getObjectId();

    for (Map.Entry<String, ColumnMeta<?>> entry : entityFacade.getColumnMeta()
        .entrySet()) {

      field = entry.getValue().fetchField(stateManager);

      if (field != null) {
        mutator.addInsertion(mappingUtils.getKeyBytes(id), entityFacade
            .getColumnFamilyName(), new HColumnImpl(entry.getKey(), field,
            keyspace.createClock(), StringSerializer.get(), entry.getValue()
                .getSerializer()));
      }

    }

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
    // use empty bitset
    EntityFacade entityFacade = new EntityFacade(metaData, NONE, mappingUtils);
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
