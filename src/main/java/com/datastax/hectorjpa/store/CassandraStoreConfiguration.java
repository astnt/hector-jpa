package com.datastax.hectorjpa.store;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.openjpa.conf.OpenJPAConfigurationImpl;
import org.apache.openjpa.lib.conf.ProductDerivations;
import org.apache.openjpa.util.UserException;

import com.datastax.hectorjpa.consitency.JPAConsistency;
import com.datastax.hectorjpa.meta.MetaCache;
import com.datastax.hectorjpa.serialize.EmbeddedSerializer;
import com.datastax.hectorjpa.serialize.JavaSerializer;
import com.datastax.hectorjpa.service.IndexingService;
import com.datastax.hectorjpa.service.SyncInMemoryIndexingService;

public class CassandraStoreConfiguration extends OpenJPAConfigurationImpl {

  public static final String PROP_PREFIX = "me.prettyprint.hom.";
  public static final String CLASSPATH_PREFIX_PROP = PROP_PREFIX
      + "classpathPrefix";
  public static final String CLUSTER_NAME_PROP = PROP_PREFIX + "clusterName";
  public static final String KEYSPACE_PROP = PROP_PREFIX + "keyspace";
  public static final String HOST_LIST_PROP = PROP_PREFIX + "hostList";

  public static final String SERIALIZER_PROP = "com.datastax.jpa.embeddedserializer";
  public static final String INDEXING_PROP = "com.datastax.jpa.indexservice";

  private MetaCache metaCache;

  private EmbeddedSerializer serializer;

  private IndexingService indexingService;

  private Cluster cluster;
  private Keyspace keyspace;

  public CassandraStoreConfiguration() {
    super(false, false);

    // override the default and the current value of lock manager plugin
    // from our superclass to use the single-jvm lock manager
    lockManagerPlugin.setDefault("version");
    lockManagerPlugin.setString("version");
    addString(CLASSPATH_PREFIX_PROP);
    addString(KEYSPACE_PROP);
    addString(CLUSTER_NAME_PROP);
    addString(HOST_LIST_PROP);
    addString(SERIALIZER_PROP);
    addString(INDEXING_PROP);

    // TODO map our metadata plugin parser factory for parsing index
    // annotations
    // metaFactoryPlugin.setClassName(CassandraPersistenceMappingParser.class.getName());

    ProductDerivations.beforeConfigurationLoad(this);
    loadGlobals();

    this.metaCache = new MetaCache();

  }

  /**
   * Get the serializer configured
   * 
   * @return
   */
  public EmbeddedSerializer getSerializer() {
    if (serializer != null) {
      return serializer;
    }

    String className = getValue(SERIALIZER_PROP).getOriginalValue();

    try {
      serializer = (EmbeddedSerializer) createInstance(className,
          JavaSerializer.class.getName());
    } catch (Exception e) {
      throw new UserException(String.format(
          "Unable to load class '%s' as an instance of %s", className,
          EmbeddedSerializer.class), e);
    }

    return serializer;
  }

  /**
   * @return the cache
   */
  public MetaCache getMetaCache() {
    return metaCache;
  }

  /**
   * @return the indexingService
   */
  public IndexingService getIndexingService() {
    if (indexingService != null) {
      return indexingService;
    }

    String className = getValue(INDEXING_PROP).getString();

    try {
      this.indexingService = (IndexingService) createInstance(className,
          SyncInMemoryIndexingService.class.getName());
    } catch (Exception e) {
      throw new UserException(String.format(
          "Unable to load class '%s' as an instance of %s", className,
          IndexingService.class), e);
    }

    this.indexingService.postCreate(this);

    return this.indexingService;
  }

  /**
   * @return the cluster
   */
  public Cluster getCluster() {
    if (cluster != null) {
      return cluster;
    }

    String clusterName = getValue(CLUSTER_NAME_PROP).getString();

    String clusterConnection = getValue(HOST_LIST_PROP).getString();

    cluster = HFactory.getOrCreateCluster(clusterName,
        new CassandraHostConfigurator(clusterConnection));

    return cluster;
  }

  /**
   * @return the keyspace
   */
  public Keyspace getKeyspace() {
    if (keyspace != null) {
      return keyspace;
    }

    keyspace = HFactory.createKeyspace(getValue(KEYSPACE_PROP).getString(),
        getCluster());

    keyspace.setConsistencyLevelPolicy(new JPAConsistencyPolicy());

    return keyspace;

  }

  /**
   * Create an instance of the object set from the given prop key. If one is not
   * preset, defaultClass will be used
   * 
   * @param propKey
   * @param defaultClass
   * @return
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  private Object createInstance(String className, String defaultClass)
      throws ClassNotFoundException, InstantiationException,
      IllegalAccessException {

    if (className == null) {
      className = defaultClass;
    }

    Class<?> clazz = Class.forName(className);

    return clazz.newInstance();

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
