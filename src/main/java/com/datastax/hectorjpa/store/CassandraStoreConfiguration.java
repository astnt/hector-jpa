package com.datastax.hectorjpa.store;

import org.apache.openjpa.conf.OpenJPAConfigurationImpl;
import org.apache.openjpa.lib.conf.ProductDerivations;
import org.apache.openjpa.util.UserException;

import com.datastax.hectorjpa.meta.MetaCache;
import com.datastax.hectorjpa.serialize.EmbeddedSerializer;
import com.datastax.hectorjpa.serialize.JavaSerializer;
import com.datastax.hectorjpa.service.InMemoryIndexingService;
import com.datastax.hectorjpa.service.IndexingService;

public class CassandraStoreConfiguration extends OpenJPAConfigurationImpl {

  private MetaCache metaCache;

  private EmbeddedSerializer serializer;
  
  private IndexingService indexingService;

  public CassandraStoreConfiguration() {
    super(false, false);

    // override the default and the current value of lock manager plugin
    // from our superclass to use the single-jvm lock manager
    lockManagerPlugin.setDefault("version");
    lockManagerPlugin.setString("version");
    addString("me.prettyprint.hom.classpathPrefix");
    addString("me.prettyprint.hom.keyspace");
    addString("me.prettyprint.hom.clusterName");
    addString("me.prettyprint.hom.hostList");
    addString(EntityManagerConfigurator.SERIALIZER_PROP);
   

    // TODO map our metadata plugin parser factory for parsing index
    // annotations
    // metaFactoryPlugin.setClassName(CassandraPersistenceMappingParser.class.getName());

    ProductDerivations.beforeConfigurationLoad(this);
    loadGlobals();

    this.metaCache = new MetaCache();

    
  }

  /**
   * Initialize the indexing service
   * @param store
   */
  public void initializeIndexingService(CassandraStore store){
    if(this.indexingService != null){
      return;
    }
    
    this.indexingService = new InMemoryIndexingService(store);
  }
  
  
  /**
   * This is a fugly hack, figure out proper configuration parsing
   * @param value
   */
  public void setSerializer(String value){
    if(serializer != null){
      return;
    }
    
    String className = getValue(EntityManagerConfigurator.SERIALIZER_PROP).getOriginalValue();

    if(className == null){
      className = JavaSerializer.class.getName();
    }
    
    try {
      Class<?> clazz = Class.forName(className);

      this.serializer = (EmbeddedSerializer) clazz.newInstance();
    } catch (Exception e) {
      throw new UserException(String.format(
          "Unable to load class '%s' as an instance of %s", className,
          EmbeddedSerializer.class), e);
    }

    
  }
  
  /**
   * Get the serializer configured
   * @return
   */
  public EmbeddedSerializer getSerializer(){
    return this.serializer;
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
    return indexingService;
  }

}
