/**
 * 
 */
package com.datastax.hectorjpa.store;

import java.util.Map;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.openjpa.conf.OpenJPAProductDerivation;
import org.apache.openjpa.conf.Specification;
import org.apache.openjpa.lib.conf.AbstractProductDerivation;
import org.apache.openjpa.lib.conf.Configuration;
import org.apache.openjpa.lib.conf.ConfigurationProvider;
import org.apache.openjpa.persistence.PersistenceProductDerivation;
import org.apache.openjpa.util.UserException;

import com.datastax.hectorjpa.consitency.JPAConsistency;
import com.datastax.hectorjpa.serialize.EmbeddedSerializer;
import com.datastax.hectorjpa.serialize.JavaSerializer;
import com.datastax.hectorjpa.service.InMemoryIndexingService;
import com.datastax.hectorjpa.service.IndexingService;

/**
 * Defines the derivations that the cassandra plugin uses. Mostly annotation
 * based additions for tracking indexing ability
 * 
 * @author Todd Nine
 * 
 */
public class CassandraProductDerivation extends AbstractProductDerivation
    implements OpenJPAProductDerivation {

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.openjpa.lib.conf.ProductDerivation#getType()
   */
  @Override
  public int getType() {
    return TYPE_SPEC_STORE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.openjpa.conf.OpenJPAProductDerivation#putBrokerFactoryAliases
   * (java.util.Map)
   */
  @Override
  public void putBrokerFactoryAliases(Map aliases) {
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.openjpa.lib.conf.AbstractProductDerivation#beforeConfigurationLoad
   * (org.apache.openjpa.lib.conf.Configuration)
   */
  @Override
  public boolean beforeConfigurationLoad(Configuration c) {

    // do nothing, may be an enhancer running
    if (!(c instanceof CassandraStoreConfiguration)) {
      return false;
    }

    CassandraStoreConfiguration conf = (CassandraStoreConfiguration) c;

    Specification jpa = PersistenceProductDerivation.SPEC_JPA;
    Specification ejb = PersistenceProductDerivation.ALIAS_EJB;

    // wire up our meta data factory
    conf.metaFactoryPlugin.setAlias(ejb.getName(),
        CassandraMetaDataFactory.class.getName());
    conf.metaFactoryPlugin.setAlias(jpa.getName(),
        CassandraMetaDataFactory.class.getName());

    // conf.metaRepositoryPlugin.setAlias("default",
    // CassandraMetaDataFactory.class.getName());

    return true;

  }

  
  

}
