/**
 * 
 */
package com.datastax.hectorjpa.store;

import java.util.Map;

import org.apache.openjpa.conf.OpenJPAProductDerivation;
import org.apache.openjpa.conf.Specification;
import org.apache.openjpa.lib.conf.AbstractProductDerivation;
import org.apache.openjpa.lib.conf.Configuration;
import org.apache.openjpa.persistence.PersistenceProductDerivation;

/**
 * Defines the derivations that the cassandra plugin uses.  Mostly annotation based
 * additions for tracking indexing ability
 * 
 * @author Todd Nine
 *
 */
public class CassandraProductDerivation extends AbstractProductDerivation
    implements OpenJPAProductDerivation {

  /* (non-Javadoc)
   * @see org.apache.openjpa.lib.conf.ProductDerivation#getType()
   */
  @Override
  public int getType() {
    return TYPE_SPEC_STORE;
  }

  /* (non-Javadoc)
   * @see org.apache.openjpa.conf.OpenJPAProductDerivation#putBrokerFactoryAliases(java.util.Map)
   */
  @Override
  public void putBrokerFactoryAliases(Map aliases) {
  }

  /* (non-Javadoc)
   * @see org.apache.openjpa.lib.conf.AbstractProductDerivation#beforeConfigurationLoad(org.apache.openjpa.lib.conf.Configuration)
   */
  @Override
  public boolean beforeConfigurationLoad(Configuration c) {
    
    CassandraStoreConfiguration conf = (CassandraStoreConfiguration) c;
    
    Specification jpa = PersistenceProductDerivation.SPEC_JPA;
    Specification ejb = PersistenceProductDerivation.ALIAS_EJB;

    //wire up our meta data factory
    conf.metaFactoryPlugin.setAlias(ejb.getName(),
        CassandraMetaDataFactory.class.getName());
    conf.metaFactoryPlugin.setAlias(jpa.getName(),
        CassandraMetaDataFactory.class.getName());
    
    return true;

  }
  
  

}
