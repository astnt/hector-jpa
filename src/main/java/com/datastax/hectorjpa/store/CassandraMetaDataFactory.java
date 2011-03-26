/**
 * 
 */
package com.datastax.hectorjpa.store;

import org.apache.openjpa.persistence.AnnotationPersistenceMetaDataParser;
import org.apache.openjpa.persistence.PersistenceMetaDataFactory;

/**
 * Meta Data factory for Cassandra specific meta data
 * 
 * @author Todd Nine
 *
 */
public class CassandraMetaDataFactory extends PersistenceMetaDataFactory {

  /* (non-Javadoc)
   * @see org.apache.openjpa.persistence.PersistenceMetaDataFactory#newAnnotationParser()
   */
  @Override
  protected AnnotationPersistenceMetaDataParser newAnnotationParser() {
    return new CassandraAnnotationParser(repos.getConfiguration());
  }

 

}
