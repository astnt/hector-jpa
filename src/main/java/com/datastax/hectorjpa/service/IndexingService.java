/**
 * 
 */
package com.datastax.hectorjpa.service;

import com.datastax.hectorjpa.store.CassandraStoreConfiguration;

/**
 * Service that defines all indexing operations
 * 
 * TODO TN, move writes of indexes to here from current mutator
 * 
 * @author Todd Nine
 *
 */
public interface IndexingService {

  /**
   * Performs a given range scan using the bytes of the id passed in the row collectionRowKey
   * If more than one column is present, the column with the latest timestamp is kept.  All others
   * are removed from the row in indexRowKey and collectionRowKey
   * @param 
   */
  public void audit(IndexAudit audit);
  
  /**
   * Performs a given range scan using the bytes of the id passed in the row collectionRowKey
   * For every column returned it is removed from the read row and the id row with the current time.
   * All others are removed from the row in indexRowKey and collectionRowKey
   * @param 
   */
  public void delete(IndexAudit audit);
  
  /**
   * inject the configuration post creation.  Will only be called on initialisation
   * @param config
   */
  public void postCreate(CassandraStoreConfiguration config);
 
}
