package com.datastax.hectorjpa.service;

import java.util.HashSet;
import java.util.Set;

import com.datastax.hectorjpa.store.CassandraStore;
import com.datastax.hectorjpa.store.CassandraStoreConfiguration;

/**
 * Simple bean class to act as a "queue" for queueing index changes.
 *  
 * @author Todd Nine
 *
 */
public class IndexQueue {

  private Set<IndexAudit> audits = new HashSet<IndexAudit>();
  
  private Set<IndexAudit> deletes = new HashSet<IndexAudit>();
  
  
  /**
   * Add this audit to the list
   * @param audit
   */
  public void addAudit(IndexAudit audit){
    audits.add(audit);
  }
  
  /**
   * Add this audit to the delete
   * @param delete
   */
  public void addDelete(IndexAudit delete){
    deletes.add(delete);
  }
  
  /**
   * Perform all audit checking
   * @param indexer
   */
  public void writeAudits(IndexingService indexer){
    
    for(IndexAudit current: audits){
      indexer.audit(current);
    }
    
    
    for(IndexAudit current: deletes){
      indexer.delete(current);
    }
    
    
  }
}
