/**
 * 
 */
package com.datastax.hectorjpa.service;


/**
 * Performs all indexing checks and writes synchronously, blocking the caller
 * 
 * @author Todd Nine
 * 
 */
public class SyncInMemoryIndexingService extends InMemoryIndexingService {


  @Override
  public void audit(IndexAudit audit) {
    super.auditInternal(audit);

  }

  @Override
  public void delete(IndexAudit audit) {
    super.deleteInternal(audit);
  }

}
